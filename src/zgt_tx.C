/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Spring 2026: CSE 4331/5331 Project 2 : Tx Manager */

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>


extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort_operation(long, char);   //commit/abort based on char value
extern void *process_read_write_operation(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus, char type, pthread_t thrid){
  this->lockmode = (char)' ';   // default
  this->Txtype = type;          // R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1;              // set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1;             // init to an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. When creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
 
    // Writes the Txtype to the file.
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  fprintf(ZGT_Sh->logfile, "T%d\t\t%c \t\tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
  // do the operations for reading. Write your code
  struct param *node = (struct param*)arg;// get tid and objno and count
  // start_operation(node->tid, node->count);
  process_read_write_operation(node->tid, node->obno, node->count, 'S');
  // finish_operation(node->tid);
  free(node);
  pthread_exit(NULL);

}


void *writetx(void *arg){ //do the operations for writing; similar to readTx

  struct param *node = (struct param*)arg;	
  // start_operation(node->tid, node->count);
  process_read_write_operation(node->tid, node->obno, node->count, 'X');
  // finish_operation(node->tid);
  free(node);
  pthread_exit(NULL);
  

}

// common method to process read/write: Just a suggestion
void *process_read_write_operation(long tid, long obno,  int count, char mode){

  start_operation(tid,count);

  //locking the trnasction manager
  zgt_p(0);

  zgt_tx *tx;
  tx = get_tx(tid); // get the transaction 

  if (tx != NULL)
  {
    if(tx->status == TR_ABORT)
    {
      zgt_v(0); //realse the lock on tm
      finish_operation(tid);
      return NULL;
    }
    int lock_status;
    lock_status = tx->set_lock(tid, 1, obno, count, mode);

    if (lock_status == 0)
    {
      tx->perform_read_write_operation(tid, obno, mode);
    }

  }
  zgt_v(0); //realse the lock on tm before sleeping our thread

  //seleeping
  if (tx != NULL && tx->status != TR_ABORT)
  {
    usleep(ZGT_Sh->optime[tid]);
  }

  finish_operation(tid);
  return NULL;
  
}


void *aborttx(void *arg)
{
  //write your code
  struct param *node = (struct param*)arg;// get tid and count  
  
  start_operation(node->tid, node->count);
  zgt_p(0);
  do_commit_abort_operation(node->tid, TR_ABORT);
  zgt_v(0);
  finish_operation(node->tid);
  pthread_exit(NULL);			
}

void *committx(void *arg)
{
 
    //remove the locks/objects before committing
  struct param *node = (struct param*)arg;// get tid and count

  //write your code
  start_operation(node->tid, node->count);
  zgt_p(0);
  do_commit_abort_operation(node->tid, TR_END);
  zgt_v(0);
  finish_operation(node->tid);
  pthread_exit(NULL);			
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existent tx

void *do_commit_abort_operation(long t, char status){

  // write your code
  // init lock

    zgt_tx *tx = get_tx(t);

    if (tx == NULL) 
    {
        fprintf(ZGT_Sh->logfile, "Cannot Commit/Abort a Tx:%d that does not exist\n", t);
        fflush(ZGT_Sh->logfile);
        return NULL;
    }

    tx->status = status;

    if (status == TR_END) 
    {
      fprintf(ZGT_Sh->logfile, "T%ld\t\tCommitTx\t", t);
    }
   else 
    {
      fprintf(ZGT_Sh->logfile, "T%ld\t\tAbortTx\t\t", t);
    }
    fflush(ZGT_Sh->logfile);
    tx->free_locks();

    int waiting = zgt_nwait(t);
    for (int i = 0; i < waiting; i++)
    {
      zgt_v(t);
    }

    tx->remove_tx();
    return NULL;
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if correct node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}


int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){

zgt_tx *tx = get_tx(tid1);

while(true) {
    zgt_hlink *linkp = ZGT_Ht->find(sgno1, obno1);
    
    if (linkp == NULL) {
        ZGT_Ht->add(tx, sgno1, obno1, lockmode1);
        return 0;
    }
    
    if (linkp->tid == tid1) {
        return 0; 
    } 
    bool conflict = false;
    long holding_tid = -1;
        zgt_hlink *curr = ZGT_Sh->head[ZGT_Ht->hashing(sgno1, obno1)];
    while(curr != NULL) {
        if (curr->obno == obno1 && curr->sgno == sgno1 && curr->tid != tid1) {
            if (curr->lockmode == 'X' || lockmode1 == 'X') {
                conflict = true;
                holding_tid = curr->tid;
                break;
            }
        }
        curr = curr->next;
    }
    
    if (!conflict) {
        ZGT_Ht->add(tx, sgno1, obno1, lockmode1);
        return 0;
    }
    
    //when conflicting wait on semaphore
    tx->status = TR_WAIT;
    tx->obno = obno1;
    tx->lockmode = lockmode1;
    tx->setTx_semno(holding_tid, holding_tid);
    

    int optime = ZGT_Sh->optime[tid1];

    fprintf(ZGT_Sh->logfile,
        "T%ld   \t%c\t\t%s\t\t%ld:%d:%d\t\t%sLock\t\tNOTGranted\tWAIT(T%ld)\n",
        tid1,
        tx->Txtype,
        (tx->Txtype == 'R') ? "ReadTX": "WriteTX",
        obno1,
        ZGT_Sh->objarray[obno1]->value,
        optime,
        (lockmode1 == 'S') ? "Read" : "Write",
        holding_tid
    );

fflush(ZGT_Sh->logfile);

    //reaaquire tms lock
    zgt_v(0); 
    zgt_p(holding_tid); 
    zgt_p(0); 
    
    //after waking up see if the tx was aborted
    if (tx->status == TR_ABORT) {
        return -1;
    }
    tx->status = TR_ACTIVE;
}
}

int zgt_tx::free_locks()
{
  
  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink* temp = head;  //first obj of tx
  
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      fprintf(ZGT_Sh->logfile, "%d : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      fflush(ZGT_Sh->logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                            temp->tid, temp->obno, temp->lockmode);
	   fflush(stdout);
#endif
      }
    }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);
  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}



void zgt_tx::perform_read_write_operation(long tid, long obno, char lockmode){
  int optime = ZGT_Sh->optime[tid];
  
  if (lockmode == 'S') {
      ZGT_Sh->objarray[obno]->value -= 4;
      fprintf(ZGT_Sh->logfile, "T%ld\t\t%c \t\tReadTx\t\t%ld:%d:%d\t\tReadLock\t\tGranted\t\t%c\n", 
              tid, this->Txtype, obno, ZGT_Sh->objarray[obno]->value, optime, this->status);
  } else {
      ZGT_Sh->objarray[obno]->value += 7;
      fprintf(ZGT_Sh->logfile, "T%ld\t\t%c \t\tWriteTx\t\t%ld:%d:%d\t\tWriteLock\t\tGranted\t\t%c\n", 
              tid, this->Txtype, obno, ZGT_Sh->objarray[obno]->value, optime, this->status);
  }
  fflush(ZGT_Sh->logfile);
}


// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}


