# Transaction Manager

A multithreaded **transaction manager** written in C++. It runs a schedule of database transactions concurrently and keeps them correct using **Strict Two-Phase Locking (Strict 2PL)** with shared (read) and exclusive (write) locks.

Each operation in an input schedule (`BeginTx`, `Read`, `Write`, `Commit`, `Abort`) runs on its own POSIX thread. The manager grants or blocks locks, makes conflicting transactions wait, and releases every lock a transaction holds when it commits or aborts. Every step is written to a log file so you can trace the interleaving.

> Built as Project 2 for **CSE 4331/5331 (Database Systems)**. The starter framework (the lock-table hash table, semaphore wrappers, and test driver) came from the course. My work is the transaction-manager and transaction logic in `zgt_tm.C` and `zgt_tx.C`.

---

## Features

- **Strict 2PL concurrency control.** Locks are held until the transaction commits or aborts.
- **Shared and exclusive locks.** Many readers can share an object. A writer gets exclusive access.
  - `S` + `S` → compatible
  - `S` + `X`, `X` + `S`, `X` + `X` → conflict, so the requester waits
- **Read-only and read/write transaction types** (`R` / `W`), declared at `BeginTx`.
- **Per-transaction operation ordering.** Every operation gets its own thread, but a mutex and condition variable per transaction make sure that transaction's operations still run in the order the schedule lists them.
- **Blocking with System V semaphores.** A transaction that hits a lock conflict sleeps on the semaphore of the transaction holding the lock. That semaphore is signaled when the holder commits or aborts.
- **Hash-table lock table** keyed on `(segment, object)`, with a per-transaction chain of the locks each transaction holds.
- **Detailed logging** of every operation: lock type, whether the lock was granted or the transaction is waiting, object values, and transaction status.

---

## Project structure

```
TransactionManager/
├── include/
│   ├── zgt_tm.h          # Transaction manager class (zgt_tm), shared state
│   ├── zgt_tx.h          # Transaction class (zgt_tx), lock table (zgt_ht)
│   ├── zgt_def.h         # Constants and transaction status codes
│   ├── zgt_extern.h      # Extern declarations and thread-param struct
│   ├── zgt_global.h      # Global object definitions
│   └── zgt_ddlock.h      # Wait-for graph structures (deadlock detection)
├── src/
│   ├── zgt_test.C        # Driver: parses the schedule file, calls the TM
│   ├── zgt_tm.C          # TM: spawns a thread per operation
│   ├── zgt_tx.C          # Begin/read/write/commit/abort and locking logic
│   ├── zgt_ht.C          # Lock table (hash table) implementation
│   ├── zgt_semaphore.C   # System V semaphore helpers (P / V / wait count)
│   ├── Makefile
│   ├── tmtest_v2         # Runs a schedule N times, then cleans up IPC
│   └── ipcs_cleanup.sh   # Removes leftover semaphores / shared memory
└── test-files/           # Sample schedules
```

---

## How it works

1. **`zgt_test`** reads the schedule line by line and calls the matching method on the transaction manager (`zgt_tm`).
2. **`zgt_tm`** gives each operation a sequence number for its transaction and spawns a thread (`begintx`, `readtx`, `writetx`, `committx`, `aborttx`).
3. Each thread first waits for its turn in its own transaction (`start_operation`). Then it takes the global TM semaphore (semaphore `0`) before it touches shared state.
4. **Read and write** operations call `set_lock`:
   - If the object isn't locked, or this transaction already holds the lock, the lock is granted and the operation runs. A read subtracts 4 from the object's value and a write adds 7, so the log shows that something happened.
   - If another transaction holds a conflicting lock, the requester is marked `W` (waiting), logs `NOTGranted WAIT(Tn)`, releases the TM semaphore, and blocks on the holder's semaphore.
5. **Commit and abort** release all of the transaction's locks, wake every transaction waiting on it, and remove it from the active list.
6. **`end all`** joins every thread, destroys the mutexes and condition variables, removes the semaphores, and closes the log.

---

## Requirements

- Linux or another Unix-like OS with **System V IPC** (`semget`, `semop`, `ipcs`, `ipcrm`)
- **g++** with pthread support
- GNU `make`

---

## Build

```bash
cd src
make
```

This produces the `zgt_test` executable. The Makefile turns on debug output by default (`-DTX_DEBUG -DTM_DEBUG -DHT_DEBUG`). Edit `DEBUGFLAGS` in the Makefile for quieter output.

To remove build artifacts:

```bash
make clean
```

---

## Usage

Run a schedule directly:

```bash
cd src
./zgt_test ../test-files/S2T.txt
```

Or run it several times in a row with the helper script. The script takes the file name **without** the `.txt` extension, expects the file in the current directory, and cleans up IPC resources at the end:

```bash
cd src
cp ../test-files/*.txt .
./tmtest_v2 5 S2T        # run S2T.txt five times
```

Stdout goes to `<name>.out`. The log goes wherever the schedule's `LogFile` line points (for example `../test-files/S2T.log`).

If a run crashes or you stop it, it can leave semaphores behind. Clear them with:

```bash
./ipcs_cleanup.sh
```

---

## Input file format

```text
// comment lines start with //
LogFile ../test-files/example.log
BeginTx 1 W        # tid, type (R = read-only, W = read/write)
Read    1 3        # tid, object number
Write   1 3
BeginTx 2 R
Read    2 3
Commit  1
Commit  2          # or: Abort <tid>
end all
```

| Command               | Meaning                                                       |
|-----------------------|---------------------------------------------------------------|
| `LogFile <path>`      | File to write the log to (must come first)                    |
| `BeginTx <tid> <R/W>` | Start transaction `tid` as read-only or read/write            |
| `Read <tid> <obj>`    | Read object `obj` (shared lock)                               |
| `Write <tid> <obj>`   | Write object `obj` (exclusive lock)                           |
| `Commit <tid>`        | Commit and release all locks                                  |
| `Abort <tid>`         | Abort and release all locks                                   |
| `end all`             | Wait for all threads, release resources, exit                 |

Limits: up to **10 transactions** (`tid` 1–10) and **15 objects** (`obj` 0–14).

---

## Sample schedules

| File           | Scenario                                                              |
|----------------|-----------------------------------------------------------------------|
| `S2T.txt`      | Serial history: two transactions, one after the other                 |
| `NoC2T.txt`    | Two concurrent transactions on disjoint objects (no conflicts)        |
| `RR.txt`       | Two read-only transactions sharing objects (shared locks coexist)     |
| `RRW.txt`      | Readers and a writer on the same object (the writer must wait)        |
| `C2Tsz.txt`    | Conflicting but serializable interleaving of two transactions         |
| `deadlock.txt` | Two transactions that deadlock (see limitations)                      |

### Sample log output

```text
---------------------------------------------------------------------------
TxId    Txtype  Operation   ObId:Obvalue:optime  LockType    Status      TxStatus
T1      R       BeginTx
T1      R       ReadTx      1:-4:...             ReadLock    Granted     P
T2      W       BeginTx
T2      W       WriteTX     1:-4:...             WriteLock   NOTGranted  WAIT(T1)
...
```

---

## Limitations

- **No deadlock detection or resolution.** The `Detect` and `Choose` commands are parsed, but their handlers are stubs, so `deadlock.txt` hangs as expected. A wait-for-graph structure is included in `zgt_ddlock.h` as a starting point.
- Each transaction can wait on only one other transaction at a time.
- Semaphores use a fixed IPC key (`TEAM_NO` in `zgt_tm.C`). Two instances on the same machine can collide. Run `ipcs_cleanup.sh` between runs if something goes wrong.

---

## Acknowledgments

The starter framework was provided by the CSE 4331/5331 course staff and is based on the Zeitgeist (`zgt_*`) transaction-manager code from Texas Instruments.
