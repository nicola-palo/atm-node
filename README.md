# Distributed Banking System with Token Ring Mutual Exclusion

Final Project 1 — Distributed Systems

Four independent ATM nodes, each running in its own terminal on localhost,
operate on a single shared bank account with no shared memory. Mutual
exclusion is guaranteed exclusively by the **Token Ring** algorithm: exactly
one token circulates along the ring `ATM1 -> ATM2 -> ATM3 -> ATM4 -> ATM1`,
and only the node holding the token can execute a transaction (critical
section). The account balance travels inside the token message, so it is
always consistent across the system.

## 1. Execution Instructions

### 1.1 Language and environment

- Programming language: **Python** (tested on Python 3.14; any version >= 3.8 works)
- Reference operating system: Linux
- No compilation is required: `atm.py` is interpreted directly

### 1.2 Libraries and dependencies

None. The project uses only the Python standard library (`socket`,
`threading`, `json`, `queue`, `termios`). No installation command is needed.

### 1.3 Starting the nodes

Open 4 separate terminals (one per node) and run, from the project folder:

```
Terminal 1:  python3 atm.py 1
Terminal 2:  python3 atm.py 2
Terminal 3:  python3 atm.py 3
Terminal 4:  python3 atm.py 4
```

Each node listens on `127.0.0.1`: node *n* on port `500n` (5001-5004).
Nodes can be started in any order: if the successor is not up yet, a node
keeps the token and retries the delivery every second, so the token is
never lost. ATM1 creates the single token about 2 seconds after its start
(initial balance 1000) and injects it into the ring. From that moment the
token circulates continuously.

### 1.4 Node-terminal association

Each terminal shows a fixed status panel titled `ATM n` with the node id,
its successor, the last known balance, the pending requests and the token
position. Moreover every log line is prefixed by `[ATM n]` and a timestamp,
so it is always possible to tell which node is running in which terminal.

### 1.5 Interactive commands

Commands are typed in the terminal of the chosen node (case-insensitive):

```
DEPOSIT <amount>    queue a deposit
WITHDRAW <amount>   queue a withdrawal (refused if over the balance)
BALANCE             show the last balance seen by this node
HELP                show the command list
```

A queued request is executed only when the token reaches that node: this
is the mutual exclusion at work.

### 1.6 Logging

Each node logs, on screen and in the file `logs/atm<n>.log`: token
reception, token forwarding, transaction begin/commit/rollback and the
updated balance. The logs clearly show the movement of the token and the
absence of concurrent transactions: every transaction is enclosed between
a `Token received` and a `Token forwarded` line of the same node, and
merging the four logs by time shows that no two nodes ever transact at the
same time.

### 1.7 Suggested demo (as in the assignment)

Start the 4 nodes and wait for the token circulation (visible in the
panels and in the Token received/forwarded lines), then type:

```
on ATM2:  WITHDRAW 200   ->  balance 800
on ATM3:  DEPOSIT 100    ->  balance 900
on ATM4:  WITHDRAW 500   ->  balance 400
```

Optionally, `WITHDRAW 99999` on any node shows a refused transaction
(insufficient funds).

## 2. Repository contents

- `atm.py` — full commented source code (comments cover token management,
  message exchange and critical-section logic)
- `README.md` — these instructions
- `VIDEO.webm` — demo video showing the 4 nodes running in separate
  terminals, the token circulation and the execution of the transactions
