"""
Four ATM processes on localhost share one account, with no shared memory.
A single token circulates in the ring ATM1 -> ATM2 -> ATM3 -> ATM4 -> ATM1
and only its holder can transact. The balance travels in the token itself,
so it can never go out of sync between nodes.

Each terminal shows a fixed status panel (balance, pending requests, token
position) with event lines scrolling below. Everything -- token traffic
included -- is logged on screen and to logs/atm<n>.log.

Run one node per terminal:  python3 atm.py <1..4>
"""

import atexit
import json
import os
import queue
import select
import signal
import socket
import sys
import termios
import threading
import time
from datetime import datetime

HOST = "127.0.0.1"        # everything stays on localhost
BASE_PORT = 5001          # node N listens on 5000 + N
NUM_NODES = 4
INITIAL_BALANCE = 1000
BOOT_DELAY = 2.0          # give the other nodes time to come up
RETRY_DELAY = 1.0         # pause between attempts to reach the successor
TOKEN_PASS_DELAY = 0.3    # per-hop pacing: simulated network latency, keeps the ring watchable
LOG_DIR = "logs"
PANEL_ROWS = 4
PANEL_REDRAW = 0.1        # max panel refresh rate in seconds


class Display:
    """Two-zone terminal UI: fixed status panel on top, scrolling events
    below, fixed prompt at the bottom. Falls back to plain line output
    when stdin/stdout are not a real terminal (automated tests, pipes)."""

    def __init__(self, node):
        self.node = node
        self.lock = threading.Lock()
        self.tty = sys.stdin.isatty() and sys.stdout.isatty()
        self.height, self.width = 24, 80
        self.buffer = ""       # current input line, we echo it ourselves
        self.dirty = False
        self.closed = False
        self.needs_resize = False
        self._init_done = False
        self.saved_termios = None
        if self.tty:
            signal.signal(signal.SIGWINCH, self._on_winch)

    def _on_winch(self, signum, frame):
        self.needs_resize = True

    def _write(self, text):
        sys.stdout.write(text)
        sys.stdout.flush()

    # --- lifecycle -------------------------------------------------------

    def setup(self):
        if not self.tty:
            return
        # os.get_terminal_size (not shutil): we want the real ioctl size,
        # shutil would let the COLUMNS env var override it
        try:
            size = os.get_terminal_size(sys.stdout.fileno())
            self.height, self.width = size.lines, size.columns
        except OSError:
            self.height, self.width = 24, 80
        self.height = max(self.height, PANEL_ROWS + 3)
        self.width = max(self.width, 40)
        if not self._init_done:
            self._init_done = True
            self.saved_termios = termios.tcgetattr(sys.stdin)
            attrs = termios.tcgetattr(sys.stdin)
            attrs[3] &= ~(termios.ECHO | termios.ICANON)  # raw-ish input, we echo
            termios.tcsetattr(sys.stdin, termios.TCSANOW, attrs)
            threading.Thread(target=self._ticker, daemon=True).start()
            atexit.register(self.teardown)
        with self.lock:
            self._write("\033[?1049h\033[?25l")  # alternate screen, hide cursor
            self._write(f"\033[{PANEL_ROWS + 1};{self.height - 1}r")  # events scroll here
            self._write("\033[2J\033[H")
        self.draw_panel()
        self.render_prompt()

    def teardown(self):
        # give the terminal back exactly as we found it
        if self.closed or not self._init_done:
            return
        self.closed = True
        with self.lock:
            try:
                self._write("\033[r\033[?25h\033[?1049l")
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self.saved_termios)
            except Exception:
                pass

    def _ticker(self):
        # redraw the panel at a human pace, not once per token hop
        while not self.closed:
            time.sleep(PANEL_REDRAW)
            if self.dirty:
                with self.lock:
                    self.dirty = False
                    self.draw_panel()

    # --- panel and events --------------------------------------------------

    def refresh_panel(self):
        if not self.tty or self.closed:
            return
        self.dirty = True

    def draw_panel(self):
        # assumes self.lock is held
        n = self.node
        balance = "unknown" if n.last_balance is None else str(n.last_balance)
        todo = f" ({n.last_request})" if n.last_request else ""
        position = "HERE" if n.token_here else "not here"
        seq = n.last_seq if n.last_seq is not None else "-"
        when = n.last_seq_time or "--:--:--"
        inner = self.width - 2
        top = f" ATM {n.node_id}  ·  successor: ATM{n.successor_id} ".ljust(inner, "─")[:inner]
        mid1 = f" Balance: {balance}   Pending: {n.pending.qsize()}{todo} "[:inner].ljust(inner)
        mid2 = f" Token: {position}   last seq: {seq} @ {when} "[:inner].ljust(inner)
        self._write(
            "\033[1;1H\033[2K┌" + top + "┐"
            "\033[2;1H\033[2K│" + mid1 + "│"
            "\033[3;1H\033[2K│" + mid2 + "│"
            "\033[4;1H\033[2K└" + "─" * inner + "┘"
        )

    def event(self, line):
        if self.closed:
            return
        if not self.tty:
            print(line, flush=True)  # degraded mode: plain line output
            return
        line = line[: self.width - 1]
        bottom = self.height - 1
        with self.lock:
            self._write(f"\033[{bottom};1H\033[2K{line}\r\n")

    # --- prompt and keyboard -------------------------------------------------

    def render_prompt(self):
        if not self.tty or self.closed:
            return
        text = f"ATM{self.node.node_id}> {self.buffer}"[: self.width - 1]
        with self.lock:
            self._write(f"\033[{self.height};1H\033[2K{text}")

    def read_line(self):
        """Blocking line read. None on Ctrl+D, KeyboardInterrupt on Ctrl+C."""
        if not self.tty:
            try:
                return input(f"ATM{self.node.node_id}> ")
            except EOFError:
                return None
        fd = sys.stdin.fileno()
        while True:
            if self.needs_resize:
                self.needs_resize = False
                self.setup()
            ready, _, _ = select.select([fd], [], [], 0.2)
            if not ready:
                continue
            ch = os.read(fd, 1)
            if ch in (b"\r", b"\n"):
                line, self.buffer = self.buffer, ""
                self.render_prompt()
                return line
            if ch in (b"\x7f", b"\x08"):
                self.buffer = self.buffer[:-1]
            elif ch == b"\x03":
                raise KeyboardInterrupt
            elif ch == b"\x04":
                return None
            elif ch == b"\x1b":
                while select.select([fd], [], [], 0.05)[0]:
                    os.read(fd, 1)  # arrow keys and friends: ignore
            elif ch >= b" ":
                self.buffer += ch.decode("utf-8", "ignore")
            self.render_prompt()


class ATMNode:
    def __init__(self, node_id: int):
        self.node_id = node_id
        # a node only knows who comes next; ATM4 wraps around to ATM1
        self.successor_id = (node_id % NUM_NODES) + 1
        self.listen_port = BASE_PORT + node_id - 1
        self.successor_port = BASE_PORT + self.successor_id - 1

        # transactions waiting for the token: the input thread fills this,
        # the network thread drains it (Queue is already thread-safe)
        self.pending = queue.Queue()
        self.last_balance = None
        self.last_request = None
        self.token_here = False
        self.last_seq = None
        self.last_seq_time = None

        self.file_lock = threading.Lock()
        os.makedirs(LOG_DIR, exist_ok=True)
        self.log_file = open(
            os.path.join(LOG_DIR, f"atm{node_id}.log"), "a", encoding="utf-8"
        )
        self.display = Display(self)

    # --- logging -------------------------------------------------------------

    def _stamp(self, message: str) -> str:
        return f"[{datetime.now().strftime('%H:%M:%S')}] [ATM{self.node_id}] {message}"

    def log_event(self, message: str) -> None:
        # things worth watching: screen (if available) + file
        line = self._stamp(message)
        with self.file_lock:
            self.log_file.write(line + "\n")
            self.log_file.flush()
        self.display.event(line)

    def log_token(self, message: str) -> None:
        # token traffic is logged like any other event: on screen and in
        # the file (par. 9), and it also feeds the status panel
        line = self._stamp(message)
        with self.file_lock:
            self.log_file.write(line + "\n")
            self.log_file.flush()
        self.display.event(line)
        self.display.refresh_panel()

    # --- message exchange ------------------------------------------------------

    def send_token(self, token: dict) -> None:
        """Pass the token to our successor, retrying until it is up."""
        payload = (json.dumps(token) + "\n").encode()
        while True:
            try:
                # one short-lived connection per hop: stateless, and the
                # nodes can be started in any order
                with socket.create_connection(
                    (HOST, self.successor_port), timeout=5
                ) as sock:
                    sock.sendall(payload)
                self.log_token(f"Token forwarded to ATM{self.successor_id}")
                self.token_here = False  # really gone only once delivered
                self.display.refresh_panel()
                return
            except OSError:
                # successor not started yet, or momentarily busy:
                # the token stays with us while we retry
                self.log_event(
                    f"ATM{self.successor_id} not reachable, "
                    f"retrying in {RETRY_DELAY:.0f}s..."
                )
                time.sleep(RETRY_DELAY)

    def server_loop(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, self.listen_port))
        srv.listen(8)
        self.log_event(
            f"Listening on {HOST}:{self.listen_port} "
            f"(successor: ATM{self.successor_id})"
        )
        while True:
            conn, _addr = srv.accept()
            threading.Thread(
                target=self.handle_connection, args=(conn,), daemon=True
            ).start()

    def handle_connection(self, conn: socket.socket) -> None:
        with conn:
            buf = b""
            while not buf.endswith(b"\n"):  # newline marks the end of the JSON message
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
            try:
                message = json.loads(buf.decode())
            except ValueError:
                self.log_event("Malformed message received, discarding it")
                return
            if message.get("type") == "TOKEN":
                self.handle_token(message)

    # --- token and critical section ---------------------------------------------

    def handle_token(self, token: dict) -> None:
        """The core of the algorithm: transact, then pass the token on."""
        # critical section: we are here only because the token is in our
        # hands -- mutual exclusion comes purely from the ring algorithm
        self.token_here = True
        self.last_balance = token["balance"]
        self.last_seq = token["seq"]
        self.last_seq_time = datetime.now().strftime("%H:%M:%S")
        self.log_token(
            f"Token received from ATM{token['from']} "
            f"(balance: {token['balance']}, seq: {token['seq']})"
        )

        # short pause on every pass: think of it as the transmission
        # latency of a real network -- the node is not holding the token
        # to do work, and the ring stays readable for humans
        time.sleep(TOKEN_PASS_DELAY)

        while True:
            try:
                kind, amount = self.pending.get_nowait()
            except queue.Empty:
                break
            self.execute_transaction(token, kind, amount)
        self.last_request = None
        self.display.refresh_panel()

        token["from"] = self.node_id
        token["seq"] += 1
        self.send_token(token)

    def execute_transaction(self, token: dict, kind: str, amount: int) -> None:
        """One transaction: read, validate, update, write back, log."""
        self.log_event(f"BEGIN transaction: {kind.lower()} {amount}")
        balance = token["balance"]
        if kind == "WITHDRAW" and amount > balance:
            self.log_event(
                f"ROLLBACK transaction: withdraw {amount} refused, "
                f"insufficient funds (balance: {balance})"
            )
            self.display.refresh_panel()
            return
        token["balance"] = balance + amount if kind == "DEPOSIT" else balance - amount
        self.last_balance = token["balance"]
        self.log_event(f"COMMIT transaction: new balance: {token['balance']}")
        self.display.refresh_panel()

    def bootstrap_token(self) -> None:
        """ATM1 throws the very first token into the ring."""
        time.sleep(BOOT_DELAY)
        self.log_event(f"Creating the token (initial balance: {INITIAL_BALANCE})")
        self.last_balance = INITIAL_BALANCE
        self.last_seq = 0
        self.last_seq_time = datetime.now().strftime("%H:%M:%S")
        self.token_here = True  # the token is ours until it can be sent
        self.display.refresh_panel()
        self.send_token(
            {
                "type": "TOKEN",
                "from": self.node_id,
                "balance": INITIAL_BALANCE,
                "seq": 0,
            }
        )

    # --- operator commands ---------------------------------------------------------

    def input_loop(self) -> None:
        # commands are only queued here: they run exclusively in
        # handle_token, i.e. when this node holds the token
        self.print_help()
        while True:
            try:
                line = self.display.read_line()
            except KeyboardInterrupt:
                self.log_event("Shutting down (Ctrl+C)")
                return
            if line is None:
                return
            self.parse_command(line.strip())

    def parse_command(self, line: str) -> None:
        parts = line.split()
        if not parts:
            return
        cmd = parts[0].upper()

        if cmd == "HELP":
            self.print_help()

        elif cmd == "BALANCE":
            if self.last_balance is None:
                self.log_event("Balance unknown: the token has not reached this node yet")
            else:
                self.log_event(f"Last known balance: {self.last_balance}")

        elif cmd in ("DEPOSIT", "WITHDRAW"):
            if len(parts) != 2 or not parts[1].isdigit() or int(parts[1]) <= 0:
                self.log_event(f"Invalid amount, usage: {cmd} <positive integer>")
                return
            amount = int(parts[1])
            self.pending.put((cmd, amount))
            self.last_request = f"{cmd} {amount}"
            self.display.refresh_panel()
            self.log_event(f"Request queued: {cmd.lower()} {amount} (waiting for the token)")

        else:
            self.log_event(f"Unknown command '{cmd}', type HELP for the command list")

    def print_help(self) -> None:
        self.log_event(
            "Commands: DEPOSIT <amount> | WITHDRAW <amount> | BALANCE | HELP"
        )

    # --- startup ----------------------------------------------------------------------

    def start(self) -> None:
        self.display.setup()
        try:
            threading.Thread(target=self.server_loop, daemon=True).start()
            if self.node_id == 1:
                threading.Thread(target=self.bootstrap_token, daemon=True).start()
            self.input_loop()
        finally:
            self.display.teardown()


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {
        str(n) for n in range(1, NUM_NODES + 1)
    }:
        print(f"Usage: python3 {sys.argv[0]} <node_id 1-{NUM_NODES}>", file=sys.stderr)
        sys.exit(1)
    ATMNode(int(sys.argv[1])).start()


if __name__ == "__main__":
    main()
