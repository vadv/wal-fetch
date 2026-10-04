# wal-fetch

Fetch WAL files and timeline history from a PostgreSQL primary for `restore_command`, using a `LOGIN REPLICATION` role.

The first call starts a local Unix-socket server; subsequent calls reuse its PostgreSQL connection.

[Русская версия](README.ru.md)

## How it works

Each `restore_command` invocation runs a short-lived client. The first client starts a detached server process, one per PGDATA, and later clients connect to it over a Unix socket. The server keeps one long-lived PostgreSQL connection and the temporary slot between requests.

### Startup

The client dials `<pgdata>/.wal-fetch/server.sock`. If nothing answers, it takes an exclusive `flock` on `server.lock` and, as lock owner, starts the server as a detached session leader that inherits the lock. Startup status returns through a pipe. The server exits after `-idle-timeout` of inactivity, and the lock passes to the next client.

### Request handling

The server accepts one connection at a time and keeps no queue: concurrent `restore_command` calls wait in the kernel socket backlog and are handled one by one. The client sends its deadline with each request. Nothing is published when a request fails.

### Protocol (version 1)

Newline-delimited JSON over one Unix-socket connection per request:

1. The client sends `{"version":1,"identity":"…","name":"000000010000000000000003","deadline":…}`.
2. The server replies `{"version":1,"ok":true,"length":16777216,"sha256":"…","id":"…"}`, followed by exactly `length` payload bytes, then half-closes its side.
3. The client verifies length and SHA-256, writes the destination through a temporary file, calls `fsync` on it, renames it into place and answers `{"version":1,"id":"…","published":true}`.
4. Slot retention advances only after this acknowledgement.

### Identity digest

Every request carries a SHA-256 digest of the effective configuration: source connection settings (host, port, user, password, database, runtime parameters), SSL environment including digests of certificate files, and all behavioral options. Credentials never cross the socket. On a digest mismatch the server rejects the client with `source configuration mismatch` instead of mixing two configurations in one process.

### WAL path

For each segment the server runs `IDENTIFY_SYSTEM`, validates the requested timeline against the fetched timeline history, derives the exact LSN range from the filename and `-wal-segment-size`, and streams that range with `START_REPLICATION PHYSICAL`. Contiguity is enforced and the payload is truncated to the segment size, so an active segment ends in a zero-filled tail. Timeline history responses are capped at 1 MiB.

## Temporary slot

Recovery can spend time replaying a file before requesting the next one. Meanwhile, the primary keeps generating WAL and may recycle files recovery still needs. By default, wal-fetch creates a temporary physical replication slot to retain WAL between calls.

- **Name:** `wal_fetch_` followed by 32 random lowercase hex digits, recorded in the server log.
- **Lifetime:** created on the first WAL request and kept by the server's connection. PostgreSQL removes it when that connection ends or fails, including server shutdown after `-idle-timeout`.
- **Retention:** advances after contiguous files have been delivered, keeping the last delivered segment. It cannot recover files already recycled or protect all WAL preceding slot creation.

Use **`-no-slot`** to disable slot creation when WAL retention is managed elsewhere.

After **5 failed WAL fetches without a successful publication**, wal-fetch releases the slot and retries without creating another. Once a WAL file is published and the replication exchange finishes successfully, the next WAL request can create a new slot. History requests do not affect the counter. Set `-slot-failure-limit N` to change the threshold, or `0` to disable it.

A PostgreSQL error while reading recycled WAL releases the slot immediately. The threshold also covers repeated local rejections, such as a future segment. A replacement slot cannot recover WAL recycled during the gap.

## Usage

Set connection settings in the PostgreSQL service environment:

```sh
PGDATA=/var/lib/postgresql/data
PGHOST=leader
PGPORT=5432
PGUSER=replicator
PGPASSFILE=/var/lib/postgresql/.pgpass
PGSSLMODE=verify-full
```

The passfile entry uses database `replication`; file permissions must be `0600`.

```conf
restore_command = '/opt/wal-fetch "%f" "%p" || pgbackrest --stanza=main archive-get "%f" "%p"'
```

Without a slot:

```sh
wal-fetch -no-slot -pgdata /var/lib/postgresql/data WAL_NAME DESTINATION
```

| Option | Default | Purpose |
|---|---|---|
| `-h`, `-p`, `-U` | PostgreSQL environment | Source host, port and user |
| `-pgdata` | `PGDATA` or current directory | Directory for the local server socket and log |
| `-no-slot` | `false` | Fetch without reserving WAL |
| `-slot-failure-limit` | `5` | Failed WAL fetches before suspending slot creation; `0` disables the limit |
| `-log-file` | `<pgdata>/.wal-fetch/server.log` | Server log file |
| `-syslog` | `false` | Also send server logs to local syslog |
| `-idle-timeout` | `5m` | Stop the server after inactivity |
| `-timeout` | `30s` | Request timeout, including queueing |
| `-wal-segment-size` | `16MB` | Source segment size; override for a non-default cluster |
| `-serve` | `false` | Run the server in the foreground |
| `-version` | none | Print version and exit |

Changing connection, slot or logging settings requires restarting the local server. Stop it with `kill -TERM "$(cat "$PGDATA/.wal-fetch/server.pid")"`; the next request starts it again.

Run the server in the foreground for debugging; it accepts the same options, without `WAL_NAME DESTINATION`:

```sh
wal-fetch -serve -pgdata /var/lib/postgresql/data
```

Print the version of the installed binary:

```sh
wal-fetch -version
```

## Security

All server state is kept in `<pgdata>/.wal-fetch`, created with mode `0700` and owned by the PostgreSQL OS user: `server.sock` (`0600`), `server.lock`, `server.pid` and `server.log`.

- Both sides check the Unix-socket peer with `SO_PEERCRED` and drop connections from a different OS UID.
- The lock and log files are opened with `O_NOFOLLOW` and must be regular files owned by the same UID with mode `0600`. The socket directory must be owned by that UID with mode `0700`.
- The socket carries filenames, digests and file bytes only; connection settings travel only inside the identity digest.
- The destination appears by `rename` after `fsync`, so PostgreSQL never observes a partial segment or history file. A failed request leaves the destination unchanged.
- Multi-host connection strings are rejected. The server pins the first system identifier and timeline it sees and stops on any change.
- Leftover staging files are removed at startup by the lock owner. Nothing from a previous run is reused.

## Logs

Server events go to `<pgdata>/.wal-fetch/server.log` by default: startup/shutdown, slot lifecycle and file requests. Client errors go to stderr, which PostgreSQL captures when running `restore_command`.

To use a different file and also send events to local syslog:

```conf
restore_command = '/opt/wal-fetch -log-file /var/log/postgresql/wal-fetch.log -syslog "%f" "%p" || pgbackrest --stanza=main archive-get "%f" "%p"'
```

The log directory must exist and be writable by the PostgreSQL OS user; the file must belong to that user with mode `0600`. Syslog uses tag `wal-fetch`, facility `daemon`; the host's syslog configuration controls its destination.

Log rotation is external. Rename the file, then have the running server reopen it in place:

```sh
kill -USR1 "$(cat "$PGDATA/.wal-fetch/server.pid")"
```

The replacement must satisfy the same rules: a regular file owned by the same user with mode `0600`. A failed reopen keeps the old descriptor and is reported in the log.

## Troubleshooting

Client errors go to stderr with the `wal-fetch:` prefix, which PostgreSQL captures for `restore_command`. Server-side rejections surface as `server rejected request: …` on the client and appear in `server.log` as `rejected error=…`.

| Message | Cause | Action |
|---|---|---|
| `source configuration mismatch` | Client settings differ from the running server's | Restart the server (see Usage), then retry |
| `socket peer UID mismatch` | Socket peer runs as another OS user | One OS user owns `$PGDATA`; check who started the other server |
| `source system or timeline changed; start a new server` | The primary was reinitialized or moved to a new timeline | The server stops itself; the next request starts a new one |
| `server startup failed` | The freshly started server failed | Read `server.log` |
| `PGDATA path is too long for Unix socket` | Path over the 108-character Unix-socket limit | Use a shorter PGDATA |
| `server already running` | Foreground `-serve` while another server owns the lock | Use the running server, or stop it first |

Inspect the temporary slot from SQL; it disappears when its owning server exits, which is expected after `-idle-timeout` or `kill -TERM`:

```sql
SELECT slot_name, active, restart_lsn
FROM pg_replication_slots
WHERE slot_name LIKE 'wal_fetch_%';
```

## Limits

Linux only. The server follows one fixed primary and timeline. A source system or timeline change stops the server; the next request starts a new one against the new source. Losing its connection also loses the temporary slot's WAL retention. An active segment is a snapshot of flushed WAL with a zero-filled tail.

Syslog requires a local receiver at startup. Delivery is best effort: a stalled receiver can lose syslog copies; file logging continues. Log rotation is external: rename the file and send `SIGUSR1` to the PID in `server.pid`; no restart is required.

## Build and test

Linux, Go 1.25. Recovery tests require Python 3, PostgreSQL 16 tools on `PATH` and a non-root user.

```sh
go build -o wal-fetch .
go test ./...
python3 -m venv tests/integration/.venv
. tests/integration/.venv/bin/activate
python -m pip install -r tests/integration/requirements.txt
python -m pytest tests/integration -v
WAL_FETCH_TEST_SEGMENT_MB=1 python -m pytest tests/integration -v
```

Release builds set the version reported by `-version`: `go build -ldflags "-X main.buildVersion=v1.0.0" -o wal-fetch .`

CI runs unit/race/vet checks and real recovery with 16 MiB and 1 MiB WAL segments, with and without a slot. A separate job runs the same integration suite against a binary built with `-race`. It checks timeline history, recovery to the target checkpoint, slot lifecycle and failure handling.

[MIT License](LICENSE). [Third-party licenses](THIRD_PARTY_NOTICES.txt).
