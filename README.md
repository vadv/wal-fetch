# wal-fetch

Fetches WAL and timeline history files from a PostgreSQL primary for `restore_command`, using a `LOGIN REPLICATION` role.

Recovery requests one file, replays it, and only then requests the next. While it replays, the primary keeps writing WAL and recycles files recovery still needs. `wal-fetch` keeps one connection and a temporary replication slot open between requests, so those files are still there when recovery comes back for them.

The first request starts a detached server per PGDATA. Later requests are short-lived clients that connect to it over a Unix socket in `<pgdata>/.wal-fetch/`.

[Русская версия](README.ru.md)

## Quick start

Connection settings come from the PostgreSQL service environment:

```sh
PGDATA=/var/lib/postgresql/data
PGHOST=leader
PGPORT=5432
PGUSER=replicator
PGPASSFILE=/var/lib/postgresql/.pgpass
PGSSLMODE=verify-full
```

The passfile entry uses database `replication` and must have mode `0600`.

```conf
restore_command = '/opt/wal-fetch "%f" "%p" || pgbackrest --stanza=main archive-get "%f" "%p"'
```

Fetch without a slot:

```sh
wal-fetch -no-slot -pgdata /var/lib/postgresql/data WAL_NAME DESTINATION
```

## How it works

### Startup

The client dials `<pgdata>/.wal-fetch/server.sock`. If nothing answers, it takes an exclusive `flock` on `server.lock` and, as lock owner, starts the server as a detached session leader that inherits the lock. Startup status comes back through a pipe. The server exits after `-idle-timeout` of inactivity, and the lock passes to the next client.

### Request handling

The server accepts one connection at a time and keeps no queue: concurrent requests wait in the kernel socket backlog. Each request carries its own deadline. A failed request publishes nothing.

### Protocol (version 1)

Newline-delimited JSON over one connection per request:

1. The client sends `{"version":1,"identity":"…","name":"000000010000000000000003","deadline":…}`.
2. The server replies `{"version":1,"ok":true,"length":16777216,"sha256":"…","id":"…"}`, then exactly `length` payload bytes, then half-closes its side.
3. The client checks length and SHA-256, writes the destination through a temporary file, `fsync`s it, renames it into place and answers `{"version":1,"id":"…","published":true}`.
4. Slot retention advances only after that acknowledgement.

### Identity digest

Every request carries a SHA-256 digest of the effective configuration: connection settings, SSL environment including digests of certificate files, and behavioral options. Credentials never cross the socket. A digest mismatch is rejected; the server never mixes two configurations in one process.

### WAL path

For each segment the server runs `IDENTIFY_SYSTEM`, validates the requested timeline against the fetched history, derives the LSN range from the filename and `-wal-segment-size`, and streams that range with `START_REPLICATION PHYSICAL`. Contiguity is enforced and the payload is truncated to the segment size, so an active segment ends in a zero-filled tail. History responses are capped at 1 MiB.

## Temporary slot

By default the server creates a temporary physical replication slot to retain WAL between requests.

- **Name:** `wal_fetch_` followed by 32 random lowercase hex digits, recorded in the server log.
- **Lifetime:** created on the first WAL request and held by the server's connection. PostgreSQL removes it when that connection ends or fails, including shutdown after `-idle-timeout`.
- **Retention:** advances after contiguous files are delivered, keeping the last delivered segment. It cannot recover files already recycled, and it does not protect WAL written before the slot was created.

Use `-no-slot` when retention is managed elsewhere.

After 5 failed WAL fetches without a successful publication, the slot is released and no new one is created. Once a WAL file is published and the replication exchange finishes successfully, the next WAL request can create a slot again. History requests do not affect the counter. `-slot-failure-limit N` changes the threshold; `0` disables it.

A PostgreSQL error while reading recycled WAL releases the slot immediately. The threshold also covers repeated local rejections, such as a future segment. A replacement slot cannot recover WAL recycled during the gap.

## Options

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

Changing connection, slot or logging settings requires restarting the local server:

```sh
kill -TERM "$(cat "$PGDATA/.wal-fetch/server.pid")"
```

The next request starts it again. For debugging, run the server in the foreground with the same options, without `WAL_NAME DESTINATION`:

```sh
wal-fetch -serve -pgdata /var/lib/postgresql/data
```

## Logs

Server events go to `<pgdata>/.wal-fetch/server.log`: startup, shutdown, slot lifecycle, file requests. Client errors go to stderr, which PostgreSQL captures for `restore_command`.

```conf
restore_command = '/opt/wal-fetch -log-file /var/log/postgresql/wal-fetch.log -syslog "%f" "%p" || pgbackrest --stanza=main archive-get "%f" "%p"'
```

The log directory must exist and be writable by the PostgreSQL OS user; the file must belong to it with mode `0600`. Syslog uses tag `wal-fetch`, facility `daemon`, and writes wherever the host syslog configuration sends it.

### Rotation

Rotation is external. Rename the file, then tell the running server to reopen it:

```sh
kill -USR1 "$(cat "$PGDATA/.wal-fetch/server.pid")"
```

The replacement must satisfy the same rules — a regular file, same owner, mode `0600` — so logrotate needs `create 0600 postgres postgres`. A failed reopen keeps the old descriptor and is reported in the log.

```conf
/var/log/postgresql/wal-fetch.log {
    daily
    rotate 14
    create 0600 postgres postgres
    postrotate
        pid=/var/lib/postgresql/data/.wal-fetch/server.pid
        if [ -f "$pid" ] && kill -0 "$(cat "$pid")" 2>/dev/null; then
            kill -USR1 "$(cat "$pid")"
        fi
    endpostrotate
}
```

Check the PID is alive before signalling: `server.pid` is removed when the server exits, and a stale PID can already belong to another process. When no server is running the hook does nothing, and the next request starts one that opens the new file.

`copytruncate` works without a signal because the server keeps writing to the same descriptor, but rename plus `SIGUSR1` is the supported path. Output inherited by the server at startup — stdout and stderr, used for startup failures and panics — keeps going to the rotated file.

## Security

All server state is in `<pgdata>/.wal-fetch`, created with mode `0700` and owned by the PostgreSQL OS user: `server.sock` (`0600`), `server.lock`, `server.pid` and `server.log`.

- Both sides check the Unix-socket peer with `SO_PEERCRED` and drop connections from a different OS UID.
- The lock and log files are opened with `O_NOFOLLOW` and must be regular files owned by the same UID with mode `0600`. The socket directory must be owned by that UID with mode `0700`.
- The socket carries filenames, digests and file bytes only; connection settings appear only inside the identity digest.
- The destination appears by `rename` after `fsync`, so PostgreSQL never sees a partial segment or history file. A failed request leaves the destination unchanged.
- Multi-host connection strings are rejected. The running server pins the source system identifier and rejects a different cluster. A timeline change within the same cluster restarts the server.
- Leftover staging files are removed at startup by the lock owner; nothing from a previous run is reused.

## Troubleshooting

Client errors go to stderr with the `wal-fetch:` prefix. Server-side rejections appear on the client as `server rejected request: …` and in `server.log` as `rejected error=…`.

| Message | Cause | Action |
|---|---|---|
| `source configuration mismatch` | Client settings differ from the running server's | Restart the server, then retry |
| `socket peer UID mismatch` | Socket peer runs as another OS user | One OS user owns `$PGDATA`; check who started the other server |
| `source system identifier changed` | The source address now belongs to a different cluster | Check the source address; the failed request allows archive fallback |
| `source system or timeline changed; start a new server` | The same cluster moved to a new timeline | The server restarts automatically |
| `server startup failed` | The freshly started server failed | Read `server.log` |
| `PGDATA path is too long for Unix socket` | Path over the 108-character Unix-socket limit | Use a shorter PGDATA |
| `server already running` | Foreground `-serve` while another server owns the lock | Use the running server, or stop it first |

The temporary slot disappears when its owning server exits, which is expected after `-idle-timeout` or `kill -TERM`:

```sql
SELECT slot_name, active, restart_lsn
FROM pg_replication_slots
WHERE slot_name LIKE 'wal_fetch_%';
```

## Limits

Linux only. Losing the server connection also loses the slot's retention. An active segment is a snapshot of flushed WAL with a zero-filled tail.

Syslog needs a local receiver at startup. Delivery is best effort: a stalled receiver can lose syslog copies, file logging continues.

## Build and test

Linux, Go 1.25. Recovery tests need Python 3, PostgreSQL 16 or 18 tools on `PATH` and a non-root user.

```sh
go build -o wal-fetch .
go test ./...
python3 -m venv tests/integration/.venv
. tests/integration/.venv/bin/activate
python -m pip install -r tests/integration/requirements.txt
python -m pytest tests/integration -v
WAL_FETCH_TEST_SEGMENT_MB=1 python -m pytest tests/integration -v
```

With PostgreSQL 18 tools on `PATH`, set `WAL_FETCH_TEST_PG_VERSION=18` when running pytest.

Release builds set the version reported by `-version`:

```sh
go build -ldflags "-X main.buildVersion=v1.0.0" -o wal-fetch .
```

A build from source reports `dev`.

CI runs unit, race and vet checks, and real recovery with 16 MiB and 1 MiB segments, with and without a slot. A separate job runs the same integration suite against a `-race` binary. It covers timeline history, recovery to the target checkpoint, slot lifecycle and failure handling.

[MIT License](LICENSE). [Third-party licenses](THIRD_PARTY_NOTICES.txt).
