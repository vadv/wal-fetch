# wal-fetch

Fetch WAL files and timeline history from a PostgreSQL primary for `restore_command`, using a `LOGIN REPLICATION` role.

The first call starts a local Unix-socket server; subsequent calls reuse its PostgreSQL connection.

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

Changing connection, slot or logging settings requires restarting the local server. Stop it with `kill -TERM "$(cat "$PGDATA/.wal-fetch/server.pid")"`; the next request starts it again.

## Logs

Server events go to `<pgdata>/.wal-fetch/server.log` by default: startup/shutdown, slot lifecycle and file requests. Client errors go to stderr, which PostgreSQL captures when running `restore_command`.

To use a different file and also send events to local syslog:

```conf
restore_command = '/opt/wal-fetch -log-file /var/log/postgresql/wal-fetch.log -syslog "%f" "%p" || pgbackrest --stanza=main archive-get "%f" "%p"'
```

The log directory must exist and be writable by the PostgreSQL OS user; the file must belong to that user with mode `0600`. Syslog uses tag `wal-fetch`, facility `daemon`; the host's syslog configuration controls its destination.

## Limits

Linux only. The server follows one fixed primary and timeline. Losing its connection also loses the temporary slot's WAL retention. An active segment is a snapshot of flushed WAL with a zero-filled tail.

Syslog requires a local receiver at startup. Delivery is best effort: a stalled receiver can lose syslog copies; file logging continues. Log rotation is external; restart the local server after renaming its log file.

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

CI runs unit/race/vet checks and real recovery with 16 MiB and 1 MiB WAL segments, with and without a slot. It checks timeline history, recovery to the target checkpoint, slot lifecycle and failure handling.

[MIT License](LICENSE). [Third-party licenses](THIRD_PARTY_NOTICES.txt).
