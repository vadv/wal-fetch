# wal-fetch Unix socket PoC

Fetch PostgreSQL WAL for `restore_command` using a `LOGIN REPLICATION` role. One Linux binary starts a private local server on demand; later calls reuse its connection and temporary physical replication slot. Requests are serial. There is no HTTP service, historical WAL cache, or background prefetch.

Licensed under the [MIT License](LICENSE). Dependency and runtime licenses are listed in [THIRD_PARTY_NOTICES.txt](THIRD_PARTY_NOTICES.txt).

## Run

```sh
export PGHOST=leader PGPORT=5432 PGUSER=replicator
export PGPASSFILE=/var/lib/postgresql/.pgpass
export PGSSLMODE=verify-full
./wal-fetch -pgdata /var/lib/postgresql/data WAL_NAME DESTINATION
```

Use a `.pgpass` entry with database `replication`, mode `0600`. `PGPASSWORD` also works; there is no password argument. pgconn handles authentication and TLS environment settings, including CA/client certificate files. Only physical replication commands are used: no ordinary SQL, `pg_read_binary_file`, or `SHOW`.

```conf
restore_command = '/opt/wal-fetch -pgdata /var/lib/postgresql/data "%f" "%p" || pgbackrest --stanza=main archive-get "%f" "%p"'
```

Paths, host and stanza are examples; provide the environment to the PostgreSQL process. The binary is not installed by this project.

| Option | Default | Meaning |
|---|---|---|
| `-pgdata` | `PGDATA`, then working directory | Existing directory identifying this restore |
| `-h`, `-p`, `-U` | pgconn environment | One fixed source endpoint and replication role |
| `-wal-segment-size` | `16MB` | Explicit source segment size, e.g. `1MB` or `64MB` |
| `-timeout` | `30s` | Client operation deadline, including queue/startup/transfer |
| `-idle-timeout` | `5m` | Server exits after this long without accepted work; e.g. `1m` |
| `-serve` | off | Run the server in foreground instead of fetching a file |

`MB` means MiB. Sizes must be powers of two from 1 MiB through 1 GiB and **must match the source cluster**; no size discovery is performed. The server's request time limit comes from its starting invocation; later clients may use shorter deadlines. Restart the server to increase that limit or change source settings.

## Lifecycle and retention

The server holds an inherited `flock` for its entire lifetime. Concurrent cold starts produce one owner; a later owner can replace a stale socket and remove abandoned staging files. `$PGDATA/.wal-fetch/` must be owned by the current UID with mode `0700`; the Unix socket is `0600`, and both peers check Linux credentials. The complete socket path must be shorter than 108 bytes. This directory also contains a lock, PID file and sanitized server log. Never remove the lock file while a server is alive.

The first WAL request creates a uniquely named `TEMPORARY PHYSICAL RESERVE_WAL` slot. Every WAL stream uses that slot on its owning connection; `CopyDone` is drained through `ReadyForQuery`, retaining the session between requests. Explicit `.history` requests use short-lived, separate replication connections, so a missing history probe does not delete the owner's slot. The source system identifier, endpoint and timeline are pinned. A configuration digest prevents silently reusing another source/segment configuration. Configuration and password rotation require a server restart.

After verified client publication, the server advances `restart_lsn` conservatively to the **start of the latest segment with contiguous published coverage**. It never claims replay (`apply=0`). Gaps, old repeats, history requests and invalid/missing ACKs do not advance retention. A newly created slot starts at PostgreSQL's checkpoint redo boundary; it does not protect all older WAL or recover recycled files. Coverage before that boundary cannot move the slot backwards.

The active segment is a snapshot: bytes through captured flush/fork, followed by zeros. Zero padding is never acknowledged as received WAL. Retaining that segment supports a later refresh, but does not update a file already returned to PostgreSQL. Old files are not cached; a request behind the retention floor may need archive fallback.

Idle expiry closes the owning connection, releases temporary retention and removes the socket. Accepted active/queued requests are processed before idle expiry. The next invocation starts a new server. `ERROR`, connection loss, server death or idle expiry breaks retention continuity: reconnecting creates a fresh slot and cannot guarantee that intervening WAL survived. Local rejections before streaming (for example, a future WAL or unrelated timeline) preserve the healthy owner and slot. PostgreSQL errors on the owning connection do not. Owner failures are logged as `retention lost`. The failing request is reported honestly; a later request may reconnect. A gracefully stopped server can be started again with new settings.

This PoC was exercised on PostgreSQL **16.11**, Linux/amd64. Timeline IDs are limited to signed 32-bit by pglogrepl. Automatic failover, changing timeline within one server lifetime, and Patroni management were not tested or implemented. Patroni 4.1.5 [excludes temporary slots from slot management](https://github.com/patroni/patroni/blob/v4.1.5/patroni/postgresql/slots.py#L252); `ignore_slots` is not needed for this temporary slot. Primary replacement still loses the session and slot. `max_replication_slots`, `max_wal_senders`, disk space and `max_slot_wal_keep_size` still constrain retention.

## Local protocol, version 1

Each connection carries one request. JSON messages are newline-terminated and limited to 4096 bytes; unknown fields are rejected.

| Direction | Message |
|---|---|
| Client → server | `{"version":1,"identity":"…","name":"…","deadline":…}` |
| Server → client, success | `{"version":1,"ok":true,"length":…,"sha256":"…","id":"…"}` followed by exactly `length` raw bytes |
| Server → client, failure | `{"version":1,"ok":false,"length":0,"error":"…"}` |
| Client → server, after publication | `{"version":1,"id":"…","published":true}` |

`deadline` is an absolute Unix timestamp in nanoseconds. `identity` is a digest of the effective source configuration, including credentials; raw credentials and the destination path are not sent. `id` is a random identifier binding the ACK to this response. The server half-closes its write side at the payload boundary. If an idle server disconnects before a response header, the client can reconnect once under its original deadline.

The client alone creates an adjacent temporary file, verifies the length, EOF and checksum, syncs it and atomically renames it to `%p`. Only then does it send `{version, id, published:true}`. Publication succeeds even if this final ACK is lost; retention then remains conservative. Malformed/truncated transfers leave the old destination untouched. The server never writes `%p`, so a timed-out client cannot cause a late server write over archive fallback. Killing a client can leave its own temporary file; network deadlines do not interrupt a blocked filesystem syscall.

The server stages one verified request at a time and computes the allowed retention boundary itself. Clients cannot supply an ACK LSN. There is no durable cache or replay acknowledgement.

## Build, test and recovery CI

Go **1.25** and Python 3 are required. Local recovery tests also need PostgreSQL 16 CLI tools on `PATH` and an unprivileged user. No existing PostgreSQL service is used.

```sh
go test -count=1 ./...
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o wal-fetch .
WAL_FETCH_TEST_EVIDENCE=/tmp/wal-fetch-evidence python3 integration.py
WAL_FETCH_TEST_SEGMENT_MB=1 WAL_FETCH_TEST_EVIDENCE=/tmp/wal-fetch-evidence python3 integration.py
```

The GitHub Actions workflow runs on push, pull request and manual dispatch, with read-only repository permissions. Its Ubuntu 24.04 matrix uses Go 1.25 and the [preinstalled PostgreSQL 16 binaries](https://github.com/actions/runner-images/blob/main/images/ubuntu/Ubuntu2404-Readme.md#postgresql), checking the major version before running. `initdb` runs as the ordinary runner user; there are no service containers, shared database services or database passwords in Actions secrets.

Both **16 MiB default** and **1 MiB override** jobs run unit/race/vet/build checks and real PostgreSQL recovery. The integration harness verifies completed and quiet active WAL, `.history`, ancestor/new timelines, recovery to the target checkpoint with shutdown, slot advancement/reuse, concurrent startup, malformed or missing ACKs, local rejection without slot loss, owner failure, idle/autorespawn, stale sockets and timeout without a late destination write. SCRAM/passfile access is tested while ordinary SQL is rejected. TLS certificate and Patroni deployment tests remain separate.

The harness stops its own servers and clusters in `finally`; incomplete cleanup makes the process fail. After successful cleanup it removes temporary PGDATA and password files. Only a JSON result and bounded, redacted diagnostic logs are retained (`WAL_FETCH_TEST_EVIDENCE`, or a separate temporary diagnostics directory). CI uploads these diagnostics with `if: always()` and seven-day retention, never PGDATA, password files or the built binary. On a cleanup failure, live cluster data is left untouched locally and is still excluded from artifacts.
