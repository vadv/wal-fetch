// Linux proof of concept: one detached serial WAL server per PGDATA.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type options struct {
	cfg           *pgconn.Config
	dir, identity string
	size          uint64
	timeout, idle time.Duration
	serve         bool
	args          []string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "wal-fetch:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("wal-fetch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	host := fs.String("h", "", "source host")
	port := fs.String("p", "", "source port")
	user := fs.String("U", "", "replication user")
	data := fs.String("pgdata", os.Getenv("PGDATA"), "socket scope (default PGDATA or working directory)")
	sizeText := fs.String("wal-segment-size", "16MB", "source WAL segment size")
	o := options{args: args}
	fs.DurationVar(&o.timeout, "timeout", 30*time.Second, "total client operation timeout")
	fs.DurationVar(&o.idle, "idle-timeout", 5*time.Minute, "server idle timeout")
	fs.BoolVar(&o.serve, "serve", false, "run server in foreground")
	help := fs.Bool("help", false, "show usage")
	if fs.Parse(args) != nil {
		return errors.New("invalid arguments; use -help")
	}
	if *help {
		fmt.Fprintln(os.Stderr, "Usage: wal-fetch [-pgdata DIR] [-h HOST] [-p PORT] [-U USER] [-timeout 30s] [-idle-timeout 5m] [-wal-segment-size 16MB] WAL_NAME DESTINATION\nServer: wal-fetch -serve [same options]\nAuthentication: PGHOST PGPORT PGUSER PGPASSWORD PGPASSFILE PGSSLMODE; no password argument.")
		return nil
	}
	if o.timeout <= 0 || o.idle <= 0 || (!o.serve && fs.NArg() != 2) {
		return errors.New("expected WAL_NAME DESTINATION and positive timeouts")
	}
	var err error
	o.size, err = segmentSize(*sizeText)
	if err != nil {
		return err
	}
	if !o.serve {
		if _, err = parseRequest(fs.Arg(0)); err != nil {
			return err
		}
	}
	if *data == "" {
		*data, err = os.Getwd()
		if err != nil {
			return errors.New("working directory unavailable")
		}
	}
	root, err := filepath.Abs(*data)
	if err != nil {
		return errors.New("invalid PGDATA")
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return errors.New("PGDATA must exist")
	}
	o.dir = filepath.Join(root, ".wal-fetch")
	if len(filepath.Join(o.dir, "server.sock")) >= 108 {
		return errors.New("PGDATA path is too long for Unix socket")
	}
	if err = privateDirectory(o.dir); err != nil {
		return err
	}
	dsn := "replication=yes dbname=replication application_name=wal-fetch-unix target_session_attrs=any"
	for _, kv := range [][2]string{{"host", *host}, {"port", *port}, {"user", *user}} {
		if kv[1] != "" {
			dsn += " " + kv[0] + "='" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(kv[1]) + "'"
		}
	}
	o.cfg, err = pgconn.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid source connection settings")
	}
	for _, f := range o.cfg.Fallbacks {
		if f.Host != o.cfg.Host || f.Port != o.cfg.Port {
			return errors.New("use one fixed source host and port")
		}
	}
	// A digest binds every request to the effective credentials/configuration;
	// neither credentials nor a connection string are sent over the Unix socket.
	identity := []any{o.cfg.Host, o.cfg.Port, o.cfg.User, o.cfg.Password, o.cfg.Database, o.cfg.RuntimeParams, o.size, o.idle.String(), root}
	for _, key := range []string{"PGSSLMODE", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY", "PGSSLCRL", "PGSSLSNI", "PGCHANNELBINDING", "PGSERVICE", "PGSERVICEFILE"} {
		value := os.Getenv(key)
		identity = append(identity, key, value)
		if strings.Contains(key, "CERT") || key == "PGSSLKEY" || key == "PGSSLCRL" {
			if b, e := os.ReadFile(value); e == nil {
				sum := sha256.Sum256(b)
				identity = append(identity, hex.EncodeToString(sum[:]))
			}
		}
	}
	b, _ := json.Marshal(identity)
	sum := sha256.Sum256(b)
	o.identity = hex.EncodeToString(sum[:])
	if o.serve {
		return serve(o)
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	name := strings.ToUpper(strings.TrimSuffix(fs.Arg(0), ".history"))
	if strings.HasSuffix(fs.Arg(0), ".history") {
		name += ".history"
	}
	return client(ctx, o, name, fs.Arg(1))
}

func privateDirectory(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return errors.New("cannot create private socket directory")
	}
	st, err := os.Lstat(path)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
		return errors.New("socket directory must be owned by this UID with mode 0700")
	}
	return nil
}

func lockFile(dir string) (*os.File, error) {
	fd, err := syscall.Open(filepath.Join(dir, "server.lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, errors.New("cannot open server lock")
	}
	f := os.NewFile(uintptr(fd), "server.lock")
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
		f.Close()
		return nil, errors.New("unsafe server lock")
	}
	return f, nil
}

func sameUID(c *net.UnixConn) bool {
	raw, err := c.SyscallConn()
	if err != nil {
		return false
	}
	var uid uint32
	var inner error
	err = raw.Control(func(fd uintptr) {
		var cred *syscall.Ucred
		cred, inner = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if inner == nil {
			uid = cred.Uid
		}
	})
	return err == nil && inner == nil && uid == uint32(os.Getuid())
}

func connectLocal(ctx context.Context, o options) (*net.UnixConn, error) {
	for ctx.Err() == nil {
		d := net.Dialer{}
		if c, err := d.DialContext(ctx, "unix", filepath.Join(o.dir, "server.sock")); err == nil {
			u := c.(*net.UnixConn)
			if !sameUID(u) {
				u.Close()
				return nil, errors.New("socket peer UID mismatch")
			}
			return u, nil
		}
		f, err := lockFile(o.dir)
		if err != nil {
			return nil, err
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			// Recheck under the lock; only its lifetime owner may replace a stale socket.
			c, e := net.DialTimeout("unix", filepath.Join(o.dir, "server.sock"), 20*time.Millisecond)
			if e == nil {
				c.Close()
				f.Close()
			} else {
				executable, e := os.Executable()
				if e != nil {
					f.Close()
					return nil, errors.New("cannot locate executable")
				}
				log, e := os.OpenFile(filepath.Join(o.dir, "server.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW, 0600)
				if e != nil {
					f.Close()
					return nil, errors.New("cannot open server log")
				}
				cmd := exec.Command(executable, append([]string{"-serve"}, o.args...)...)
				cmd.Env = append(os.Environ(), "WAL_FETCH_INHERITED_LOCK=1")
				cmd.ExtraFiles = []*os.File{f}
				cmd.Stdout = log
				cmd.Stderr = log
				cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
				e = cmd.Start()
				log.Close()
				f.Close() // Do not unlock the inherited open-file description.
				if e != nil {
					return nil, errors.New("cannot start detached server")
				}
				_ = cmd.Process.Release()
			}
		} else {
			f.Close()
			if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
				return nil, errors.New("server lock failed")
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil, errors.New("server startup deadline exceeded")
}
