package main

import (
	"errors"
	"fmt"
	"log/syslog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Test binaries can run the real detached-child path against a private syslog socket.
func TestMain(m *testing.M) {
	if os.Getenv("WAL_FETCH_LOG_TEST_CHILD") == "1" {
		openLocalSyslog = func() (*syslog.Writer, error) {
			return syslog.Dial("unixgram", os.Getenv("WAL_FETCH_LOG_TEST_SOCKET"), syslog.LOG_DAEMON|syslog.LOG_INFO, "wal-fetch")
		}
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestLogFiles(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "public"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing/secret-path", "link", "fifo", ".", "public"} {
		start := time.Now()
		f, err := openLogFile(filepath.Join(dir, name))
		if err == nil {
			f.Close()
			t.Fatalf("accepted %s", name)
		}
		if time.Since(start) > time.Second || strings.Contains(err.Error(), "secret-path") {
			t.Fatal(err)
		}
	}
	b, _ := os.ReadFile(target)
	if string(b) != "keep" {
		t.Fatal("symlink target changed")
	}
	st, _ := os.Stat(filepath.Join(dir, "public"))
	if st.Mode().Perm() != 0644 {
		t.Fatal("changed existing permissions")
	}
	f, err := openLogFile(target)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("-append")
	f.Close()
	b, _ = os.ReadFile(target)
	if string(b) != "keep-append" {
		t.Fatal("log was not appended")
	}
}

func TestLocalSyslog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "syslog.sock")
	sink, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	original := openLocalSyslog
	t.Cleanup(func() { openLocalSyslog = original })
	openLocalSyslog = func() (*syslog.Writer, error) {
		return syslog.Dial("unixgram", path, syslog.LOG_DAEMON|syslog.LOG_INFO, "wal-fetch")
	}
	logs, err := openServerLog(options{logFile: filepath.Join(dir, "server.log"), syslog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	logs.logger.Print("server started")
	buf := make([]byte, 4096)
	_ = sink.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := sink.ReadFromUnix(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(buf[:n]), "<30>") || !strings.Contains(string(buf[:n]), fmt.Sprintf("wal-fetch[%d]", os.Getpid())) || !strings.Contains(string(buf[:n]), "server started") {
		t.Fatalf("bad local syslog record: %q", buf[:n])
	}
	file, _ := os.ReadFile(logs.file.Name())
	if !strings.Contains(string(file), time.Now().UTC().Format("2006/01/02")) || !strings.Contains(string(file), "server started") {
		t.Fatalf("bad file record: %s", file)
	}
	// A failed file write must not suppress the independent syslog write.
	logs.file.Close()
	logs.logger.Print("after file failure")
	n, _, err = sink.ReadFromUnix(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "after file failure") {
		t.Fatalf("%s %v", buf[:n], err)
	}
	// A failed syslog write must not fail logging or discard the file record.
	logs.file, err = openLogFile(filepath.Join(dir, "still-file.log"))
	if err != nil {
		t.Fatal(err)
	}
	sink.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	logs.logger.Print("after syslog failure")
	file, _ = os.ReadFile(logs.file.Name())
	if !strings.Contains(string(file), "after syslog failure") {
		t.Fatal("file record missing")
	}
	openLocalSyslog = func() (*syslog.Writer, error) { return nil, errors.New("secret-syslog-failure") }
	if _, err = openServerLog(options{logFile: filepath.Join(dir, "failed.log"), syslog: true}); err == nil || err.Error() != "cannot initialize local syslog" {
		t.Fatalf("unsafe init result: %v", err)
	}
}

func TestLogStartup(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("WAL_FETCH_LOG_TEST_CHILD", "1")
	t.Setenv("WAL_FETCH_LOG_TEST_SOCKET", filepath.Join(dir, "absent.sock"))
	t.Setenv("PGPASSWORD", "secret-password")
	t.Setenv("PGHOST", "127.0.0.1")
	t.Setenv("PGSSLMODE", "disable")
	for _, args := range [][]string{
		{"-log-file", filepath.Join(dir, "missing", "secret-name.log")},
		{"-syslog"},
	} {
		start := time.Now()
		err := run(append(append([]string{"-pgdata", dir, "-timeout", "10s"}, args...), "00000002.history", filepath.Join(dir, "out")))
		if err == nil || strings.Contains(err.Error(), "secret") || time.Since(start) > 3*time.Second {
			t.Fatalf("startup not fast/safe: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, ".wal-fetch/server.sock")); !os.IsNotExist(err) {
			t.Fatal("failed startup left a socket")
		}
	}
}

func TestLogReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.log")
	logs, err := openServerLog(options{logFile: path})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	logs.logger.Print("before rotation")
	if err := os.Rename(path, filepath.Join(dir, "server.log.1")); err != nil {
		t.Fatal(err)
	}
	if err := logs.reopen(); err != nil {
		t.Fatal(err)
	}
	logs.logger.Print("after rotation")
	rotated, err := os.ReadFile(filepath.Join(dir, "server.log.1"))
	if err != nil || !strings.Contains(string(rotated), "before rotation") || strings.Contains(string(rotated), "after rotation") {
		t.Fatalf("rotation split records incorrectly: %q", rotated)
	}
	fresh, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(fresh), "after rotation") {
		t.Fatalf("missing record after reopen: %q", fresh)
	}
	// A failed reopen keeps writing to the current descriptor.
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if err := logs.reopen(); err == nil {
		t.Fatal("reopened onto a permissive file")
	}
	logs.logger.Print("after failed reopen")
	rotated, err = os.ReadFile(filepath.Join(dir, "server.log.1"))
	if err != nil || strings.Contains(string(rotated), "after failed reopen") {
		t.Fatalf("records leaked past a failed reopen: %q", rotated)
	}
	fresh, err = os.ReadFile(path)
	if err != nil || !strings.Contains(string(fresh), "after failed reopen") {
		t.Fatalf("failed reopen lost the current descriptor: %q", fresh)
	}
}
