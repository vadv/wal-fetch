package main

import (
	"errors"
	"fmt"
	"log/syslog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBoundedSyslogInitialization(t *testing.T) {
	original := openLocalSyslog
	gate, returned := make(chan struct{}), make(chan struct{})
	openLocalSyslog = func() (*syslog.Writer, error) {
		defer close(returned)
		<-gate
		return nil, errors.New("private fixture error")
	}
	t.Cleanup(func() {
		close(gate) // Let the abandoned initialization goroutine finish.
		<-returned
		openLocalSyslog = original
	})
	start := time.Now()
	logs, err := openServerLog(options{logFile: filepath.Join(t.TempDir(), "server.log"), syslog: true, timeout: 20 * time.Millisecond})
	if logs != nil || err == nil || err.Error() != "cannot initialize local syslog" || time.Since(start) > time.Second {
		t.Fatalf("unbounded or unsafe initialization: logger=%v error=%v", logs, err)
	}
}

func TestBoundedSyslogBackpressure(t *testing.T) {
	// A private Linux abstract socket avoids touching the host's syslog service.
	address := &net.UnixAddr{Name: fmt.Sprintf("@wal-fetch-log-%d-%d", os.Getpid(), time.Now().UnixNano()), Net: "unixgram"}
	sink, err := net.ListenUnixgram("unixgram", address)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	probe, err := net.DialUnix("unixgram", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := probe.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	full := false
	for i := 0; i < 4096; i++ {
		if _, err := probe.Write([]byte("fill")); err != nil {
			var ne net.Error
			full = errors.As(err, &ne) && ne.Timeout()
			break
		}
	}
	if !full {
		t.Fatal("private receiver queue did not reach backpressure")
	}
	original := openLocalSyslog
	openLocalSyslog = func() (*syslog.Writer, error) {
		return syslog.Dial("unixgram", address.Name, syslog.LOG_DAEMON|syslog.LOG_INFO, "wal-fetch")
	}
	t.Cleanup(func() { openLocalSyslog = original })
	path := filepath.Join(t.TempDir(), "server.log")
	logs, err := openServerLog(options{logFile: path, syslog: true}) // Zero fixture timeout remains valid.
	if err != nil {
		t.Fatal(err)
	}
	cleanup := make(chan struct{})
	go func() {
		defer close(cleanup) // Simulates caller cleanup after logging.
		for i := 0; i < 128; i++ {
			logs.logger.Printf("event-%03d", i)
		}
		logs.Close()
	}()
	select {
	case <-cleanup:
	case <-time.After(time.Second):
		t.Error("syslog backpressure blocked Write, Close or caller cleanup")
	}
	// Closing the private receiver releases any blocked stdlib Write, allowing
	// the asynchronous worker to stop without leaking a test goroutine.
	sink.Close()
	select {
	case <-cleanup:
	case <-time.After(time.Second):
		t.Fatal("caller did not stop after releasing the private receiver")
	}
	select {
	case <-logs.done:
	case <-time.After(time.Second):
		t.Fatal("syslog worker did not stop after releasing the private receiver")
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.Count(string(data), "event-") != 128 {
		t.Fatalf("syslog backpressure lost file records: %v", err)
	}
}
