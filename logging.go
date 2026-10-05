package main

import (
	"errors"
	"fmt"
	"log"
	"log/syslog"
	"os"
	"sync"
	"syscall"
	"time"
)

// Production always connects to the local syslog service.
var openLocalSyslog = func() (*syslog.Writer, error) {
	return syslog.New(syslog.LOG_DAEMON|syslog.LOG_INFO, "wal-fetch")
}

type serverLog struct {
	mu     sync.Mutex // Serializes file writes with rotation reopen and Close.
	path   string
	file   *os.File
	closed bool
	syslog *syslog.Writer
	logger *log.Logger
	queue  chan string
	done   chan struct{} // Closed when the worker exits.
}

func openLogFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, errors.New("cannot open server log file")
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
		f.Close()
		return nil, errors.New("server log must be a regular file owned by this UID with mode 0600")
	}
	return f, nil
}

func boundedLocalSyslog(timeout time.Duration) (*syslog.Writer, error) {
	if timeout <= 0 || timeout > time.Second {
		timeout = time.Second
	}
	type result struct {
		writer *syslog.Writer
		err    error
	}
	ready := make(chan result)
	stop := make(chan struct{})
	defer close(stop)
	factory := openLocalSyslog
	go func() {
		writer, err := factory()
		select {
		case ready <- result{writer, err}:
		case <-stop:
			if writer != nil {
				writer.Close()
			}
		}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ready:
		if r.err == nil && r.writer != nil {
			return r.writer, nil
		}
		if r.writer != nil {
			r.writer.Close()
		}
	case <-timer.C:
	}
	return nil, errors.New("cannot initialize local syslog")
}

func openServerLog(o options) (*serverLog, error) {
	f, err := openLogFile(o.logFile)
	if err != nil {
		return nil, err
	}
	l := &serverLog{path: o.logFile, file: f}
	if o.syslog {
		l.syslog, err = boundedLocalSyslog(o.timeout)
		if err != nil {
			f.Close()
			return nil, err
		}
		l.queue, l.done = make(chan string, 32), make(chan struct{})
		go func() {
			defer close(l.done)
			defer l.syslog.Close()
			for message := range l.queue {
				_, _ = l.syslog.Write([]byte(message))
			}
		}()
	}
	l.logger = log.New(l, fmt.Sprintf("wal-fetch[%d] ", os.Getpid()), log.Ldate|log.Ltime|log.LUTC|log.Lmsgprefix)
	return l, nil
}

func (l *serverLog) Write(b []byte) (int, error) {
	// File writes are synchronous; a slow syslog drops queued copies and never blocks WAL delivery.
	l.mu.Lock()
	if !l.closed {
		_, _ = l.file.Write(b)
	}
	l.mu.Unlock()
	if l.queue != nil {
		select {
		case l.queue <- string(b):
		default:
		}
	}
	return len(b), nil
}

// reopen swaps the descriptor after an external rotation. A record is written
// entirely to one file, and a failed reopen keeps the current descriptor.
func (l *serverLog) reopen() error {
	f, err := openLogFile(l.path)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		f.Close()
		return errors.New("server log is closed")
	}
	old := l.file
	l.file = f
	old.Close()
	return nil
}

func (l *serverLog) Close() {
	// Close never waits for a syslog write, which can block indefinitely on a stalled receiver.
	if l.queue != nil {
		close(l.queue)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	l.file.Close()
}

func (s *source) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}
