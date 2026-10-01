package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

const protocolVersion = 1
const maxLine = 4096
const maxHistory = 1 << 20

type wireRequest struct {
	Version  int    `json:"version"`
	Identity string `json:"identity"`
	Name     string `json:"name"`
	Deadline int64  `json:"deadline"`
}
type wireHeader struct {
	Version int    `json:"version"`
	OK      bool   `json:"ok"`
	Length  int64  `json:"length"`
	SHA256  string `json:"sha256,omitempty"`
	ID      string `json:"id,omitempty"`
	Error   string `json:"error,omitempty"`
}
type wireACK struct {
	Version   int    `json:"version"`
	ID        string `json:"id"`
	Published bool   `json:"published"`
}

func readJSON(r *bufio.Reader, value any) error {
	line, err := r.ReadSlice('\n')
	if err == io.EOF && len(line) == 0 {
		return io.EOF
	}
	if err != nil {
		return fmt.Errorf("invalid or incomplete protocol line: %w", err)
	}
	if len(line) > maxLine {
		return errors.New("invalid or incomplete protocol line")
	}
	d := json.NewDecoder(bytes.NewReader(line))
	d.DisallowUnknownFields()
	if d.Decode(value) != nil {
		return errors.New("invalid protocol JSON")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("extra protocol data")
	}
	return nil
}
func writeJSON(w io.Writer, value any) error {
	b, err := json.Marshal(value)
	if err != nil || len(b)+1 > maxLine {
		return errors.New("invalid protocol message")
	}
	b = append(b, '\n')
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}
func token() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("randomness unavailable")
	}
	return hex.EncodeToString(b[:])
}

func client(ctx context.Context, o options, name, dest string) error {
	var c *net.UnixConn
	var r *bufio.Reader
	var h wireHeader
	var err error
	deadline, _ := ctx.Deadline()
	for attempt := 0; attempt < 2; attempt++ {
		c, err = connectLocal(ctx, o)
		if err != nil {
			return err
		}
		_ = c.SetDeadline(deadline)
		err = writeJSON(c, wireRequest{protocolVersion, o.identity, name, deadline.UnixNano()})
		r = bufio.NewReaderSize(c, maxLine)
		if err == nil {
			err = readJSON(r, &h)
		}
		if err == nil {
			break
		}
		c.Close()
		// An idle server may close just after Dial succeeded. No header means
		// nothing was published, so one reconnect under the same deadline is safe.
		if attempt != 0 || !(errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)) {
			return errors.New("invalid server response")
		}
	}
	defer c.Close()
	if h.Version != protocolVersion {
		return errors.New("server protocol version mismatch")
	}
	if !h.OK {
		if h.Error == "source configuration mismatch" {
			return errors.New(h.Error)
		}
		return fmt.Errorf("server rejected request: %s", safeLocalMessage(h.Error))
	}
	req, err := parseRequest(name)
	if err != nil {
		return err
	}
	if len(h.ID) != 32 || len(h.SHA256) != 64 || h.Length <= 0 || (!req.history && h.Length != int64(o.size)) || (req.history && h.Length > maxHistory) {
		return errors.New("invalid payload header")
	}
	if _, err = hex.DecodeString(h.ID); err != nil {
		return errors.New("invalid response ID")
	}
	if _, err = hex.DecodeString(h.SHA256); err != nil {
		return errors.New("invalid payload digest")
	}
	err = publish(ctx, dest, func(f *os.File) error {
		hash := sha256.New()
		if _, e := io.CopyN(io.MultiWriter(f, hash), r, h.Length); e != nil {
			return errors.New("incomplete payload")
		}
		if _, e := r.ReadByte(); e != io.EOF {
			return errors.New("extra payload or missing stream end")
		}
		if hex.EncodeToString(hash.Sum(nil)) != h.SHA256 {
			return errors.New("payload checksum mismatch")
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Publication belongs exclusively to this client. Losing this ACK cannot undo it.
	_ = writeJSON(c, wireACK{protocolVersion, h.ID, true})
	return nil
}
func safeLocalMessage(s string) string {
	// The server emits only locally constructed errors, but don't echo arbitrary peer text.
	if len(s) > 160 {
		return "operation failed"
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return "operation failed"
		}
	}
	return s
}

func serve(o options) (serveErr error) {
	var ready *os.File
	if os.Getenv("WAL_FETCH_INHERITED_LOCK") == "1" {
		ready = os.NewFile(4, "server.ready")
		defer func() {
			if serveErr != nil {
				fmt.Fprintln(ready, safeLocalMessage(serveErr.Error()))
			}
			ready.Close()
		}()
	}
	logs, err := openServerLog(o)
	if err != nil {
		return err
	}
	defer logs.Close()
	var lock *os.File
	if os.Getenv("WAL_FETCH_INHERITED_LOCK") == "1" {
		lock = os.NewFile(3, "server.lock")
	} else {
		lock, err = lockFile(o.dir)
		if err != nil {
			return err
		}
		if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
			lock.Close()
			return errors.New("server already running")
		}
	}
	defer lock.Close()
	lockStat, e := lock.Stat()
	pathStat, e2 := os.Stat(filepath.Join(o.dir, "server.lock"))
	if e != nil || e2 != nil || !os.SameFile(lockStat, pathStat) || syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return errors.New("invalid inherited server lock")
	}
	// No cache survives a crash. The lifetime lock makes stale staging cleanup safe.
	stale, err := filepath.Glob(filepath.Join(o.dir, "stage-*"))
	if err != nil {
		return errors.New("cannot inspect staging directory")
	}
	for _, path := range stale {
		if err := os.Remove(path); err != nil {
			return errors.New("cannot remove stale staging file")
		}
	}
	path := filepath.Join(o.dir, "server.sock")
	if st, e := os.Lstat(path); e == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return errors.New("refusing to replace non-socket")
		}
		if os.Remove(path) != nil {
			return errors.New("cannot remove stale socket")
		}
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return errors.New("cannot listen on Unix socket")
	}
	defer listener.Close()
	if os.Chmod(path, 0600) != nil {
		return errors.New("cannot secure Unix socket")
	}
	pidpath := filepath.Join(o.dir, "server.pid")
	if os.WriteFile(pidpath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600) != nil {
		return errors.New("cannot write server PID")
	}
	defer os.Remove(pidpath)
	s := source{o: o, logger: logs.logger}
	defer s.close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	go func() { <-ctx.Done(); listener.Close() }()
	s.logf("server started")
	defer s.logf("server stopped")
	if ready != nil {
		_, _ = ready.WriteString("ready\n")
		ready.Close()
	}
	for {
		// The serial accept loop has no internal queue. Kernel-queued clients are
		// accepted immediately; the idle clock runs only while waiting in Accept.
		_ = listener.SetDeadline(time.Now().Add(o.idle))
		c, e := listener.AcceptUnix()
		if e != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ne, ok := e.(net.Error); ok && ne.Timeout() {
				return nil
			}
			return errors.New("Unix accept failed")
		}
		if !sameUID(c) {
			c.Close()
			continue
		}
		s.handle(ctx, c)
		c.Close()
	}
}

func (s *source) handle(parent context.Context, c *net.UnixConn) {
	stopClose := context.AfterFunc(parent, func() { c.Close() })
	defer stopClose()
	deadline := time.Now().Add(s.o.timeout)
	_ = c.SetDeadline(deadline)
	r := bufio.NewReaderSize(c, maxLine)
	name, outcome := "", "rejected error=incomplete request"
	var payloadSize int64
	defer func() {
		if name == "" {
			s.logf("%s", outcome)
		} else {
			s.logf("%s name=%s bytes=%d", outcome, name, payloadSize)
		}
	}()
	fail := func(err error) {
		outcome = "rejected error=" + safeLocalMessage(err.Error())
		_ = writeJSON(c, wireHeader{Version: protocolVersion, Error: err.Error()})
	}
	var q wireRequest
	if err := readJSON(r, &q); err != nil {
		fail(err)
		return
	}
	if q.Version != protocolVersion {
		fail(errors.New("protocol version mismatch"))
		return
	}
	if q.Identity != s.o.identity {
		fail(errors.New("source configuration mismatch"))
		return
	}
	req, err := parseRequest(q.Name)
	if err != nil {
		fail(err)
		return
	}
	name = q.Name // parseRequest has validated every byte.
	clientEnd := time.Unix(0, q.Deadline)
	if clientEnd.Before(deadline) {
		deadline = clientEnd
	}
	if !deadline.After(time.Now()) {
		fail(errors.New("request deadline exceeded"))
		return
	}
	_ = c.SetDeadline(deadline)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	f, err := os.CreateTemp(s.o.dir, "stage-*")
	if err != nil {
		fail(errors.New("cannot stage payload"))
		return
	}
	defer os.Remove(f.Name())
	defer f.Close()
	candidate := s.progress
	if req.history {
		var data []byte
		data, err = s.history(ctx, req.tli)
		if err == nil {
			if _, err = f.Write(data); err != nil {
				err = errors.New("write staging file failed")
			}
		}
	} else {
		candidate, err = s.fetch(ctx, req, f)
	}
	if err != nil {
		if !req.history && s.streaming {
			s.lost()
		}
		fail(err)
		return
	}
	// Always end COPY, including invalid/missing client ACKs. Do not advertise
	// anything until publication was acknowledged by this request's client.
	if !req.history {
		defer func() {
			if s.conn != nil && s.streaming {
				if e := s.finish(ctx); e != nil {
					s.lost()
				}
			}
		}()
	}
	stat, err := f.Stat()
	if err != nil {
		fail(errors.New("stat staging file failed"))
		return
	}
	if _, err = f.Seek(0, 0); err != nil {
		fail(errors.New("seek staging file failed"))
		return
	}
	digest := sha256.New()
	if _, err = io.Copy(digest, f); err != nil {
		fail(errors.New("checksum staging file failed"))
		return
	}
	if _, err = f.Seek(0, 0); err != nil {
		fail(errors.New("seek staging file failed"))
		return
	}
	payloadSize = stat.Size()
	outcome = "publication unconfirmed"
	id := token()
	h := wireHeader{Version: protocolVersion, OK: true, Length: stat.Size(), SHA256: hex.EncodeToString(digest.Sum(nil)), ID: id}
	if writeJSON(c, h) != nil {
		return
	}
	if _, err = io.Copy(c, f); err != nil {
		return
	}
	if c.CloseWrite() != nil {
		return
	}
	var ack wireACK
	if readJSON(r, &ack) != nil || ack.Version != protocolVersion || ack.ID != id || !ack.Published {
		return
	}
	outcome = "fetched"
	if !req.history && !s.o.noSlot {
		if candidate.floor > s.progress.floor {
			if err = s.feedback(candidate.floor); err != nil {
				s.lost()
				return
			}
			s.logf("retention floor advanced to %s", candidate.floor)
		}
		s.progress = candidate
	}
}
