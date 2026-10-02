package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"
	"time"
)

func TestCoverageAndFeedback(t *testing.T) {
	p := coverage{floor: 0x180, end: 0x180}
	for _, tc := range []struct {
		tli                        uint32
		start, end, floor, covered pglogrepl.LSN
	}{
		{1, 0x100, 0x200, 0x180, 0x200}, // old prefix may exist; never move initial guard back
		{1, 0x400, 0x500, 0x180, 0x200}, // gap is not acknowledged
		{1, 0x200, 0x300, 0x200, 0x300},
		{1, 0x300, 0x380, 0x300, 0x380}, // active prefix, zero tail not covered
		{1, 0x300, 0x380, 0x300, 0x380}, // repeat
		{2, 0x300, 0x400, 0x300, 0x400}, // valid descendant's shared fork segment
		{1, 0x300, 0x500, 0x300, 0x400}, // old branch must not extend newer branch coverage
		{2, 0x400, 0x500, 0x400, 0x500},
	} {
		p = p.published(tc.tli, tc.start, tc.end)
		if p.floor != tc.floor || p.end != tc.covered {
			t.Fatalf("%+v: %+v", tc, p)
		}
	}
	for _, floor := range []pglogrepl.LSN{0, 0x500} {
		b := feedbackData(floor)
		if len(b) != 34 || b[0] != 'r' || binary.BigEndian.Uint64(b[1:9]) != uint64(floor) || binary.BigEndian.Uint64(b[9:17]) != uint64(floor) || binary.BigEndian.Uint64(b[17:25]) != 0 {
			t.Fatal("unsafe feedback")
		}
	}
}
func TestBoundedProtocol(t *testing.T) {
	var q wireRequest
	if err := readJSON(bufio.NewReader(iotest.ErrReader(syscall.ECONNRESET)), &q); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("lost transport error needed for idle reconnect: %v", err)
	}
	for _, text := range []string{"", `{}`, "{\"version\":1}\nextra", strings.Repeat("x", maxLine+1) + "\n", "{broken}\n", "{\"unknown\":1}\n", "{} {}\n"} {
		var q wireRequest
		err := readJSON(bufio.NewReaderSize(strings.NewReader(text), maxLine), &q)
		// Framing consumes one line; trailing payload belongs to the next protocol stage.
		if strings.Contains(text, "extra") {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		if text == "{}" {
			if err == nil {
				t.Fatal("unterminated header accepted")
			}
			continue
		}
		if err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
}
func TestServerRejectsBeforeConnecting(t *testing.T) {
	for _, q := range []wireRequest{{Version: 99}, {Version: 1, Identity: "wrong"}, {Version: 1, Identity: "right", Name: "../secret-password\ninjected"}} {
		dir := t.TempDir()
		ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, "server.sock"), Net: "unix"})
		if e != nil {
			t.Fatal(e)
		}
		var records bytes.Buffer
		done := make(chan struct{})
		go func() {
			defer close(done)
			c, e := ln.AcceptUnix()
			if e != nil {
				return
			}
			defer c.Close()
			s := source{o: options{dir: dir, identity: "right", timeout: time.Second}, logger: log.New(&records, "", 0)}
			s.handle(context.Background(), c)
		}()
		c, e := net.DialUnix("unix", nil, ln.Addr().(*net.UnixAddr))
		if e != nil {
			t.Fatal(e)
		}
		_ = writeJSON(c, q)
		var h wireHeader
		if e = readJSON(bufio.NewReader(c), &h); e != nil || h.OK || h.Error == "" {
			t.Fatalf("%+v %v", h, e)
		}
		c.Close()
		ln.Close()
		<-done
		if !strings.Contains(records.String(), "rejected error=") || strings.Contains(records.String(), "secret-password") || strings.Contains(records.String(), "injected") {
			t.Fatalf("unsafe rejection log: %s", records.String())
		}
	}
}
func TestClientPublication(t *testing.T) {
	for _, mode := range []string{"complete", "ack-lost", "version", "length", "truncated", "extra", "digest", "id", "retry-close", "source-changed", "timeout", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, "server.sock"), Net: "unix"})
			if e != nil {
				t.Fatal(e)
			}
			defer ln.Close()
			done := make(chan error, 1)
			go func() {
				c, e := ln.AcceptUnix()
				if e != nil {
					done <- e
					return
				}
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				var q wireRequest
				if e = readJSON(bufio.NewReader(c), &q); e != nil {
					done <- e
					return
				}
				if mode == "retry-close" {
					c.Close()
					c, e = ln.AcceptUnix()
					if e != nil {
						done <- e
						return
					}
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(5 * time.Second))
					if e = readJSON(bufio.NewReader(c), &q); e != nil {
						done <- e
						return
					}
				}
				if mode == "source-changed" {
					if e = writeJSON(c, wireHeader{Version: 1, Error: errSourceChanged.Error()}); e != nil {
						done <- e
						return
					}
					c.Close()
					c, e = ln.AcceptUnix()
					if e != nil {
						done <- e
						return
					}
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(5 * time.Second))
					if e = readJSON(bufio.NewReader(c), &q); e != nil {
						done <- e
						return
					}
				}
				payload := []byte("exact history\n")
				sum := sha256.Sum256(payload)
				h := wireHeader{Version: 1, OK: true, Length: int64(len(payload)), ID: strings.Repeat("a", 32), SHA256: hex.EncodeToString(sum[:])}
				switch mode {
				case "version":
					h.Version = 2
				case "length":
					h.Length = maxHistory + 1
				case "id":
					h.ID = strings.Repeat("z", 32)
				case "digest":
					h.SHA256 = strings.Repeat("0", 64)
				}
				if mode == "malformed" {
					_, _ = io.WriteString(c, "{} {}\n")
					done <- nil
					return
				}
				_ = writeJSON(c, h)
				if mode == "timeout" {
					time.Sleep(150 * time.Millisecond)
				}
				if mode == "truncated" {
					payload = payload[:2]
				}
				if mode == "extra" {
					payload = append(payload, 'x')
				}
				_, _ = c.Write(payload)
				_ = c.CloseWrite()
				if mode == "complete" || mode == "retry-close" || mode == "source-changed" {
					var ack wireACK
					e = readJSON(bufio.NewReader(c), &ack)
					if e == nil && (!ack.Published || ack.ID != h.ID) {
						e = fmt.Errorf("bad ACK")
					}
					done <- e
					return
				}
				done <- nil
			}()
			dest := filepath.Join(dir, "destination")
			_ = os.WriteFile(dest, []byte("ORIGINAL"), 0600)
			timeout := 5 * time.Second // Successful publication includes a real fsync.
			if mode == "timeout" {
				timeout = 100 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			err := client(ctx, options{dir: dir, identity: "config", size: 16 << 20}, "00000002.history", dest)
			ok := mode == "complete" || mode == "ack-lost" || mode == "retry-close" || mode == "source-changed"
			if (err == nil) != ok {
				t.Fatalf("%s: %v", mode, err)
			}
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			b, _ := os.ReadFile(dest)
			want := "ORIGINAL"
			if ok {
				want = "exact history\n"
			}
			if string(b) != want {
				t.Fatalf("destination %q", b)
			}
			leftovers, _ := filepath.Glob(filepath.Join(dir, ".wal-fetch-*"))
			if len(leftovers) != 0 {
				t.Fatal("temporary file leaked")
			}
		})
	}
}
func TestSizeAndLineage(t *testing.T) {
	for _, tc := range []struct {
		value string
		size  uint64
	}{{"16MB", 16 << 20}, {"1MB", 1 << 20}, {"64MB", 64 << 20}, {"24MB", 0}, {"0", 0}, {"2GB", 0}} {
		n, e := segmentSize(tc.value)
		if tc.size == 0 && e == nil || tc.size != 0 && (e != nil || n != tc.size) {
			t.Fatalf("%s: %d %v", tc.value, n, e)
		}
	}
	spans, e := parseHistory([]byte("1\t0/1800000\tPITR\n"), 2, 0x2800000)
	if e != nil {
		t.Fatal(e)
	}
	r, _ := parseRequest("000000020000000000000001")
	a, z, e := requiredRange(r, 16<<20, spans[1])
	if e != nil || a != 0x1000000 || z != 0x2000000 {
		t.Fatalf("shared prefix: %s %s %v", a, z, e)
	}
	if _, e = parseHistory([]byte("1 0/5000000 future"), 2, 0x2800000); e == nil {
		t.Fatal("invalid fork accepted")
	}
}

// Exercise physical CopyData itself: server WAL end is not evidence of receipt.
func TestPhysicalTransfer(t *testing.T) {
	for _, mode := range []string{"complete", "gap", "short", "timeout", "no-slot/complete", "no-slot/gap", "no-slot/short", "no-slot/timeout", "suspended/complete", "suspended/timeout"} {
		t.Run(mode, func(t *testing.T) {
			noSlot := strings.HasPrefix(mode, "no-slot/")
			suspended := strings.HasPrefix(mode, "suspended/")
			mode = strings.TrimPrefix(mode, "no-slot/")
			mode = strings.TrimPrefix(mode, "suspended/")
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			done := make(chan error, 1)
			go func() {
				c, e := ln.Accept()
				if e != nil {
					done <- e
					return
				}
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(time.Second))
				b := pgproto3.NewBackend(c, c)
				if _, e = b.ReceiveStartupMessage(); e != nil {
					done <- e
					return
				}
				b.Send(&pgproto3.AuthenticationOk{})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if e = b.Flush(); e != nil {
					done <- e
					return
				}
				m, e := b.Receive()
				if e != nil {
					done <- e
					return
				}
				q, ok := m.(*pgproto3.Query)
				want := "START_REPLICATION SLOT test_slot PHYSICAL 0/1000000 TIMELINE 1"
				if noSlot || suspended {
					want = "START_REPLICATION PHYSICAL 0/1000000 TIMELINE 1"
				}
				if !ok || strings.TrimSpace(q.String) != want {
					done <- fmt.Errorf("bad start command")
					return
				}
				b.Send(&pgproto3.CopyBothResponse{})
				if mode == "timeout" {
					data := make([]byte, 18)
					data[0] = 'k'
					data[17] = 1
					b.Send(&pgproto3.CopyData{Data: data})
				} else {
					data := make([]byte, 25)
					data[0] = 'w'
					start := uint64(0x1000000)
					if mode == "gap" {
						start++
					}
					binary.BigEndian.PutUint64(data[1:], start)
					binary.BigEndian.PutUint64(data[9:], 0x9000000)
					payload := []byte("ABCD")
					if mode == "short" {
						payload = payload[:2]
					}
					b.Send(&pgproto3.CopyData{Data: append(data, payload...)})
					if mode == "short" {
						b.Send(&pgproto3.CopyDone{})
					}
				}
				if e = b.Flush(); e != nil {
					done <- e
					return
				}
				if mode == "timeout" {
					m, e = b.Receive()
					if e != nil {
						done <- e
						return
					}
					feedback, ok := m.(*pgproto3.CopyData)
					if !ok || len(feedback.Data) != 34 || !bytes.Equal(feedback.Data[1:25], make([]byte, 24)) {
						done <- fmt.Errorf("false feedback")
						return
					}
					_, _ = b.Receive()
				}
				done <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			c, err := pgconn.Connect(ctx, "host=127.0.0.1 port="+fmt.Sprint(ln.Addr().(*net.TCPAddr).Port)+" user=test sslmode=disable replication=yes")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(context.Background())
			deadline, _ := ctx.Deadline()
			_ = c.Conn().SetDeadline(deadline)
			s := source{conn: c, slot: "test_slot", o: options{noSlot: noSlot}, slotSuspended: suspended}
			if noSlot || suspended {
				s.slot = ""
				s.acked = 123 // Slotless keepalive feedback must still be all zero.
			}
			var dst bytes.Buffer
			err = s.receive(ctx, &dst, 1, 0x1000000, 0x1000004)
			if (err == nil) != (mode == "complete") {
				t.Fatalf("%s: %v", mode, err)
			}
			if mode == "complete" && dst.String() != "ABCD" {
				t.Fatal("wrong bytes")
			}
			_ = c.Close(ctx)
			if e := <-done; e != nil {
				t.Fatal(e)
			}
		})
	}
}
