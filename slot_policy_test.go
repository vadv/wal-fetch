package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestSlotFailuresSurviveLostSession(t *testing.T) {
	var records bytes.Buffer
	s := source{o: options{slotFailureLimit: 2}, slot: "old_slot", logger: log.New(&records, "", 0)}
	s.failedFetch()
	s.lost() // A server ERROR must not restart the failure series.
	s.failedFetch()
	if !s.slotSuspended || s.failedWAL != 2 || s.usesSlot() {
		t.Fatal("lost connection reset the failure series")
	}
	s.close()
	for range 10 {
		s.failedFetch()
	}
	if s.failedWAL != 2 || !s.slotSuspended || strings.Count(records.String(), "slots suspended") != 1 {
		t.Fatal("suppression did not survive reconnects or log only once")
	}
	for _, o := range []options{{slotFailureLimit: 0}, {slotFailureLimit: 2, noSlot: true}} {
		s := source{o: o}
		for range 3 {
			s.failedFetch()
		}
		if s.slotSuspended || s.failedWAL != 0 {
			t.Fatal("disabled slot policy changed state")
		}
	}
	if err := run([]string{"-slot-failure-limit", "-1", "000000010000000000000001", "unused"}); err == nil || !strings.Contains(err.Error(), "nonnegative") {
		t.Fatalf("negative failure limit accepted: %v", err)
	}
}

// Retention may resume only after both publication and the real COPY drain.
func TestSlotPolicyCopyDone(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		suspended, published, fail bool
	}{
		{"unconfirmed", true, false, false},
		{"published", true, true, false},
		{"finish-error", true, true, true},
		{"healthy-owner", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			done := make(chan error, 1)
			go func() {
				c, err := ln.Accept()
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				b := pgproto3.NewBackend(c, c)
				if _, err := b.ReceiveStartupMessage(); err != nil {
					done <- err
					return
				}
				b.Send(&pgproto3.AuthenticationOk{})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if err := b.Flush(); err != nil {
					done <- err
					return
				}
				msg, err := b.Receive()
				if _, ok := msg.(*pgproto3.CopyDone); err != nil || !ok {
					done <- fmt.Errorf("expected CopyDone, got %T: %w", msg, err)
					return
				}
				if tc.fail {
					b.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "58P01", Message: "fixture error"})
				} else {
					b.Send(&pgproto3.CopyDone{})
					b.Send(&pgproto3.CommandComplete{CommandTag: []byte("START_REPLICATION")})
					b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				}
				if err := b.Flush(); err != nil {
					done <- err
					return
				}
				msg, err = b.Receive()
				if _, ok := msg.(*pgproto3.Terminate); err != nil || !ok {
					done <- fmt.Errorf("expected Terminate, got %T: %w", msg, err)
					return
				}
				done <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := pgconn.Connect(ctx, "host=127.0.0.1 port="+fmt.Sprint(ln.Addr().(*net.TCPAddr).Port)+" user=test sslmode=disable replication=yes")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(context.Background())
			_ = c.Conn().SetDeadline(time.Now().Add(5 * time.Second))
			s := source{conn: c, streaming: true, failedWAL: 5, slotSuspended: tc.suspended, o: options{slotFailureLimit: 5}}
			err = s.finish(ctx, tc.published)
			if (err != nil) != tc.fail {
				t.Fatalf("finish error: %v", err)
			}
			if tc.fail {
				s.lost() // Match handle's cleanup after a failed COPY drain.
			}
			confirmed := tc.published && !tc.fail
			if confirmed && (s.failedWAL != 0 || s.slotSuspended) {
				t.Fatal("confirmed publication did not clear the failure series")
			}
			if !confirmed && (s.failedWAL != 5 || !s.slotSuspended) {
				t.Fatal("unconfirmed publication or COPY error re-enabled slots")
			}
			if confirmed && tc.suspended && s.conn != nil {
				t.Fatal("probe connection survived rearming")
			}
			if confirmed && !tc.suspended && s.conn != c {
				t.Fatal("healthy owner was unnecessarily closed")
			}
			s.close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
