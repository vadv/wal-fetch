package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgio"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"io"
	"log"
	"net"
	"os"
	"time"
)

type coverage struct {
	floor, end pglogrepl.LSN
	tli        uint32
}

// codeSourceChanged is the wire code the client recognizes as "start a new server".
// The wording is part of the protocol contract.
const codeSourceChanged = "source system or timeline changed; start a new server"

// errSourceChanged is fatal: the server follows one fixed source identity.
var errSourceChanged = errors.New(codeSourceChanged)

// Only overlapping/adjacent published coverage advances retention; forget jumps.
func (p coverage) published(tli uint32, start, end pglogrepl.LSN) coverage {
	if start > p.end || end <= p.end || tli < p.tli {
		return p
	}
	p.end = end
	p.tli = tli
	if start > p.floor {
		p.floor = start
	}
	return p
}

type source struct {
	o                                options
	conn                             *pgconn.PgConn
	slot, systemID, network, address string
	timeline                         uint32
	progress                         coverage
	acked                            pglogrepl.LSN
	streaming                        bool
	failedWAL                        int
	slotSuspended                    bool
	changed                          bool // errSourceChanged stops the whole server
	logger                           *log.Logger
}

func (s *source) usesSlot() bool {
	return !s.o.noSlot && !s.slotSuspended
}

func (s *source) failedFetch() {
	if s.o.noSlot || s.o.slotFailureLimit == 0 || s.slotSuspended {
		return
	}
	s.failedWAL++
	if s.failedWAL >= s.o.slotFailureLimit {
		s.close()
		s.slotSuspended = true
		s.logf("temporary slots suspended after %d WAL fetch failures; probing without retention", s.failedWAL)
	}
}

func (s *source) close() {
	if s.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = s.conn.Close(ctx)
		cancel()
	}
	s.conn = nil
	s.slot = ""
	s.streaming = false
	s.acked = 0
	s.progress = coverage{}
}
func (s *source) lost() {
	if s.slot != "" {
		s.logf("retention lost: owning replication session failed; continuity is not guaranteed")
	}
	s.close()
}
func (s *source) connect(ctx context.Context) (*pgconn.PgConn, error) {
	cfg := s.o.cfg.Copy()
	if s.address != "" {
		cfg.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, s.network, s.address)
		}
	}
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, safeError(ctx, "connect source", err)
	}
	deadline, _ := ctx.Deadline()
	_ = conn.Conn().SetDeadline(deadline)
	id, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil || id.SystemID == "" || id.Timeline <= 0 {
		_ = conn.Close(ctx)
		if err != nil {
			return nil, safeError(ctx, "identify source", err)
		}
		return nil, errors.New("invalid source identity")
	}
	if s.systemID != "" && (s.systemID != id.SystemID || s.timeline != uint32(id.Timeline)) {
		_ = conn.Close(ctx)
		return nil, errSourceChanged
	}
	if s.systemID == "" {
		s.systemID = id.SystemID
		s.timeline = uint32(id.Timeline)
		s.network = conn.Conn().RemoteAddr().Network()
		s.address = conn.Conn().RemoteAddr().String()
	}
	return conn, nil
}
func (s *source) history(ctx context.Context, tli uint32) ([]byte, error) {
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close(ctx)
	h, err := getHistory(ctx, c, tli)
	if err == nil && len(h) > maxHistory {
		return nil, errors.New("history is too large")
	}
	return h, err
}
func (s *source) ensure(ctx context.Context) error {
	if s.conn != nil {
		deadline, _ := ctx.Deadline()
		return s.conn.Conn().SetDeadline(deadline)
	}
	var err error
	s.conn, err = s.connect(ctx)
	if err != nil {
		return err
	}
	if !s.usesSlot() {
		return nil
	}
	s.slot = "wal_fetch_" + token()
	_, err = pglogrepl.CreateReplicationSlot(ctx, s.conn, s.slot, "", pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.PhysicalReplication, SnapshotAction: "RESERVE_WAL"})
	if err != nil {
		s.lost()
		return safeError(ctx, "create temporary slot", err)
	}
	rows, err := s.conn.Exec(ctx, "READ_REPLICATION_SLOT "+s.slot).ReadAll()
	if err != nil {
		s.lost()
		return safeError(ctx, "read temporary slot", err)
	}
	if len(rows) != 1 || len(rows[0].Rows) != 1 || len(rows[0].Rows[0]) != 3 {
		s.lost()
		return errors.New("invalid slot state")
	}
	floor, err := pglogrepl.ParseLSN(string(rows[0].Rows[0][1]))
	if err != nil || floor == 0 {
		s.lost()
		return errors.New("slot has no reserved WAL")
	}
	s.progress = coverage{floor: floor, end: floor}
	s.logf("temporary slot created name=%s floor=%s; earlier WAL is not guaranteed", s.slot, floor)
	return nil
}
func (s *source) fetch(ctx context.Context, req request, f *os.File) (candidate coverage, err error) {
	if err = s.ensure(ctx); err != nil {
		return candidate, err
	}
	id, e := pglogrepl.IdentifySystem(ctx, s.conn)
	if e != nil {
		s.lost()
		return candidate, safeError(ctx, "IDENTIFY_SYSTEM", e)
	}
	if id.SystemID != s.systemID || uint32(id.Timeline) != s.timeline {
		s.lost()
		return candidate, errSourceChanged
	}
	var history []byte
	if id.Timeline > 1 {
		history, err = getHistory(ctx, s.conn, uint32(id.Timeline))
		if err != nil {
			s.lost()
			return candidate, err
		}
	}
	lineage, err := parseHistory(history, uint32(id.Timeline), id.XLogPos)
	if err != nil {
		return candidate, err
	}
	var branch *span
	for i := range lineage {
		if lineage[i].tli == req.tli {
			branch = &lineage[i]
			break
		}
	}
	if branch == nil {
		return candidate, errors.New("requested timeline is not in source ancestry")
	}
	start, end, err := requiredRange(req, s.o.size, *branch)
	if err != nil {
		return candidate, err
	}
	// Local filename/range rejections above leave the healthy owner and its slot
	// intact. A failure after starting COPY needs session cleanup instead.
	defer func() {
		if err != nil {
			s.lost()
		}
	}()
	if err = s.receive(ctx, f, req.tli, start, end); err != nil {
		return candidate, err
	}
	if err = f.Truncate(int64(s.o.size)); err != nil {
		return candidate, errors.New("cannot size staging file")
	}
	if !s.usesSlot() {
		return candidate, nil
	}
	return s.progress.published(req.tli, start, end), nil
}
func (s *source) receive(ctx context.Context, dst io.Writer, tli uint32, start, end pglogrepl.LSN) error {
	started := s.usesSlot()
	var err error
	if !s.usesSlot() {
		// The pinned pglogrepl always inserts SLOT, even with an empty name.
		// Use pgx's existing encoder for this slotless physical command.
		s.conn.Frontend().SendQuery(&pgproto3.Query{String: fmt.Sprintf("START_REPLICATION PHYSICAL %s TIMELINE %d", start, tli)})
		err = s.conn.Frontend().Flush()
	} else {
		err = pglogrepl.StartReplication(ctx, s.conn, s.slot, start, pglogrepl.StartReplicationOptions{Timeline: int32(tli), Mode: pglogrepl.PhysicalReplication})
	}
	if err != nil {
		return safeError(ctx, "START_REPLICATION", err)
	}
	s.streaming = true
	next := start
	for next < end {
		msg, err := s.conn.ReceiveMessage(ctx)
		if err != nil {
			return safeError(ctx, "receive WAL", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CopyBothResponse:
			if started {
				return errors.New("unexpected replication response")
			}
			started = true
		case *pgproto3.CopyData:
			if !started || len(m.Data) == 0 {
				return errors.New("empty replication message")
			}
			switch m.Data[0] {
			case pglogrepl.XLogDataByteID:
				x, e := pglogrepl.ParseXLogData(m.Data[1:])
				if e != nil {
					return errors.New("invalid WAL data")
				}
				if x.WALStart != next {
					return errors.New("non-contiguous WAL transfer")
				}
				data := x.WALData
				if uint64(len(data)) > uint64(end-next) {
					data = data[:int(end-next)]
				}
				n, e := dst.Write(data)
				if e != nil || n != len(data) {
					return errors.New("write staging file failed")
				}
				next += pglogrepl.LSN(n)
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				k, e := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if e != nil {
					return errors.New("invalid keepalive")
				}
				if k.ReplyRequested {
					if e = s.feedback(s.acked); e != nil {
						return safeError(ctx, "feedback", e)
					}
				}
			default:
				return errors.New("unknown replication message")
			}
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		case *pgproto3.ErrorResponse:
			return safeError(ctx, "replication server", pgconn.ErrorResponseToPgError(m))
		default:
			return errors.New("replication ended before required WAL arrived")
		}
	}
	return nil
}
func feedbackData(floor pglogrepl.LSN) []byte {
	b := []byte{pglogrepl.StandbyStatusUpdateByteID}
	b = pgio.AppendUint64(b, uint64(floor))
	b = pgio.AppendUint64(b, uint64(floor))
	b = pgio.AppendUint64(b, 0) // apply ALWAYS zero; pglogrepl substitutes write for zero.
	b = pgio.AppendInt64(b, time.Now().UnixMicro()-946684800000000)
	return append(b, 0)
}
func (s *source) feedback(floor pglogrepl.LSN) error {
	if !s.usesSlot() {
		floor = 0
	}
	s.conn.Frontend().Send(&pgproto3.CopyData{Data: feedbackData(floor)})
	if err := s.conn.Frontend().Flush(); err != nil {
		return err
	}
	if floor > s.acked {
		s.acked = floor
	}
	return nil
}
func (s *source) finish(ctx context.Context, published bool) error {
	// The pinned helper drains CopyData, CopyDone, rows and CommandComplete
	// through ReadyForQuery. Our net deadline bounds its synchronous reads.
	_, err := pglogrepl.SendStandbyCopyDone(ctx, s.conn)
	if err != nil {
		return safeError(ctx, "finish replication", err)
	}
	s.streaming = false
	if err := s.conn.Conn().SetDeadline(time.Time{}); err != nil {
		return err
	}
	if published && !s.o.noSlot {
		s.failedWAL = 0
		if s.slotSuspended {
			s.close()
			s.slotSuspended = false
			s.logf("temporary slots re-enabled after confirmed WAL publication; earlier WAL is not guaranteed")
		}
	}
	return nil
}
