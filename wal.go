package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var fileNamePattern = regexp.MustCompile(`^(?:[0-9A-Fa-f]{24}|[0-9A-Fa-f]{8}\.history)$`)
var lsnPattern = regexp.MustCompile(`^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$`)

type request struct {
	tli      uint32
	log, seg uint64
	history  bool
}

type span struct {
	tli        uint32
	start, end pglogrepl.LSN
}

func parseRequest(name string) (request, error) {
	var r request
	if !fileNamePattern.MatchString(name) {
		return r, errors.New("unsupported WAL filename")
	}
	tli, _ := strconv.ParseUint(name[:8], 16, 32)
	r.tli, r.history = uint32(tli), strings.HasSuffix(name, ".history")
	// pglogrepl's timeline API uses signed int32.
	if tli == 0 || tli > math.MaxInt32 || (r.history && tli == 1) {
		return r, errors.New("unsupported timeline")
	}
	if !r.history {
		r.log, _ = strconv.ParseUint(name[8:16], 16, 32)
		r.seg, _ = strconv.ParseUint(name[16:24], 16, 32)
	}
	return r, nil
}

func getHistory(ctx context.Context, conn *pgconn.PgConn, tli uint32) ([]byte, error) {
	h, err := pglogrepl.TimelineHistory(ctx, conn, int32(tli))
	if err != nil {
		return nil, safeError(ctx, "TIMELINE_HISTORY", err)
	}
	if h.FileName != fmt.Sprintf("%08X.history", tli) || len(h.Content) == 0 {
		return nil, errors.New("invalid timeline history response")
	}
	return h.Content, nil
}

func parseHistory(data []byte, current uint32, flush pglogrepl.LSN) ([]span, error) {
	var spans []span
	var start pglogrepl.LSN
	var previous uint32
	s := bufio.NewScanner(strings.NewReader(string(data)))
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || !lsnPattern.MatchString(fields[1]) {
			return nil, errors.New("invalid timeline history")
		}
		tli, e := strconv.ParseUint(fields[0], 10, 32)
		end, e2 := pglogrepl.ParseLSN(fields[1])
		if e != nil || e2 != nil || tli == 0 || uint32(tli) <= previous || tli >= uint64(current) || end < start || end > flush {
			return nil, errors.New("invalid timeline ancestry")
		}
		spans = append(spans, span{uint32(tli), start, end})
		start, previous = end, uint32(tli)
	}
	if s.Err() != nil || (current > 1 && len(spans) == 0) {
		return nil, errors.New("incomplete timeline history")
	}
	return append(spans, span{current, start, flush}), nil
}

func segmentSize(value string) (uint64, error) {
	var digits int
	for digits < len(value) && value[digits] >= '0' && value[digits] <= '9' {
		digits++
	}
	n, err := strconv.ParseUint(value[:digits], 10, 64)
	mult, ok := map[string]uint64{"": 1, "B": 1, "kB": 1024, "MB": 1 << 20, "GB": 1 << 30}[strings.TrimSpace(value[digits:])]
	if err != nil || !ok || n > (1<<30)/mult {
		return 0, errors.New("invalid WAL segment size")
	}
	n *= mult
	if n < 1<<20 || n > 1<<30 || n&(n-1) != 0 {
		return 0, errors.New("invalid WAL segment size")
	}
	return n, nil
}

func requiredRange(r request, size uint64, branch span) (pglogrepl.LSN, pglogrepl.LSN, error) {
	if r.seg >= (1<<32)/size {
		return 0, 0, errors.New("invalid WAL segment number")
	}
	start := r.log<<32 | r.seg*size
	if start > math.MaxUint64-size {
		return 0, 0, errors.New("WAL position overflow")
	}
	end := start + size
	// The fork's segment has a shared prefix; whole earlier segments don't belong to this TLI.
	if end <= uint64(branch.start) || start >= uint64(branch.end) {
		return 0, 0, errors.New("WAL segment is outside the requested timeline or captured flush position")
	}
	if end > uint64(branch.end) {
		end = uint64(branch.end)
	}
	return pglogrepl.LSN(start), pglogrepl.LSN(end), nil
}

func publish(ctx context.Context, dest string, write func(*os.File) error) error {
	f, err := os.CreateTemp(filepath.Dir(dest), ".wal-fetch-*")
	if err != nil {
		return errors.New("create destination temporary file failed")
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return errors.New("sync destination failed")
	}
	if err := f.Close(); err != nil {
		return errors.New("close destination failed")
	}
	if err := ctx.Err(); err != nil {
		return errors.New("deadline exceeded; destination unchanged")
	}
	if err := os.Rename(f.Name(), dest); err != nil {
		return errors.New("publish destination failed")
	}
	return nil
}

func safeError(ctx context.Context, stage string, err error) error {
	if ctx.Err() != nil || pgconn.Timeout(err) {
		return fmt.Errorf("%s: deadline exceeded", stage)
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && regexp.MustCompile(`^[0-9A-Z]{5}$`).MatchString(pe.Code) {
		return fmt.Errorf("%s failed (SQLSTATE %s)", stage, pe.Code)
	}
	return fmt.Errorf("%s failed", stage)
}
