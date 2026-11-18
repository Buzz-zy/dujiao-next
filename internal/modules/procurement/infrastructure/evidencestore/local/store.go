package local

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dujiao-next/internal/modules/procurement/contract"
)

const (
	logFileName   = "trade-evidence.v1.log"
	frameMagic    = "EVJ1"
	maxRecordSize = 1 << 20
)

var (
	ErrEventConflict = errors.New("evidence event ID payload conflict")
	ErrCorruptLog    = errors.New("evidence log is corrupt and was quarantined")
	ErrInvalidEvent  = errors.New("invalid evidence event")
)

// Store is a single-process append-only spool. S3 remains a separate phase.
type Store struct {
	mu         sync.Mutex
	dir        string
	file       *os.File
	lock       *os.File
	events     map[string]contract.EvidenceEvent
	pending    map[string]struct{}
	writeErr   error
	writeFrame func(*os.File, []byte) (int, error)
	syncFile   func(*os.File) error
}

func New(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("evidence directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create evidence directory: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open evidence lock: %w", err)
	}
	if err := acquireFileLock(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("evidence directory is already locked: %w", err)
	}
	s := &Store{
		dir: dir, lock: lock, events: make(map[string]contract.EvidenceEvent), pending: make(map[string]struct{}),
		writeFrame: func(f *os.File, p []byte) (int, error) { return f.Write(p) },
		syncFile:   func(f *os.File) error { return f.Sync() },
	}
	path := filepath.Join(dir, logFileName)
	if err := s.scan(path); err != nil {
		s.release()
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		s.release()
		return nil, fmt.Errorf("open evidence log: %w", err)
	}
	s.file = f
	if err := syncDirectory(dir); err != nil {
		s.release()
		_ = f.Close()
		return nil, fmt.Errorf("sync evidence directory: %w", err)
	}
	return s, nil
}

func (s *Store) AppendIntent(event contract.EvidenceEvent) error {
	return s.append(event, "trade_intent")
}
func (s *Store) AppendResponse(event contract.EvidenceEvent) error {
	return s.append(event, "trade_response")
}

func (s *Store) append(event contract.EvidenceEvent, kind string) error {
	if event.EventID == "" || event.ProcurementOrderID == 0 || event.TraceID == "" || event.Supplier == "" || event.EventType != kind || event.SchemaVersion != 1 {
		return ErrInvalidEvent
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(encoded) > maxRecordSize {
		return fmt.Errorf("evidence record exceeds %d bytes", maxRecordSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return fmt.Errorf("evidence log is unavailable after an earlier write failure: %w", s.writeErr)
	}
	if old, ok := s.events[event.EventID]; ok {
		if bytes.Equal(mustJSON(old), encoded) {
			return nil
		}
		return ErrEventConflict
	}
	frame := make([]byte, 12+len(encoded))
	copy(frame[:4], frameMagic)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(encoded)))
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(encoded))
	copy(frame[12:], encoded)
	if n, err := s.writeFrame(s.file, frame); err != nil || n != len(frame) {
		if err == nil {
			err = io.ErrShortWrite
		}
		s.writeErr = err
		return fmt.Errorf("append evidence record: %w", err)
	}
	if err := s.syncFile(s.file); err != nil {
		s.writeErr = err
		return fmt.Errorf("sync evidence record: %w", err)
	}
	s.events[event.EventID] = event
	if kind == "trade_response" {
		s.pending[event.EventID] = struct{}{}
	}
	return nil
}

func mustJSON(event contract.EvidenceEvent) []byte { data, _ := json.Marshal(event); return data }

func (s *Store) GetByEventID(id string) (*contract.EvidenceEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event, ok := s.events[id]
	if !ok {
		return nil, nil
	}
	copy := event
	return &copy, nil
}

func (s *Store) GetByTraceID(traceID string) ([]contract.EvidenceEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []contract.EvidenceEvent
	for _, event := range s.events {
		if event.TraceID == traceID {
			out = append(out, event)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out, nil
}

func (s *Store) ListPending() ([]contract.EvidenceEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.pending))
	for id := range s.pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]contract.EvidenceEvent, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.events[id])
	}
	return out, nil
}

// MarkImported removes a response from the local replay set after remote persistence.
// It appends a tombstone; the original evidence frame is never changed or deleted.
func (s *Store) MarkImported(eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return fmt.Errorf("evidence log is unavailable after an earlier write failure: %w", s.writeErr)
	}
	if _, ok := s.pending[eventID]; !ok {
		return nil
	}
	marker := struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
	}{eventID, "imported"}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	frame := make([]byte, 12+len(data))
	copy(frame[:4], frameMagic)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(data)))
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(data))
	copy(frame[12:], data)
	if n, err := s.writeFrame(s.file, frame); err != nil || n != len(frame) {
		if err == nil {
			err = io.ErrShortWrite
		}
		s.writeErr = err
		return err
	}
	if err := s.syncFile(s.file); err != nil {
		s.writeErr = err
		return err
	}
	delete(s.pending, eventID)
	return nil
}

func (s *Store) scan(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	validRecords := make([][]byte, 0)
	var corruption error
	for {
		header := make([]byte, 12)
		_, err := io.ReadFull(r, header)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			corruption = err
			break
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if string(header[:4]) != frameMagic || size == 0 || size > maxRecordSize {
			corruption = errors.New("invalid frame header")
			break
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(r, data); err != nil {
			corruption = err
			break
		}
		if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(header[8:12]) {
			corruption = errors.New("checksum mismatch")
			break
		}
		var envelope struct {
			EventType string `json:"event_type"`
			EventID   string `json:"event_id"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			corruption = err
			break
		}
		if envelope.EventType == "imported" {
			if envelope.EventID == "" {
				corruption = errors.New("invalid import marker")
				break
			}
			delete(s.pending, envelope.EventID)
			validRecords = append(validRecords, append(append([]byte(nil), header...), data...))
			continue
		}
		var event contract.EvidenceEvent
		if err := json.Unmarshal(data, &event); err != nil {
			corruption = err
			break
		}
		if event.EventID == "" || event.EventType != "trade_intent" && event.EventType != "trade_response" {
			corruption = errors.New("invalid event payload")
			break
		}
		if old, ok := s.events[event.EventID]; ok && !bytes.Equal(mustJSON(old), data) {
			corruption = ErrEventConflict
			break
		}
		s.events[event.EventID] = event
		validRecords = append(validRecords, append(append([]byte(nil), header...), data...))
		if event.EventType == "trade_response" {
			s.pending[event.EventID] = struct{}{}
		}
	}
	if corruption != nil {
		return s.quarantineAndRestorePrefix(path, f, corruption, validRecords)
	}
	return nil
}

func (s *Store) quarantineAndRestorePrefix(path string, source *os.File, cause error, records [][]byte) error {
	name := path + ".corrupt." + time.Now().UTC().Format("20060102T150405.000000000")
	_ = source.Close()
	if err := os.Rename(path, name); err != nil {
		return fmt.Errorf("%w: %v; quarantine failed: %v", ErrCorruptLog, cause, err)
	}
	recovery, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("%w: %v; original preserved at %s; valid-prefix restore failed: %v", ErrCorruptLog, cause, name, err)
	}
	for _, record := range records {
		if _, err := recovery.Write(record); err != nil {
			_ = recovery.Close()
			return fmt.Errorf("%w: %v; original preserved at %s; prefix restore failed: %v", ErrCorruptLog, cause, name, err)
		}
	}
	if err := recovery.Sync(); err != nil {
		_ = recovery.Close()
		return fmt.Errorf("%w: %v; original preserved at %s; prefix sync failed: %v", ErrCorruptLog, cause, name, err)
	}
	closeErr := recovery.Close()
	if closeErr != nil {
		return fmt.Errorf("%w: %v; original preserved at %s; prefix close failed: %v", ErrCorruptLog, cause, name, closeErr)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("%w: %v; original preserved at %s; directory sync failed: %v", ErrCorruptLog, cause, name, err)
	}
	return fmt.Errorf("%w: %v (original preserved at %s; %d valid records restored)", ErrCorruptLog, cause, name, len(records))
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.file != nil {
		err = s.file.Close()
		s.file = nil
	}
	s.release()
	return err
}
func (s *Store) release() {
	if s.lock != nil {
		_ = releaseFileLock(s.lock)
		_ = s.lock.Close()
		s.lock = nil
	}
}

var _ contract.EvidenceStore = (*Store)(nil)
