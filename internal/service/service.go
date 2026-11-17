package service

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/config"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/event"
	"cadenceforge/internal/journal"
	"cadenceforge/internal/media"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
)

type Service struct {
	Store    *journal.Store[domain.State]
	Blobs    *blob.Store
	Probe    media.Prober
	Config   config.Config
	mutation sync.Mutex
	wake     chan struct{}
	activeMu sync.Mutex
	active   map[string]context.CancelFunc
}

func New(c config.Config) (*Service, error) {
	st, err := journal.Open(c.Root, domain.StateCodec{})
	if err != nil {
		return nil, err
	}
	blobs, err := blob.Open(c.Root)
	if err != nil {
		st.Close()
		return nil, err
	}
	s := &Service{Store: st, Blobs: blobs, Probe: media.Prober{Binary: c.FFprobe}, Config: c, wake: make(chan struct{}, 1), active: map[string]context.CancelFunc{}}
	if err = st.Update(func(state *domain.State) error {
		domain.RecoverInterrupted(state)
		return nil
	}); err != nil {
		st.Close()
		return nil, err
	}
	return s, nil
}
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("entropy unavailable: %w", err))
	}
	return hex.EncodeToString(b[:])
}
func hash(value any) string {
	b, _ := json.Marshal(value)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func (s *Service) Notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Events is the read-side catch-up boundary. It returns a consistent snapshot
// slice of the durable log; the event package owns ordering and paging, and the
// transport layer owns the 200-record page limit.
func (s *Service) Events(cursor uint64, limit int) ([]event.Event, error) {
	st, err := s.Store.Snapshot()
	if err != nil {
		return nil, err
	}
	return st.Log.After(cursor, limit), nil
}
