package journal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Codec is the only thing the journal knows about a document. It turns a
// durable byte image into a typed value and back. The journal never inspects
// the meaning of those bytes: it does not know about sources, sheets, renders
// or business events.
type Codec[T any] interface {
	Empty() T
	Decode([]byte) (T, error)
	Encode(T) ([]byte, error)
	Validate(T) error
}

// Store is a single-instance, single-document durable store. Each successful
// Update replaces the state document once, with the whole typed value
// committed atomically by temporary file, fsync, rename and directory sync.
type Store[T any] struct {
	mu       sync.Mutex
	root     string
	codec    Codec[T]
	raw      []byte
	lock     *os.File
	poisoned error
}

func Open[T any](root string, codec Codec[T]) (*Store[T], error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(root, "process.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data root already in use: %w", err)
	}
	s := &Store[T]{root: root, codec: codec, lock: lock}
	state := codec.Empty()
	data, err := os.ReadFile(filepath.Join(root, "state.json"))
	if err == nil {
		state, err = codec.Decode(data)
	} else if os.IsNotExist(err) {
		err = nil
	}
	if err == nil {
		err = codec.Validate(state)
	}
	if err == nil {
		s.raw, err = codec.Encode(state)
	}
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("load state: %w", err)
	}
	return s, nil
}

func (s *Store[T]) Snapshot() (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.codec.Decode(s.raw)
}

// Update applies fn to a private copy and commits the resulting document once.
// If fn makes no observable change, no durable write occurs. A state mutation
// and anything embedded in the document (including business events) therefore
// share exactly one commit.
func (s *Store[T]) Update(fn func(*T) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned != nil {
		return s.poisoned
	}
	next, err := s.codec.Decode(s.raw)
	if err != nil {
		return err
	}
	if err = fn(&next); err != nil {
		return err
	}
	b, err := s.codec.Encode(next)
	if err != nil {
		return err
	}
	if bytes.Equal(b, s.raw) {
		return nil
	}
	if err = s.codec.Validate(next); err != nil {
		return fmt.Errorf("state invariant: %w", err)
	}
	f, err := os.CreateTemp(s.root, ".state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.root, "state.json")); err != nil {
		return err
	}
	s.raw = b
	d, err := os.Open(s.root)
	if err != nil {
		s.poisoned = err
		return err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		s.poisoned = fmt.Errorf("state committed but durability uncertain; restart required: %w", err)
		return s.poisoned
	}
	return nil
}

func (s *Store[T]) Close() error {
	if s.lock != nil {
		return s.lock.Close()
	}
	return nil
}
