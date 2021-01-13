// Edge Network
// (c) 2021 Edge Network technologies Ltd.
package atomicstore

import (
	"context"
	"sync"

	"github.com/edge/utils/pkg/atomiccounter"
)

// Store stores mapped data.
type Store struct {
	cond  *sync.Cond
	count *atomiccounter.Counter
	sync.Map
}

// lockable returns true when a sync.Cond is present.
func (s *Store) lockable() bool {
	return s.cond != nil
}

// Len returns the size of the store.
func (s *Store) Len() uint64 {
	return s.count.Get()
}

// Insert creates a new entry of overwrites the existing.
func (s *Store) Insert(key string, val interface{}) (interface{}, bool) {
	if s.lockable() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
	}

	// If the document didn't already exist, store it,
	// increment counter and return the stored item.
	if resp, loaded := s.LoadOrStore(key, val); !loaded {
		s.count.Inc()
		return resp, loaded
	}

	// This entry already exists. Overwrite it.
	s.Store(key, val)
	return val, true
}

// Upsert creates a new entry if the key doesn't exist.
func (s *Store) Upsert(key string, val interface{}) (interface{}, bool) {
	if s.lockable() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
	}

	resp, loaded := s.LoadOrStore(key, val)
	// Increment the global counter if the value was not loaded.
	if !loaded {
		s.count.Inc()
	}

	return resp, loaded
}

// Remove deletes a key from the store.
func (s *Store) Remove(key string) {
	if s.lockable() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
	}

	// Only reduce coounter and attempt delete if the entry exists.
	if _, ok := s.Get(key); ok {
		s.count.Dec()
		s.Delete(key)
	}
}

// Get gets the value of a mapped key.
func (s *Store) Get(key string) (value interface{}, ok bool) {
	return s.Load(key)
}

// RangeAll calls an input method for each entry.
func (s *Store) RangeAll(r func(k, v interface{}) bool) {
	s.Range(func(k, v interface{}) bool {
		return r(k, v)
	})
}

// GetKeyMap returns a map of all keys.
func (s *Store) GetKeyMap() map[string]bool {
	keys := make(map[string]bool, 0)
	s.Range(func(k, v interface{}) bool {
		keys[k.(string)] = true
		return true
	})
	return keys
}

// NotifyDidChange triggers a change notification.
func (s *Store) NotifyDidChange() {
	if s.lockable() {
		s.cond.Broadcast()
	}
}

// WaitForDataChange creates a waiting lock.
func (s *Store) WaitForDataChange(ctx context.Context) {
	wait := make(chan bool, 1)
	if !s.lockable() {
		return
	}

	// Force a broadcast if context is closed.
	go func() {
		select {
		case <-ctx.Done():
			s.cond.L.Lock()
			s.cond.Broadcast()
			s.cond.L.Unlock()
			return
		case <-wait:
			return
		}
	}()

	s.cond.L.Lock()
	s.cond.Wait()
	wait <- true
	s.cond.L.Unlock()
}

// Flush clears the store.
func (s *Store) Flush() {
	s.Range(func(k, v interface{}) bool {
		s.count.Dec()
		s.Delete(k)
		return true
	})
	if s.lockable() {
		s.cond.Broadcast()
	}
}

// New returns a new store.
func New(lockable bool) *Store {
	s := &Store{
		count: atomiccounter.New(),
	}
	if lockable {
		s.cond = sync.NewCond(new(sync.Mutex))
	}

	return s
}

// When a context is closed we broadcast
// When a host disconnects, the context is closed
// The closed context is triggering all other waits.
