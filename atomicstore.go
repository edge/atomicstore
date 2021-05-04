// Edge Network
// (c) 2021 Edge Network technologies Ltd.
package atomicstore

import (
	"context"
	"sync"

	"github.com/edge/atomiccounter"
)

// Store stores mapped data.
type Store struct {
	cond     *sync.Cond
	count    *atomiccounter.Counter
	onInsert func(string, interface{})
	onUpdate func(string, interface{})
	onRemove func(string, interface{})
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

func (s *Store) insert(key string, val interface{}, unique bool) (interface{}, bool) {
	if s.lockable() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
	}

	// Unique values only get inserted if they don't already exist.
	if unique {
		resp, loaded := s.LoadOrStore(key, val)
		if !loaded {
			s.count.Inc()
		}
		return resp, loaded
	}

	// Check for value
	_, exists := s.Get(key)
	s.Store(key, val)

	if !exists {
		s.count.Inc()
	}

	return val, exists
}

// Insert creates a new entry or overwrites the existing.
func (s *Store) Insert(key string, val interface{}) (interface{}, bool) {
	resp, loaded := s.insert(key, val, false)

	// This was an update
	if loaded && s.onUpdate != nil {
		s.onUpdate(key, resp)
		return resp, loaded
	}
	// This was new
	if !loaded && s.onInsert != nil {
		s.onInsert(key, resp)
	}
	return resp, loaded
}

// InsertUnique creates a new entry if the key doesn't exist.
func (s *Store) InsertUnique(key string, val interface{}) (interface{}, bool) {
	resp, loaded := s.insert(key, val, true)

	if !loaded && s.onInsert != nil {
		s.onInsert(key, resp)
	}

	return resp, loaded
}

// Remove deletes a key from the store.
func (s *Store) Remove(key string) bool {
	if s.lockable() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
	}

	// Only reduce coounter and attempt delete if the entry exists.
	if resp, ok := s.Get(key); ok {
		s.count.Dec()
		s.Delete(key)
		// This entry already exists. Overwrite it.
		if s.onRemove != nil {
			s.onRemove(key, resp)
		}
		return true
	}
	return false
}

// Get gets the value of a mapped key.
func (s *Store) Get(key string) (value interface{}, ok bool) {
	return s.Load(key)
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
		s.Remove(k.(string))
		return true
	})
	if s.lockable() {
		s.cond.Broadcast()
	}
}

// OnInsertHandler adds a callback handler for inserts.
func (s *Store) OnInsertHandler(f func(string, interface{})) {
	s.onInsert = f
}

// OnInsertHandler adds a callback handler for updates.
func (s *Store) OnUpdateHandler(f func(string, interface{})) {
	s.onUpdate = f
}

// OnInsertHandler adds a callback handler for removals.
func (s *Store) OnRemoveHandler(f func(string, interface{})) {
	s.onRemove = f
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
