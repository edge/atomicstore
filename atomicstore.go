// Edge Network
// (c) 2021 Edge Network technologies Ltd.
package atomicstore

import (
	"context"
	"sync"

	"github.com/edge/atomiccounter"
)

type options struct {
	unique       bool
	runCallbacks bool
}

var (
	defaultCallback      = func(string, interface{}) {}
	defaultBatchCallback = func(KV) {}
)

// KV stores a map of keyed entries and is used for batch actions,
type KV map[interface{}]interface{}

// Store stores mapped data.
type Store struct {
	cond          *sync.Cond
	count         *atomiccounter.Counter
	onInsert      func(string, interface{})
	onUpdate      func(string, interface{})
	onRemove      func(string, interface{})
	onBatchInsert func(KV)
	onBatchUpdate func(KV)
	onBatchRemove func(KV)
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

func (s *Store) insert(key, val interface{}, o options) (interface{}, bool) {
	if s.lockable() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
	}

	// Unique values only get inserted if they don't already exist.
	if o.unique {
		resp, loaded := s.LoadOrStore(key, val)
		if !loaded {
			s.count.Inc()
			if o.runCallbacks && s.onInsert != nil {
				s.onInsert(key.(string), resp)
			}
		}
		return resp, loaded
	}

	// Check for value
	_, exists := s.Get(key)
	s.Store(key, val)

	if !exists {
		s.count.Inc()
		if o.runCallbacks {
			s.onInsert(key.(string), val)
		}
	} else if o.runCallbacks {
		s.onUpdate(key.(string), val)
	}

	return val, exists
}

// Insert creates a new entry or overwrites the existing.
func (s *Store) Insert(key string, val interface{}) (interface{}, bool) {
	return s.insert(key, val, options{unique: false, runCallbacks: true})
}

// InsertUnique creates a new entry if the key doesn't exist.
func (s *Store) InsertUnique(key string, val interface{}) (interface{}, bool) {
	return s.insert(key, val, options{unique: true, runCallbacks: true})
}

func (s *Store) remove(key interface{}, o options) (interface{}, bool) {
	if s.lockable() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
	}

	// Only reduce coounter and attempt delete if the entry exists.
	if resp, ok := s.Get(key); ok {
		s.count.Dec()
		s.Delete(key)
		// This entry already exists. Overwrite it.
		if o.runCallbacks {
			s.onRemove(key.(string), resp)
		}
		return resp, true
	}
	return nil, false
}

// Remove deletes a key from the store.
func (s *Store) Remove(key interface{}) bool {
	_, success := s.remove(key, options{runCallbacks: true})
	return success
}

// Get gets the value of a mapped key.
func (s *Store) Get(key interface{}) (value interface{}, ok bool) {
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

// OnBatchInsertHandler adds a callback handler for batch inserts.
func (s *Store) OnBatchInsertHandler(f func(KV)) {
	s.onBatchInsert = f
}

// OnBatchInsertHandler adds a callback handler for batch updates.
func (s *Store) OnBatchUpdateHandler(f func(KV)) {
	s.onBatchUpdate = f
}

// OnBatchInsertHandler adds a callback handler for batch removals.
func (s *Store) OnBatchRemoveHandler(f func(KV)) {
	s.onBatchRemove = f
}

// New returns a new store.
func New(lockable bool) *Store {
	s := &Store{
		count:         atomiccounter.New(),
		onInsert:      defaultCallback,
		onUpdate:      defaultCallback,
		onRemove:      defaultCallback,
		onBatchInsert: defaultBatchCallback,
		onBatchUpdate: defaultBatchCallback,
		onBatchRemove: defaultBatchCallback,
	}
	if lockable {
		s.cond = sync.NewCond(new(sync.Mutex))
	}

	return s
}
