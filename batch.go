// Edge Network
// (c) 2021 Edge Network technologies Ltd.
package atomicstore

import (
	"sync"
)

const (
	oppInsert       opp = "insert"
	oppInsertUnique opp = "insertUnique"
	oppRemove       opp = "remove"
)

type opp string
type job struct {
	method opp
	key    interface{}
	value  interface{}
}

type batch struct {
	sync.RWMutex
	jobs    []*job
	created *KV
	updated *KV
	deleted *KV
	store   *Store
}

// Insert adds an insert job to the batch.
func (b *batch) Insert(key, val interface{}) {
	b.Lock()
	defer b.Unlock()
	b.jobs = append(b.jobs, &job{
		method: oppInsert,
		key:    key,
		value:  val,
	})
}

// InsertUnique adds a unique insert job to the batch.
func (b *batch) InsertUnique(key, val interface{}) {
	b.Lock()
	defer b.Unlock()
	b.jobs = append(b.jobs, &job{
		method: oppInsertUnique,
		key:    key,
		value:  val,
	})
}

// Remove adds a remove job to the batch.
func (b *batch) Remove(key interface{}) {
	b.Lock()
	defer b.Unlock()
	b.jobs = append(b.jobs, &job{
		method: oppRemove,
		key:    key,
	})
}

// Execute runs each batched job.
func (b *batch) Execute() {
	var wg sync.WaitGroup
	for _, v := range b.jobs {
		wg.Add(1)
		go b.do(v, &wg)
	}
	wg.Wait()

	if b.created.Len() > 0 && b.store.onBatchInsert != nil {
		b.store.onBatchInsert(b.created)
	}

	if b.updated.Len() > 0 && b.store.onBatchUpdate != nil {
		b.store.onBatchUpdate(b.updated)
	}

	if b.deleted.Len() > 0 && b.store.onBatchRemove != nil {
		b.store.onBatchRemove(b.deleted)
	}
}

func (b *batch) do(j *job, wg *sync.WaitGroup) {
	defer wg.Done()
	switch j.method {
	case oppInsert, oppInsertUnique:
		unique := j.method == oppInsertUnique

		doc, exists := b.store.insert(j.key, j.value, options{
			unique:       unique,
			runCallbacks: false,
		})

		// If the document is update and this isn't a unique key
		if !exists {
			(*b.created)[j.key] = doc
		} else if !unique {
			(*b.updated)[j.key] = doc
		}
	case oppRemove:
		if doc, success := b.store.remove(j.key, options{runCallbacks: false}); success {
			(*b.deleted)[j.key] = doc
		}
	}
}

// Batch returns a new batch operation struct.
func (s *Store) Batch() *batch {
	return &batch{
		store:   s,
		jobs:    make([]*job, 0),
		created: &KV{},
		updated: &KV{},
		deleted: &KV{},
	}
}
