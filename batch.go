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
type jobs []*job

type batch struct {
	sync.RWMutex
	jobs
	created KV
	updated KV
	deleted KV
	store   *Store
}

// Insert adds an insert job to the batch.
func (b *batch) Insert(key string, val interface{}) {
	b.Lock()
	defer b.Unlock()
	b.jobs = append(b.jobs, &job{oppInsert, key, val})
}

// InsertUnique adds a unique insert job to the batch.
func (b *batch) InsertUnique(key string, val interface{}) {
	b.Lock()
	defer b.Unlock()
	b.jobs = append(b.jobs, &job{oppInsertUnique, key, val})
}

// Remove adds a remove job to the batch.
func (b *batch) Remove(key string) {
	b.Lock()
	defer b.Unlock()
	b.jobs = append(b.jobs, &job{method: oppRemove, key: key})
}

// Execute runs each batched job.
func (b *batch) Execute() {
	var wg sync.WaitGroup
	for _, v := range b.jobs {
		wg.Add(1)
		go b.do(v, &wg)
	}
	wg.Wait()

	if len(b.created) > 0 && b.store.onBatchInsert != nil {
		b.store.onBatchInsert(b.created)
	}

	if len(b.updated) > 0 && b.store.onBatchUpdate != nil {
		b.store.onBatchUpdate(b.updated)
	}

	if len(b.deleted) > 0 && b.store.onBatchRemove != nil {
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
			b.created[j.key] = doc
		} else if !unique {
			b.updated[j.key] = doc
		}
	case oppRemove:
		if doc, success := b.store.remove(j.key, options{runCallbacks: false}); success {
			b.deleted[j.key] = doc
		}
	}
}

// Batch returns a new batch operation struct.
func (s *Store) Batch() *batch {
	return &batch{
		store:   s,
		created: KV{},
		updated: KV{},
		deleted: KV{},
	}
}
