package backfill

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type del struct {
	repo string
	path string
}

type Memjob struct {
	repo      string
	state     string
	lk        sync.Mutex
	delBuffer []*del

	createdAt time.Time
	updatedAt time.Time
}

// Memstore is a simple in-memory implementation of the Backfill Store interface
type Memstore struct {
	lk   sync.RWMutex
	jobs map[string]*Memjob
}

func NewMemstore() *Memstore {
	return &Memstore{
		jobs: make(map[string]*Memjob),
	}
}

func (s *Memstore) EnqueueJob(repo string) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if _, ok := s.jobs[repo]; ok {
		return fmt.Errorf("job already exists for repo %s", repo)
	}

	j := &Memjob{
		repo:      repo,
		createdAt: time.Now(),
		updatedAt: time.Now(),
		state:     StateEnqueued,
		delBuffer: []*del{},
	}
	s.jobs[repo] = j
	return nil
}

func (s *Memstore) GetJob(ctx context.Context, repo string) (Job, error) {
	s.lk.RLock()
	defer s.lk.RUnlock()

	j, ok := s.jobs[repo]
	if !ok || j == nil {
		return nil, nil
	}
	return j, nil
}

func (s *Memstore) GetNextEnqueuedJob(ctx context.Context) (Job, error) {
	s.lk.RLock()
	defer s.lk.RUnlock()

	for _, j := range s.jobs {
		if j.State() == StateEnqueued {
			return j, nil
		}
	}
	return nil, nil
}

func (j *Memjob) Repo() string {
	return j.repo
}

func (j *Memjob) State() string {
	j.lk.Lock()
	defer j.lk.Unlock()

	return j.state
}

func (j *Memjob) SetState(ctx context.Context, state string) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	j.state = state
	j.updatedAt = time.Now()
	return nil
}

func (j *Memjob) BufferDelete(ctx context.Context, repo, path string) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	j.delBuffer = append(j.delBuffer, &del{
		repo: repo,
		path: path,
	})

	j.updatedAt = time.Now()
	return nil
}

func (j *Memjob) ForEachBufferedDelete(ctx context.Context, fn func(repo, path string) error) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	for _, del := range j.delBuffer {
		if err := fn(del.repo, del.path); err != nil {
			return err
		}
	}
	return nil
}

func (j *Memjob) ClearBufferedDeletes(ctx context.Context) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	j.delBuffer = []*del{}
	j.updatedAt = time.Now()
	return nil
}
