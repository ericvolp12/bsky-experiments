package backfill

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/repomgr"
	typegen "github.com/whyrusleeping/cbor-gen"
)

type bufferedOp struct {
	kind string
	rec  *typegen.CBORMarshaler
}

type Memjob struct {
	repo        string
	state       string
	lk          sync.Mutex
	bufferedOps map[string][]*bufferedOp

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
		repo:        repo,
		createdAt:   time.Now(),
		updatedAt:   time.Now(),
		state:       StateEnqueued,
		bufferedOps: map[string][]*bufferedOp{},
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

func (j *Memjob) ShouldBufferOp(ctx context.Context, kind, path string) (bool, error) {
	j.lk.Lock()
	defer j.lk.Unlock()

	_, hasBufferedEvent := j.bufferedOps[path]
	shouldBuffer := false

	ek := repomgr.EventKind(kind)

	switch ek {
	case repomgr.EvtKindCreateRecord:
		// Only buffer creates if there is already a buffered event at this path
		// Otherwise it's safe to just process the create
		if hasBufferedEvent {
			shouldBuffer = true
		}
	case repomgr.EvtKindUpdateRecord:
		shouldBuffer = true
	case repomgr.EvtKindDeleteRecord:
		shouldBuffer = true
	}

	return shouldBuffer, nil
}

func (j *Memjob) BufferOp(ctx context.Context, kind, path string, rec *typegen.CBORMarshaler) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	if _, ok := j.bufferedOps[path]; !ok {
		j.bufferedOps[path] = []*bufferedOp{}
	}

	j.bufferedOps[path] = append(j.bufferedOps[path], &bufferedOp{
		kind: kind,
		rec:  rec,
	})

	j.updatedAt = time.Now()
	return nil
}

func (j *Memjob) ForEachBufferedOp(ctx context.Context, fn func(kind, path string, rec *typegen.CBORMarshaler) error) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	for path, ops := range j.bufferedOps {
		for _, op := range ops {
			if err := fn(op.kind, path, op.rec); err != nil {
				return err
			}
		}
	}

	return nil
}

func (j *Memjob) ClearBufferedOps(ctx context.Context) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	j.bufferedOps = map[string][]*bufferedOp{}
	j.updatedAt = time.Now()
	return nil
}
