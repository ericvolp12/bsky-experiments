package consumer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"golang.org/x/sync/semaphore"
)

// Bitmapper is a service for tracking bitmaps
type Bitmapper struct {
	Store         *store.Store
	ActiveBitmaps map[string]*roaring.Bitmap
	lk            sync.RWMutex
	shutdown      chan chan error

	didCache *lru.ARCCache[string, uint32]
}

// NewBitmapper creates a new Bitmapper
func NewBitmapper(store *store.Store) (*Bitmapper, error) {
	didCache, err := lru.NewARC[string, uint32](500_000)
	if err != nil {
		return nil, fmt.Errorf("failed to create DID cache: %w", err)
	}

	bm := &Bitmapper{
		Store:         store,
		ActiveBitmaps: make(map[string]*roaring.Bitmap),
		shutdown:      make(chan chan error),
		didCache:      didCache,
	}

	// Start a routine to save bitmaps every 30 seconds and clear the active bitmaps
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for {
			select {
			case errCh := <-bm.shutdown:
				bm.lk.Lock()
				for key := range bm.ActiveBitmaps {
					err := bm.saveBM(context.Background(), key)
					if err != nil {
						slog.Error("failed to save bitmap", "error", err)
					}
				}
				bm.ActiveBitmaps = make(map[string]*roaring.Bitmap)
				bm.lk.Unlock()
				errCh <- nil
			case <-ticker.C:
				bm.lk.Lock()
				for key := range bm.ActiveBitmaps {
					err := bm.saveBM(context.Background(), key)
					if err != nil {
						slog.Error("failed to save bitmap", "error", err)
					}
				}
				bm.ActiveBitmaps = make(map[string]*roaring.Bitmap)
				bm.lk.Unlock()
			}
		}
	}()

	return bm, nil
}

func (bm *Bitmapper) Shutdown() error {
	errCh := make(chan error)
	bm.shutdown <- errCh
	return <-errCh
}

func (bm *Bitmapper) loadBM(ctx context.Context, key string) error {
	ctx, span := tracer.Start(ctx, "loadBM")
	defer span.End()

	bitmap, err := bm.Store.Queries.GetBitmapByID(ctx, key)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get bitmap by ID: %w", err)
	} else if errors.Is(err, sql.ErrNoRows) {
		bm.ActiveBitmaps[key] = roaring.NewBitmap()
		return nil
	}

	rbm := roaring.NewBitmap()
	_, err = rbm.FromBuffer(bitmap.Bitmap)
	if err != nil {
		return fmt.Errorf("failed to load bitmap from buffer: %w", err)
	}

	bm.ActiveBitmaps[key] = rbm

	return nil
}

func (bm *Bitmapper) saveBM(ctx context.Context, key string) error {
	ctx, span := tracer.Start(ctx, "saveBM")
	defer span.End()

	rbm, ok := bm.ActiveBitmaps[key]
	if !ok {
		return fmt.Errorf("bitmap not found")
	}

	bitmap, err := rbm.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to convert bitmap to bytes: %w", err)
	}

	err = bm.Store.Queries.UpsertBitmap(ctx, store_queries.UpsertBitmapParams{
		ID:        key,
		Bitmap:    bitmap,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to upsert bitmap: %w", err)
	}

	return nil
}

func (bm *Bitmapper) AddMember(ctx context.Context, key string, memberDID string) error {
	ctx, span := tracer.Start(ctx, "AddMember")
	defer span.End()

	// Get the actorID by DID
	actorID, ok := bm.didCache.Get(memberDID)
	if !ok {
		actor, err := bm.Store.Queries.GetActorByDID(ctx, memberDID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("failed to get actor by DID: %w", err)
		}
		actorID = uint32(actor.ID.Int64)
		bm.didCache.Add(memberDID, actorID)
	}

	// Add the member to the bitmap
	bm.lk.Lock()
	defer bm.lk.Unlock()

	rbm, ok := bm.ActiveBitmaps[key]
	if !ok {
		err := bm.loadBM(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to load bitmap: %w", err)
		}
		rbm = bm.ActiveBitmaps[key]
	}

	if actorID > 0 {
		rbm.Add(actorID)
	}

	return nil
}

func (bm *Bitmapper) RemoveMember(ctx context.Context, key string, memberDID string) error {
	ctx, span := tracer.Start(ctx, "RemoveMember")
	defer span.End()

	// Get the actorID by DID
	actorID, ok := bm.didCache.Get(memberDID)
	if !ok {
		actor, err := bm.Store.Queries.GetActorByDID(ctx, memberDID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("failed to get actor by DID: %w", err)
		}
		actorID = uint32(actor.ID.Int64)
		bm.didCache.Add(memberDID, actorID)
	}

	// Remove the member from the bitmap
	bm.lk.Lock()
	defer bm.lk.Unlock()

	rbm, ok := bm.ActiveBitmaps[key]
	if !ok {
		err := bm.loadBM(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to load bitmap: %w", err)
		}
		rbm = bm.ActiveBitmaps[key]
	}

	if actorID > 0 {
		rbm.Remove(actorID)
	}

	return nil
}

func (bm *Bitmapper) GetBitmap(ctx context.Context, key string) (*roaring.Bitmap, error) {
	ctx, span := tracer.Start(ctx, "GetBitmap")
	defer span.End()

	bm.lk.Lock()
	defer bm.lk.Unlock()

	rbm, ok := bm.ActiveBitmaps[key]
	if !ok {
		err := bm.loadBM(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to load bitmap: %w", err)
		}
		rbm = bm.ActiveBitmaps[key]
	}

	return rbm, nil
}

func (bm *Bitmapper) GetMembers(ctx context.Context, key string) ([]uint32, error) {
	ctx, span := tracer.Start(ctx, "GetMembers")
	defer span.End()

	rbm, err := bm.GetBitmap(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get bitmap: %w", err)
	}

	members := rbm.ToArray()
	return members, nil
}

var parallelism = int64(50)

func (bm *Bitmapper) GetIntersection(ctx context.Context, keys []string) (*roaring.Bitmap, error) {
	ctx, span := tracer.Start(ctx, "GetIntersection")
	defer span.End()

	foundBitmaps := make([]*roaring.Bitmap, len(keys))

	// Fetch all bitmaps in parallel
	sem := semaphore.NewWeighted(parallelism)
	for i, key := range keys {
		if err := sem.Acquire(ctx, 1); err != nil {
			return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
		}
		go func(j int, k string) {
			defer sem.Release(1)
			rbm, err := bm.GetBitmap(ctx, k)
			if err != nil {
				return
			}
			foundBitmaps[j] = rbm
		}(i, key)
	}

	if err := sem.Acquire(ctx, parallelism); err != nil {
		return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	bitmaps := []*roaring.Bitmap{}

	// Skip any nil bitmaps
	for _, rbm := range foundBitmaps {
		if rbm != nil {
			bitmaps = append(bitmaps, rbm)
		}
	}

	// Calculate the intersection
	intersectionBM := roaring.FastAnd(bitmaps...)

	return intersectionBM, nil
}

func (bm *Bitmapper) GetUnion(ctx context.Context, keys []string) (*roaring.Bitmap, error) {
	ctx, span := tracer.Start(ctx, "GetUnion")
	defer span.End()

	foundBitmaps := make([]*roaring.Bitmap, len(keys))

	// Fetch all bitmaps in parallel
	sem := semaphore.NewWeighted(parallelism)
	for i, key := range keys {
		if err := sem.Acquire(ctx, 1); err != nil {
			return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
		}
		go func(j int, k string) {
			defer sem.Release(1)
			rbm, err := bm.GetBitmap(ctx, k)
			if err != nil {
				return
			}
			foundBitmaps[j] = rbm
		}(i, key)
	}

	if err := sem.Acquire(ctx, parallelism); err != nil {
		return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	bitmaps := []*roaring.Bitmap{}

	// Skip any nil bitmaps
	for _, rbm := range foundBitmaps {
		if rbm != nil {
			bitmaps = append(bitmaps, rbm)
		}
	}

	// Calculate the union
	unionBM := roaring.FastOr(bitmaps...)

	return unionBM, nil
}
