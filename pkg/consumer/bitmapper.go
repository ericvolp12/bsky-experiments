package consumer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"golang.org/x/sync/semaphore"
)

// Bitmapper is a service for tracking bitmaps
type Bitmapper struct {
	Store *store.Store
}

// NewBitmapper creates a new Bitmapper
func NewBitmapper(store *store.Store) (*Bitmapper, error) {
	return &Bitmapper{
		Store: store,
	}, nil
}

func (bm *Bitmapper) AddMember(ctx context.Context, key string, memberDID string) error {
	ctx, span := tracer.Start(ctx, "AddMember")
	defer span.End()

	// Get the actor by DID
	actor, err := bm.Store.Queries.GetActorByDID(ctx, memberDID)
	if err != nil {
		return fmt.Errorf("failed to get actor by DID: %w", err)
	}

	// Load the bitmap in a transaction, add the member, and save the bitmap
	tx, err := bm.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	qtx := bm.Store.Queries.WithTx(tx)
	rbm := roaring.NewBitmap()
	bitmap, err := qtx.GetBitmapByID(ctx, key)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to get bitmap by ID: %w", err)
		}
	} else {
		_, err = rbm.FromBuffer(bitmap.Bitmap)
		if err != nil {
			return fmt.Errorf("failed to load bitmap from buffer: %w", err)
		}
	}

	if actor.ID.Valid {
		rbm.Add(uint32(actor.ID.Int64))
	}

	newBitmap, err := rbm.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to convert bitmap to bytes: %w", err)
	}

	err = qtx.UpsertBitmap(ctx, store_queries.UpsertBitmapParams{
		ID:        key,
		Bitmap:    newBitmap,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to upsert bitmap: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (bm *Bitmapper) RemoveMember(ctx context.Context, key string, memberDID string) error {
	ctx, span := tracer.Start(ctx, "RemoveMember")
	defer span.End()

	// Get the actor by DID
	actor, err := bm.Store.Queries.GetActorByDID(ctx, memberDID)
	if err != nil {
		return fmt.Errorf("failed to get actor by DID: %w", err)
	}

	// Load the bitmap in a transaction, remove the member, and save the bitmap
	tx, err := bm.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	qtx := bm.Store.Queries.WithTx(tx)
	rbm := roaring.NewBitmap()
	bitmap, err := qtx.GetBitmapByID(ctx, key)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to get bitmap by ID: %w", err)
		}
	} else {
		_, err = rbm.FromBuffer(bitmap.Bitmap)
		if err != nil {
			return fmt.Errorf("failed to load bitmap from buffer: %w", err)
		}
	}

	if actor.ID.Valid {
		rbm.Remove(uint32(actor.ID.Int64))
	}

	newBitmap, err := rbm.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to convert bitmap to bytes: %w", err)
	}

	err = qtx.UpsertBitmap(ctx, store_queries.UpsertBitmapParams{
		ID:        key,
		Bitmap:    newBitmap,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to upsert bitmap: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (bm *Bitmapper) GetBitmap(ctx context.Context, key string) (*roaring.Bitmap, error) {
	ctx, span := tracer.Start(ctx, "GetBitmap")
	defer span.End()

	bitmap, err := bm.Store.Queries.GetBitmapByID(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get bitmap by ID: %w", err)
	}

	rbm := roaring.NewBitmap()
	_, err = rbm.FromBuffer(bitmap.Bitmap)
	if err != nil {
		return nil, fmt.Errorf("failed to load bitmap from buffer: %w", err)
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

func (bm *Bitmapper) GetIntersection(ctx context.Context, keys []string) (*roaring.Bitmap, error) {
	ctx, span := tracer.Start(ctx, "GetIntersection")
	defer span.End()

	bitmaps := make([]*roaring.Bitmap, len(keys))

	// Fetch all bitmaps in parallel
	sem := semaphore.NewWeighted(10)
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
			bitmaps[j] = rbm
		}(i, key)
	}

	if err := sem.Acquire(ctx, 10); err != nil {
		return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	// Check if any bitmaps failed to load
	for i, rbm := range bitmaps {
		if rbm == nil {
			return nil, fmt.Errorf("failed to load bitmap for key %s", keys[i])
		}
	}

	// Calculate the intersection
	intersectionBM := roaring.FastAnd(bitmaps...)

	return intersectionBM, nil
}

func (bm *Bitmapper) GetUnion(ctx context.Context, keys []string) (*roaring.Bitmap, error) {
	ctx, span := tracer.Start(ctx, "GetUnion")
	defer span.End()

	bitmaps := make([]*roaring.Bitmap, len(keys))

	// Fetch all bitmaps in parallel
	sem := semaphore.NewWeighted(10)
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
			bitmaps[j] = rbm
		}(i, key)
	}

	if err := sem.Acquire(ctx, 10); err != nil {
		return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	// Check if any bitmaps failed to load
	for i, rbm := range bitmaps {
		if rbm == nil {
			return nil, fmt.Errorf("failed to load bitmap for key %s", keys[i])
		}
	}

	// Calculate the union
	unionBM := roaring.FastOr(bitmaps...)

	return unionBM, nil
}
