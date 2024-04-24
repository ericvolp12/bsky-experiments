package bitmapper

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/hashicorp/golang-lru/arc/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// MetaDBName is the filename of the meta database
var MetaDBName = "meta.db"

// ShardDBPattern is the pattern for the shard database filenames
var ShardDBPattern = "shard_%d.db"

var tracer = otel.Tracer("bitmapper")

// Bitmapper is a struct that manages collections of Roaring Bitmaps
// Collections of bitmaps are organized into Groups that share UID mappings
// Each Group has its own SQLite database for persisting the UID to string ID mappings
// as well as a set of sharded SQLite databases for persisting the Roaring Bitmaps
type Bitmapper struct {
	groups  map[string]*Group
	groupLk sync.RWMutex

	CrossGroupLk sync.RWMutex

	// Root directory for SQLite files, one subfolder per Group
	dbDir string
}

// Group is a struct that manages a collection of Roaring Bitmaps
// Bitmaps are organized by UID, with mappings to string IDs
// Each Group has its own SQLite database for persisting the UID to string ID mappings
// as well as a set of sharded SQLite databases for persisting the Roaring Bitmaps
// Groups use an ARC cache for lazily loading Bitmaps from disk to keep memory usage manageable
// Writes to Entities in a Group are immediately persisted to disk
type Group struct {
	name string

	// UID assignment
	nextUID uint32
	nextLk  sync.Mutex

	// UID to String Mappings
	uts   map[uint32]string
	utsLk sync.RWMutex
	stu   map[string]uint32
	stuLk sync.RWMutex

	// LRU Cache for Entity Bitmaps
	entities  *arc.ARCCache[uint32, *Entity]
	cacheLock sync.Mutex
	dbCache   *arc.ARCCache[int, *sql.DB]

	// Bookkeeping for Persisting the Group
	dbDir     string
	metaDB    *sql.DB
	shardSize uint32
}

// Entity is a member of a Group
// It contains a Roaring Bitmap and a lock for concurrent access
// After modifying an Entitiy, use the Group's UpdateEntity method to persist the changes
type Entity struct {
	LK *sync.RWMutex
	BM *roaring.Bitmap
}

// Config is the configuration for a Bitmapper instance
type Config struct {
	// Root directory for SQLite files, one subfolder per group
	DBDir  string
	Groups []GroupConfig
}

// GroupConfig is the configuration for a Group instance
type GroupConfig struct {
	Name      string
	ShardSize uint32
	CacheSize int
	dbDir     string
}

// NewGroup creates a new Group instance
func NewGroup(ctx context.Context, cfg GroupConfig) (*Group, error) {
	ctx, span := tracer.Start(ctx, "NewGroup")
	defer span.End()

	span.SetAttributes(attribute.String("name", cfg.Name), attribute.Int("shard_size", int(cfg.ShardSize)))

	ents, err := arc.NewARC[uint32, *Entity](cfg.CacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create entity cache: %w", err)
	}

	dbCache, err := arc.NewARC[int, *sql.DB](100)
	if err != nil {
		return nil, fmt.Errorf("failed to create db cache: %w", err)
	}

	dbDir := fmt.Sprintf("%s/%s", cfg.dbDir, cfg.Name)

	group := &Group{
		name:      cfg.Name,
		nextUID:   0,
		uts:       make(map[uint32]string),
		stu:       make(map[string]uint32),
		entities:  ents,
		dbDir:     dbDir,
		shardSize: cfg.ShardSize,
		dbCache:   dbCache,
	}

	metaDB, err := group.initMetaDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize meta db: %w", err)
	}
	group.metaDB = metaDB

	// Load the UID to String mappings from the meta database
	if err := group.loadFromMetaDB(ctx); err != nil {
		return nil, fmt.Errorf("failed to load from meta db: %w", err)
	}

	return group, nil
}

// NewBitmapper creates a new Bitmapper instance and initializes the groups
func NewBitmapper(ctx context.Context, cfg Config) (*Bitmapper, error) {
	ctx, span := tracer.Start(ctx, "NewBitmapper")
	defer span.End()

	span.SetAttributes(attribute.String("db_dir", cfg.DBDir))

	bitmapper := &Bitmapper{
		groups: make(map[string]*Group),
		dbDir:  cfg.DBDir,
	}

	// Create the root directory if it doesn't exist
	if err := os.MkdirAll(cfg.DBDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	wg := sync.WaitGroup{}
	errs := make([]error, len(cfg.Groups))

	for i, groupCfg := range cfg.Groups {
		wg.Add(1)
		go func(groupCfg GroupConfig) {
			defer wg.Done()
			// Create the group directory if it doesn't exist
			if err := os.MkdirAll(fmt.Sprintf("%s/%s", cfg.DBDir, groupCfg.Name), 0755); err != nil {
				errs[i] = fmt.Errorf("failed to create group directory: %w", err)
				return
			}

			groupCfg.dbDir = cfg.DBDir

			group, err := NewGroup(ctx, groupCfg)
			if err != nil {
				errs[i] = fmt.Errorf("failed to create group: %w", err)
				return
			}

			bitmapper.groups[groupCfg.Name] = group
		}(groupCfg)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return bitmapper, nil
}

// GetGroup retrieves a Group by name
func (b *Bitmapper) GetGroup(ctx context.Context, name string) (*Group, error) {
	ctx, span := tracer.Start(ctx, "GetGroup")
	defer span.End()

	span.SetAttributes(attribute.String("name", name))

	b.groupLk.RLock()
	group, ok := b.groups[name]
	b.groupLk.RUnlock()
	if !ok {
		return nil, fmt.Errorf("group not found: %s", name)
	}

	return group, nil
}

// PeekNextUID returns the next UID that will be assigned
func (g *Group) PeekNextUID() uint32 {
	g.nextLk.Lock()
	defer g.nextLk.Unlock()

	uid := g.nextUID

	return uid
}

// GetShardSize returns the shard size for the group
func (g *Group) GetShardSize() uint32 {
	return g.shardSize
}

// ErrorUIDNotFound is returned when a UID is not found
var ErrorUIDNotFound = fmt.Errorf("uid not found")

// GetUID resolves a string ID to a UID
// If the UID is not found and initOnMiss is true, a new UID is initialized
// The bool return value indicates whether the UID was initialized
func (g *Group) GetUID(ctx context.Context, stringID string, initOnMiss, flush bool) (uint32, bool, error) {
	g.stuLk.RLock()
	uid, ok := g.stu[stringID]
	g.stuLk.RUnlock()
	if ok {
		return uid, false, nil
	}

	if !initOnMiss {
		return 0, false, ErrorUIDNotFound
	}

	// If there's no mapping, initialize a new UID
	g.nextLk.Lock()
	uid = g.nextUID
	g.nextUID++
	// Hold the lock while updating the mapping
	defer g.nextLk.Unlock()

	// Update the mapping
	g.stuLk.Lock()
	g.stu[stringID] = uid
	g.stuLk.Unlock()

	g.utsLk.Lock()
	g.uts[uid] = stringID
	g.utsLk.Unlock()

	if flush {
		if err := g.UpdateUIDMapping(ctx, uid, stringID); err != nil {
			return 0, false, fmt.Errorf("failed to update uid mapping: %w", err)
		}
	}

	return uid, true, nil
}

// GetStringID resolves a UID to a string ID
func (g *Group) GetStringID(ctx context.Context, uid uint32) (string, error) {
	g.utsLk.RLock()
	stringID, ok := g.uts[uid]
	g.utsLk.RUnlock()
	if ok {
		return stringID, nil
	}

	return "", ErrorUIDNotFound
}

// GetEntity retrieves an Entity from the cache or database
func (g *Group) GetEntity(ctx context.Context, uid uint32) (*Entity, error) {
	// Grab the cache lock and check if the entity is already loaded
	g.cacheLock.Lock()
	if e, ok := g.entities.Get(uid); ok {
		g.cacheLock.Unlock()
		return e, nil
	}

	// Initialize a lock for the entity so anyone calling GetEntity will wait
	// but not block calls to GetEntity for other UIDs while we load the entity
	ent := &Entity{LK: &sync.RWMutex{}}
	ent.LK.Lock()
	defer ent.LK.Unlock()
	g.entities.Add(uid, ent)
	g.cacheLock.Unlock()

	// Load the entity from the database
	var err error
	ent.BM, err = g.loadBitmap(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to load entity: %w", err)
	}

	return ent, nil
}

// UpdateEntity updates an Entity in the cache and database
// Only call while holding the Entity's lock
func (g *Group) UpdateEntity(ctx context.Context, uid uint32, ent *Entity) error {
	// Update the cache
	g.entities.Add(uid, ent)

	// Update the database
	if err := g.updateBitmap(ctx, uid, ent.BM); err != nil {
		return fmt.Errorf("failed to update entity: %w", err)
	}

	return nil
}

// UpdateUIDMapping explicitly updates the UID to string ID mapping in the meta database
// This should only be called by an external process when bulk updating the mapping
// To initialize a new UID, use GetUID with initOnMiss=true (which will call this function)
func (g *Group) UpdateUIDMapping(ctx context.Context, uid uint32, stringID string) error {
	return g.updateUIDMapping(ctx, uid, stringID)
}
