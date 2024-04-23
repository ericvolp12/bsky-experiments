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

type Entity struct {
	LK sync.RWMutex
	BM *roaring.Bitmap
}

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
	entities *arc.ARCCache[uint32, *Entity]
	dbCache  *arc.ARCCache[int, *sql.DB]

	// Bookkeeping for Persisting the Group
	dbDir     string
	metaDB    *sql.DB
	shardSize uint32
}

type Bitmapper struct {
	groups  map[string]*Group
	groupLk sync.RWMutex

	CrossGroupLk sync.RWMutex

	// Root directory for SQLite files, one subfolder per group
	dbDir string
}

type GroupConfig struct {
	Name      string
	ShardSize uint32
	CacheSize int
	dbDir     string
}

type BitmapperConfig struct {
	// Root directory for SQLite files, one subfolder per group
	DBDir  string
	Groups []GroupConfig
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
func NewBitmapper(ctx context.Context, cfg BitmapperConfig) (*Bitmapper, error) {
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

	for _, groupCfg := range cfg.Groups {
		// Create the group directory if it doesn't exist
		if err := os.MkdirAll(fmt.Sprintf("%s/%s", cfg.DBDir, groupCfg.Name), 0755); err != nil {
			return nil, fmt.Errorf("failed to create group directory: %w", err)
		}

		groupCfg.dbDir = cfg.DBDir

		group, err := NewGroup(ctx, groupCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create group: %w", err)
		}

		bitmapper.groups[groupCfg.Name] = group
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

func (g *Group) PeekNextUID() uint32 {
	g.nextLk.Lock()
	defer g.nextLk.Unlock()

	uid := g.nextUID

	return uid
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
	// Check the cache
	if e, ok := g.entities.Get(uid); ok {
		return e, nil
	}

	// Load the entity from the database
	bm, err := g.loadBitmap(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to load entity: %w", err)
	}

	ent := &Entity{BM: bm, LK: sync.RWMutex{}}

	// Add the entity to the cache
	g.entities.Add(uid, ent)

	return ent, nil
}

// UpdateEntity updates an Entity in the cache and database
func (g *Group) UpdateEntity(ctx context.Context, uid uint32, bm *roaring.Bitmap) error {
	// Update the cache
	ent := &Entity{BM: bm, LK: sync.RWMutex{}}
	g.entities.Add(uid, ent)

	// Update the database
	if err := g.updateBitmap(ctx, uid, bm); err != nil {
		return fmt.Errorf("failed to update entity: %w", err)
	}

	return nil
}

func (g *Group) loadBitmap(ctx context.Context, uid uint32) (*roaring.Bitmap, error) {
	shard := int(uid / g.shardSize)
	db, err := g.initShardDB(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize shard db: %w", err)
	}

	var bmBytes []byte
	if err := db.QueryRowContext(ctx, `SELECT compressed_bitmap FROM entities WHERE uid = ?`, uid).Scan(&bmBytes); err != nil {
		// If the entity doesn't exist, return an empty bitmap
		if err == sql.ErrNoRows {
			return roaring.NewBitmap(), nil
		}
		return nil, fmt.Errorf("failed to query entity: %w", err)
	}

	bm := roaring.New()
	if _, err := bm.FromBuffer(bmBytes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	return bm, nil
}

func (g *Group) updateBitmap(ctx context.Context, uid uint32, bm *roaring.Bitmap) error {
	shard := int(uid / g.shardSize)
	db, err := g.initShardDB(ctx, shard)
	if err != nil {
		return fmt.Errorf("failed to initialize shard db: %w", err)
	}

	bmBytes, err := bm.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to marshal bitmap: %w", err)
	}

	if _, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO entities (uid, compressed_bitmap) VALUES (?, ?);`, uid, bmBytes); err != nil {
		return fmt.Errorf("failed to update entity: %w", err)
	}

	return nil
}

func (g *Group) UpdateUIDMapping(ctx context.Context, uid uint32, stringID string) error {
	if _, err := g.metaDB.ExecContext(ctx, `INSERT OR REPLACE INTO entity_map (uid, string_id) VALUES (?, ?);`, uid, stringID); err != nil {
		return fmt.Errorf("failed to update uid mapping: %w", err)
	}

	return nil
}

// loadFromMetaDB loads the UID to String mappings from the meta database
func (g *Group) loadFromMetaDB(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "loadFromMetaDB")
	defer span.End()

	span.SetAttributes(attribute.String("group", g.name))

	rows, err := g.metaDB.QueryContext(ctx, `SELECT uid, string_id FROM entity_map;`)
	if err != nil {
		return fmt.Errorf("failed to query entity map: %w", err)
	}
	defer rows.Close()

	g.stuLk.Lock()
	g.utsLk.Lock()
	g.nextLk.Lock()
	defer g.stuLk.Unlock()
	defer g.utsLk.Unlock()
	defer g.nextLk.Unlock()

	var nextUID uint32

	for rows.Next() {
		var uid uint32
		var stringID string
		if err := rows.Scan(&uid, &stringID); err != nil {
			return fmt.Errorf("failed to scan entity map: %w", err)
		}

		g.stu[stringID] = uid
		g.uts[uid] = stringID
		if uid >= nextUID {
			nextUID = uid + 1
		}
	}

	g.nextUID = nextUID

	return nil
}

// initializes a MetaDB for the group at bitmapper_db_dir/group_name/meta.db
func (g *Group) initMetaDB(ctx context.Context) (*sql.DB, error) {
	metaDBPath := fmt.Sprintf("%s/%s", g.dbDir, MetaDBName)
	// Open the database
	db, err := sql.Open("sqlite3", metaDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at %q: %w", metaDBPath, err)
	}

	// Set pragmas
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL;`); err != nil {
		return nil, fmt.Errorf("failed to set journal mode at %q: %w", metaDBPath, err)
	}

	// Set synchronous mode to off to increase write performance
	// not super risky with WAL mode and redundant disks
	if _, err := db.ExecContext(ctx, `PRAGMA synchronous=off;`); err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}

	// Create the table if it doesn't exist
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS entity_map (
		uid INTEGER PRIMARY KEY,
		string_id TEXT NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("failed to create entity_map table: %w", err)
	}

	// Initialize the string_id index if it doesn't exist
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS string_id_idx ON entity_map (string_id);`); err != nil {
		return nil, fmt.Errorf("failed to create string_id index: %w", err)
	}

	return db, nil
}

// initializes a ShardDB for the group at bitmapper_db_dir/group_name/shard_%d.db
func (g *Group) initShardDB(ctx context.Context, shard int) (*sql.DB, error) {
	db, ok := g.dbCache.Get(shard)
	if ok {
		return db, nil
	}

	dbPath := fmt.Sprintf("%s/"+ShardDBPattern, g.dbDir, shard)
	// Open the database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set pragmas
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL;`); err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}

	if _, err := db.ExecContext(ctx, `PRAGMA synchronous=off;`); err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}

	// Create the table if it doesn't exist
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS entities (
		uid INTEGER PRIMARY KEY,
		string_id TEXT NOT NULL,
		compressed_bitmap BLOB NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("failed to create entities table: %w", err)
	}

	g.dbCache.Add(shard, db)

	return db, nil
}
