package bitmapper

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"go.opentelemetry.io/otel/attribute"
)

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

// UpdateUIDMapping updates the UID to string ID mapping in the meta database
func (g *Group) updateUIDMapping(ctx context.Context, uid uint32, stringID string) error {
	if _, err := g.metaDB.ExecContext(ctx, `INSERT OR REPLACE INTO entity_map (uid, string_id) VALUES (?, ?);`, uid, stringID); err != nil {
		return fmt.Errorf("failed to update uid mapping: %w", err)
	}

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
		compressed_bitmap BLOB NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("failed to create entities table: %w", err)
	}

	g.dbCache.Add(shard, db)

	return db, nil
}
