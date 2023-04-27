package graph

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteReaderWriter is a struct that implements the ReaderWriter interface a Graph into/from SQLite databases.
type SQLiteReaderWriter struct {
	DB *sql.DB
}

// NewSQLiteReaderWriter initializes a new SQLiteReaderWriter object and returns it.
func NewSQLiteReaderWriter(filename string) (*SQLiteReaderWriter, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}

	createNodesTable := `
CREATE TABLE IF NOT EXISTS nodes (
	id TEXT PRIMARY KEY,
	handle TEXT
);
`

	createEdgesTable := `
CREATE TABLE IF NOT EXISTS edges (
	from_id TEXT,
	to_id TEXT,
	weight INTEGER,
	FOREIGN KEY (from_id) REFERENCES nodes (id),
	FOREIGN KEY (to_id) REFERENCES nodes (id),
	PRIMARY KEY (from_id, to_id)
);
`

	_, err = db.Exec(createNodesTable)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createEdgesTable)
	if err != nil {
		return nil, err
	}

	return &SQLiteReaderWriter{DB: db}, nil
}

// WriteGraph writes a Graph object to a SQLite database.
func (rw *SQLiteReaderWriter) WriteGraph(g Graph) error {
	tx, err := rw.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, node := range g.Nodes {
		_, err := tx.Exec("INSERT OR REPLACE INTO nodes (id, handle) VALUES (?, ?)", node.DID, node.Handle)
		if err != nil {
			return err
		}
	}

	for from, edges := range g.Edges {
		for to, weight := range edges {
			_, err := tx.Exec("INSERT OR REPLACE INTO edges (from_id, to_id, weight) VALUES (?, ?, ?)", from, to, weight)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// ReadGraph reads a Graph object from a SQLite database.
func (rw *SQLiteReaderWriter) ReadGraph() (Graph, error) {
	rows, err := rw.DB.Query("SELECT n1.id, n1.handle, n2.id, n2.handle, e.weight FROM edges e JOIN nodes n1 ON e.from_id = n1.id JOIN nodes n2 ON e.to_id = n2.id")
	if err != nil {
		return Graph{}, err
	}
	defer rows.Close()

	g := NewGraph()

	for rows.Next() {
		var fromDID, fromHandle, toDID, toHandle string
		var weight int
		err := rows.Scan(&fromDID, &fromHandle, &toDID, &toHandle, &weight)
		if err != nil {
			return Graph{}, err
		}
		fromNode := Node{DID: NodeID(fromDID), Handle: fromHandle}
		toNode := Node{DID: NodeID(toDID), Handle: toHandle}
		g.AddEdge(fromNode, toNode, weight)
	}

	return g, nil
}
