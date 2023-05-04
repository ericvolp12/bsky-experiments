package search

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // postgres driver
	"go.opentelemetry.io/otel"
)

type NotFoundError struct {
	error
}

// Post relationships
const (
	ReplyRelationship = "r"
	QuoteRelationship = "q"
)

type Post struct {
	ID                 string    `json:"id"`
	Text               string    `json:"text"`
	ParentPostID       *string   `json:"parent_post_id"`
	RootPostID         *string   `json:"root_post_id"`
	AuthorDID          string    `json:"author_did"`
	CreatedAt          time.Time `json:"created_at"`
	HasEmbeddedMedia   bool      `json:"has_embedded_media"`
	ParentRelationship *string   `json:"parent_relationship"` // null, "r", "q"
}

type PostView struct {
	Post         `json:"post"`
	AuthorHandle string `json:"author_handle"`
	Depth        int    `json:"depth"`
}

type Author struct {
	DID    string `json:"did"`
	Handle string `json:"handle"`
}

type PostRegistry struct {
	db *sql.DB
}

func NewPostRegistry(connectionString string) (*PostRegistry, error) {
	var db *sql.DB
	var err error

	for i := 0; i < 5; i++ {
		db, err = sql.Open("postgres", connectionString)
		if err != nil {
			return nil, err
		}

		err = db.Ping()
		if err == nil {
			break
		}

		db.Close() // Close the connection if it failed.
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		return nil, err
	}

	registry := &PostRegistry{
		db: db,
	}

	err = registry.initializeDB()
	if err != nil {
		return nil, err
	}

	return registry, nil
}

func (pr *PostRegistry) initializeDB() error {
	createAuthorsTableQuery := `CREATE TABLE IF NOT EXISTS authors (
		did TEXT PRIMARY KEY,
		handle TEXT NOT NULL
	)`
	_, err := pr.db.Exec(createAuthorsTableQuery)
	if err != nil {
		return err
	}

	createPostsTableQuery := `CREATE TABLE IF NOT EXISTS posts (
		id TEXT PRIMARY KEY,
		text TEXT NOT NULL,
		parent_post_id TEXT,
		root_post_id TEXT,
		author_did TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		has_embedded_media BOOLEAN NOT NULL,
		parent_relationship CHAR(3),
		FOREIGN KEY (author_did) REFERENCES authors(did)
	)`
	_, err = pr.db.Exec(createPostsTableQuery)
	return err
}

func (pr *PostRegistry) AddPost(ctx context.Context, post *Post) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddPost")
	defer span.End()
	insertQuery := `INSERT INTO posts (id, text, parent_post_id, root_post_id, author_did, created_at, has_embedded_media, parent_relationship)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	_, err := pr.db.Exec(insertQuery, post.ID, post.Text, post.ParentPostID, post.RootPostID, post.AuthorDID, post.CreatedAt, post.HasEmbeddedMedia, post.ParentRelationship)
	return err
}

func (pr *PostRegistry) AddAuthor(ctx context.Context, author *Author) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddAuthor")
	defer span.End()
	insertQuery := `INSERT INTO authors (did, handle) VALUES ($1, $2) ON CONFLICT (did) DO UPDATE SET handle = $2`
	_, err := pr.db.Exec(insertQuery, author.DID, author.Handle)
	return err
}

func (pr *PostRegistry) GetPost(ctx context.Context, postID string) (*Post, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPost")
	defer span.End()
	selectQuery := `SELECT id, text, parent_post_id, root_post_id, author_did, created_at, has_embedded_media, parent_relationship FROM posts WHERE id = $1`
	row := pr.db.QueryRow(selectQuery, postID)

	post := &Post{}
	err := row.Scan(&post.ID, &post.Text, &post.ParentPostID, &post.RootPostID, &post.AuthorDID, &post.CreatedAt, &post.HasEmbeddedMedia, &post.ParentRelationship)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("post not found")
		}
		return nil, err
	}

	return post, nil
}

func (pr *PostRegistry) GetAuthor(ctx context.Context, did string) (*Author, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAuthor")
	defer span.End()
	selectQuery := `SELECT did, handle FROM authors WHERE did = $1`
	row := pr.db.QueryRow(selectQuery, did)

	author := &Author{}
	err := row.Scan(&author.DID, &author.Handle)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("author not found")
		}
		return nil, err
	}

	return author, nil
}

func (pr *PostRegistry) GetAuthorsByHandle(ctx context.Context, handle string) ([]*Author, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAuthorsByHandle")
	defer span.End()
	selectQuery := `SELECT did, handle FROM authors WHERE handle = $1`
	rows, err := pr.db.Query(selectQuery, handle)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var authors []*Author
	for rows.Next() {
		author := &Author{}
		err := rows.Scan(&author.DID, &author.Handle)
		if err != nil {
			return nil, err
		}
		authors = append(authors, author)
	}

	return authors, nil
}

func (pr *PostRegistry) GetThreadView(ctx context.Context, postID, authorID string) ([]PostView, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetThreadView")
	defer span.End()

	query := `
WITH RECURSIVE post_tree AS (
    -- Base case: select root post
    SELECT id,
           text,
           parent_post_id,
           root_post_id,
           author_did,
           a2.handle,
           created_at,
           has_embedded_media,
           0 AS depth
    FROM posts
             LEFT JOIN authors a2 on a2.did = posts.author_did
    WHERE id = $1
      AND author_did = $2

    UNION ALL

    -- Recursive case: select child posts
    SELECT p.id,
           p.text,
           p.parent_post_id,
           p.root_post_id,
           p.author_did,
           a.handle,
           p.created_at,
           p.has_embedded_media,
           pt.depth + 1 AS depth
    FROM posts p
             JOIN
         post_tree pt ON p.parent_post_id = pt.id AND p.parent_relationship = 'r'
             LEFT JOIN authors a on p.author_did = a.did)

SELECT id,
       text,
       parent_post_id,
       root_post_id,
       author_did,
       handle,
       created_at,
       has_embedded_media,
       depth
FROM post_tree
ORDER BY depth;`

	rows, err := pr.db.QueryContext(ctx, query, postID, authorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var postViews []PostView
	for rows.Next() {
		var pv PostView
		err := rows.Scan(&pv.ID, &pv.Text, &pv.ParentPostID, &pv.RootPostID, &pv.AuthorDID, &pv.AuthorHandle, &pv.CreatedAt, &pv.HasEmbeddedMedia, &pv.Depth)
		if err != nil {
			return nil, err
		}
		postViews = append(postViews, pv)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(postViews) == 0 {
		return nil, NotFoundError{fmt.Errorf("post not found")}
	}

	return postViews, nil
}

func (pr *PostRegistry) Close() error {
	return pr.db.Close()
}
