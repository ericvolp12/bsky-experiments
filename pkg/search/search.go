package search

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
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
	db      *sql.DB
	queries *search_queries.Queries
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

	queries := search_queries.New(db)

	registry := &PostRegistry{
		db:      db,
		queries: queries,
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
	post, err := pr.queries.GetPost(ctx, postID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("post not found")}
		}
		return nil, err
	}

	return postFromQueryPost(post), nil
}

func (pr *PostRegistry) GetAuthor(ctx context.Context, did string) (*Author, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAuthor")
	defer span.End()
	author, err := pr.queries.GetAuthor(ctx, did)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("author not found")}
		}
		return nil, err
	}
	return &Author{DID: author.Did, Handle: author.Handle}, nil
}

func (pr *PostRegistry) GetAuthorsByHandle(ctx context.Context, handle string) ([]*Author, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAuthorsByHandle")
	defer span.End()

	authors, err := pr.queries.GetAuthorsByHandle(ctx, handle)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("authors not found")}
		}
		return nil, err
	}

	retAuthors := make([]*Author, len(authors))
	for i, author := range authors {
		retAuthors[i] = &Author{DID: author.Did, Handle: author.Handle}
	}

	return retAuthors, nil
}

func (pr *PostRegistry) GetThreadView(ctx context.Context, postID, authorID string) ([]PostView, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetThreadView")
	defer span.End()
	threadViews, err := pr.queries.GetThreadView(ctx, search_queries.GetThreadViewParams{ID: postID, AuthorDid: authorID})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("thread not found")}
		}
		return nil, err
	}

	retThreadViews := make([]PostView, len(threadViews))
	for i, threadView := range threadViews {
		var parentPostIDPtr *string
		if threadView.ParentPostID.Valid {
			parentPostID := fmt.Sprintf("%s", threadView.ParentPostID.String)
			parentPostIDPtr = &parentPostID
		}

		var rootPostIDPtr *string
		if threadView.RootPostID.Valid {
			rootPostID := fmt.Sprintf("%s", threadView.RootPostID.String)
			rootPostIDPtr = &rootPostID
		}

		retThreadViews[i] = PostView{
			Post: Post{
				ID:               threadView.ID,
				Text:             threadView.Text,
				ParentPostID:     parentPostIDPtr,
				RootPostID:       rootPostIDPtr,
				AuthorDID:        threadView.AuthorDid,
				CreatedAt:        threadView.CreatedAt,
				HasEmbeddedMedia: threadView.HasEmbeddedMedia,
			},
			AuthorHandle: threadView.Handle.String,
			Depth:        int(threadView.Depth.(int64)),
		}
	}

	return retThreadViews, nil
}

func (pr *PostRegistry) GetOldestPresentParent(ctx context.Context, postID string) (*Post, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetOldestPresentParent")
	defer span.End()
	postView, err := pr.queries.GetOldestPresentParent(ctx, postID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("post not found")}
		}
		return nil, err
	}

	var parentPostIDPtr *string
	if postView.ParentPostID.Valid {
		parentPostIDPtr = &postView.ParentPostID.String
	}

	var rootPostIDPtr *string
	if postView.RootPostID.Valid {
		rootPostIDPtr = &postView.RootPostID.String
	}

	var parentRelationshipPtr *string
	if postView.ParentRelationship.Valid {
		parentRelationshipPtr = &postView.ParentRelationship.String
	}

	return &Post{
		ID:                 postView.ID,
		Text:               postView.Text,
		ParentPostID:       parentPostIDPtr,
		RootPostID:         rootPostIDPtr,
		AuthorDID:          postView.AuthorDid,
		CreatedAt:          postView.CreatedAt,
		HasEmbeddedMedia:   postView.HasEmbeddedMedia,
		ParentRelationship: parentRelationshipPtr,
	}, nil
}

func (pr *PostRegistry) Close() error {
	return pr.db.Close()
}

// postFromQueryPost turns a queries.Post into a search.Post
func postFromQueryPost(p search_queries.Post) *Post {
	var parentPostIDPtr *string
	if p.ParentPostID.Valid {
		parentPostIDPtr = &p.ParentPostID.String
	}

	var rootPostIDPtr *string
	if p.RootPostID.Valid {
		rootPostIDPtr = &p.RootPostID.String
	}

	var parentRelationshipPtr *string
	if p.ParentRelationship.Valid {
		parentRelationshipPtr = &p.ParentRelationship.String
	}

	return &Post{
		ID:                 p.ID,
		Text:               p.Text,
		ParentPostID:       parentPostIDPtr,
		RootPostID:         rootPostIDPtr,
		AuthorDID:          p.AuthorDid,
		CreatedAt:          p.CreatedAt,
		HasEmbeddedMedia:   p.HasEmbeddedMedia,
		ParentRelationship: parentRelationshipPtr,
	}
}
