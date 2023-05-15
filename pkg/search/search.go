package search

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	_ "github.com/lib/pq" // postgres driver
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
)

type NotFoundError struct {
	error
}

// Post relationships
const (
	ReplyRelationship = "r"
	QuoteRelationship = "q"
	PositiveSentiment = "p"
	NegativeSentiment = "n"
	NeutralSentiment  = "u"
)

type Post struct {
	ID                  string    `json:"id"`
	Text                string    `json:"text"`
	ParentPostID        *string   `json:"parent_post_id"`
	RootPostID          *string   `json:"root_post_id"`
	AuthorDID           string    `json:"author_did"`
	CreatedAt           time.Time `json:"created_at"`
	HasEmbeddedMedia    bool      `json:"has_embedded_media"`
	ParentRelationship  *string   `json:"parent_relationship"` // null, "r", "q"
	Sentiment           *string   `json:"sentiment"`
	SentimentConfidence *float64  `json:"sentiment_confidence"`
}

type Percentile struct {
	Percentile float64 `json:"percentile"`
	Count      int64   `json:"count"`
}

type Bracket struct {
	Min   int   `json:"min"`
	Count int64 `json:"count"`
}

type AuthorStats struct {
	TotalAuthors  int64        `json:"total_authors"`
	MeanPostCount float64      `json:"mean_post_count"`
	Percentiles   []Percentile `json:"percentiles"`
	Brackets      []Bracket    `json:"brackets"`
	UpdatedAt     time.Time    `json:"updated_at"`
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

type PostgresRegistry struct {
	db      *sql.DB
	queries *search_queries.Queries
}

type PostRegistry interface {
	initializeDB() error
	AddPost(ctx context.Context, post *Post) error
	AddAuthor(ctx context.Context, author *Author) error
	GetPost(ctx context.Context, postID string) (*Post, error)
	GetAuthorStats(ctx context.Context) (*AuthorStats, error)
	GetTopPosters(ctx context.Context, count int32) ([]search_queries.GetTopPostersRow, error)
	GetAuthor(ctx context.Context, did string) (*Author, error)
	GetAuthorsByHandle(ctx context.Context, handle string) ([]*Author, error)
	GetThreadView(ctx context.Context, postID, authorID string) ([]PostView, error)
	GetOldestPresentParent(ctx context.Context, postID string) (*Post, error)
	Close() error
}

func NewPostRegistry(connectionString string) (PostRegistry, error) {
	if strings.HasPrefix(connectionString, "postgres://") {
		return newPostgresRegistry(connectionString)
	}
	if strings.HasPrefix(connectionString, "neo4j") || strings.HasPrefix(connectionString, "bolt") {
		return newNeo4jRegistry(connectionString)
	}
	return nil, fmt.Errorf("unknown connection: %s", connectionString)
}

func newPostgresRegistry(connectionString string) (PostRegistry, error) {
	var db *sql.DB
	var err error

	for i := 0; i < 5; i++ {
		db, err = otelsql.Open(
			"postgres",
			connectionString,
			otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
		)
		if err != nil {
			return nil, err
		}

		err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
			semconv.DBSystemPostgreSQL,
		))
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

	registry := &PostgresRegistry{
		db:      db,
		queries: queries,
	}

	err = registry.initializeDB()
	if err != nil {
		return nil, err
	}

	return registry, nil
}

func (pr *PostgresRegistry) initializeDB() error {
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
		sentiment CHAR(3),
		sentiment_confidence FLOAT,
		FOREIGN KEY (author_did) REFERENCES authors(did)
	)`
	_, err = pr.db.Exec(createPostsTableQuery)
	return err
}

func (pr *PostgresRegistry) AddPost(ctx context.Context, post *Post) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddPost")
	defer span.End()

	parentPostID := sql.NullString{
		String: "",
		Valid:  false,
	}
	if post.ParentPostID != nil {
		parentPostID.String = *post.ParentPostID
		parentPostID.Valid = true
	}

	rootPostID := sql.NullString{
		String: "",
		Valid:  false,
	}
	if post.RootPostID != nil {
		rootPostID.String = *post.RootPostID
		rootPostID.Valid = true
	}

	parentRelationship := sql.NullString{
		String: "",
		Valid:  false,
	}
	if post.ParentRelationship != nil {
		parentRelationship.String = *post.ParentRelationship
		parentRelationship.Valid = true
	}

	sentiment := sql.NullString{
		String: "",
		Valid:  false,
	}
	if post.Sentiment != nil {
		sentiment.String = *post.Sentiment
		sentiment.Valid = true
	}

	sentimentConfidence := sql.NullFloat64{
		Float64: 0,
		Valid:   false,
	}
	if post.SentimentConfidence != nil {
		sentimentConfidence.Float64 = *post.SentimentConfidence
		sentimentConfidence.Valid = true
	}

	err := pr.queries.AddPost(ctx, search_queries.AddPostParams{
		ID:                  post.ID,
		Text:                post.Text,
		ParentPostID:        parentPostID,
		RootPostID:          rootPostID,
		AuthorDid:           post.AuthorDID,
		CreatedAt:           post.CreatedAt,
		HasEmbeddedMedia:    post.HasEmbeddedMedia,
		ParentRelationship:  parentRelationship,
		Sentiment:           sentiment,
		SentimentConfidence: sentimentConfidence,
	})
	return err
}

func (pr *PostgresRegistry) AddAuthor(ctx context.Context, author *Author) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddAuthor")
	defer span.End()

	err := pr.queries.AddAuthor(ctx, search_queries.AddAuthorParams{
		Did:    author.DID,
		Handle: author.Handle,
	})
	return err
}

func (pr *PostgresRegistry) GetPost(ctx context.Context, postID string) (*Post, error) {
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

func (pr *PostgresRegistry) GetAuthorStats(ctx context.Context) (*AuthorStats, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAuthorStats")
	defer span.End()
	authorStats, err := pr.queries.GetAuthorStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get author stats: %w", err)
	}

	return &AuthorStats{
		TotalAuthors: authorStats.Total,
		// Parse mean as a float64 from string
		MeanPostCount: authorStats.Mean,
		Percentiles: []Percentile{
			{
				Percentile: 0.25,
				Count:      authorStats.P25,
			},
			{
				Percentile: 0.50,
				Count:      authorStats.P50,
			},
			{
				Percentile: 0.75,
				Count:      authorStats.P75,
			},
			{
				Percentile: 0.90,
				Count:      authorStats.P90,
			},
			{
				Percentile: 0.95,
				Count:      authorStats.P95,
			},
			{
				Percentile: 0.99,
				Count:      authorStats.P99,
			},
		},
		Brackets: []Bracket{
			{
				Min:   1,
				Count: authorStats.Gt1,
			},
			{
				Min:   5,
				Count: authorStats.Gt5,
			},
			{
				Min:   10,
				Count: authorStats.Gt10,
			},
			{
				Min:   20,
				Count: authorStats.Gt20,
			},
			{
				Min:   100,
				Count: authorStats.Gt100,
			},
			{
				Min:   1000,
				Count: authorStats.Gt1000,
			},
		},
		UpdatedAt: time.Now(),
	}, nil
}

func (pr *PostgresRegistry) GetTopPosters(ctx context.Context, count int32) ([]search_queries.GetTopPostersRow, error) {
	tracer := otel.Tracer("search")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetTopPosters")
	defer span.End()

	topPosters, err := pr.queries.GetTopPosters(ctx, count)
	if err != nil {
		return nil, fmt.Errorf("failed to get top posters: %w", err)
	}

	return topPosters, nil
}

func (pr *PostgresRegistry) GetAuthor(ctx context.Context, did string) (*Author, error) {
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

func (pr *PostgresRegistry) GetAuthorsByHandle(ctx context.Context, handle string) ([]*Author, error) {
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

func (pr *PostgresRegistry) GetThreadView(ctx context.Context, postID, authorID string) ([]PostView, error) {
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

		var sentimentPtr *string
		if threadView.Sentiment.Valid {
			sentiment := fmt.Sprintf("%s", threadView.Sentiment.String)
			sentimentPtr = &sentiment
		}

		var sentimentConfidencePtr *float64
		if threadView.SentimentConfidence.Valid {
			sentimentConfidence := threadView.SentimentConfidence.Float64
			sentimentConfidencePtr = &sentimentConfidence
		}

		retThreadViews[i] = PostView{
			Post: Post{
				ID:                  threadView.ID,
				Text:                threadView.Text,
				ParentPostID:        parentPostIDPtr,
				RootPostID:          rootPostIDPtr,
				AuthorDID:           threadView.AuthorDid,
				CreatedAt:           threadView.CreatedAt,
				HasEmbeddedMedia:    threadView.HasEmbeddedMedia,
				Sentiment:           sentimentPtr,
				SentimentConfidence: sentimentConfidencePtr,
			},
			AuthorHandle: threadView.Handle.String,
			Depth:        int(threadView.Depth.(int64)),
		}
	}

	return retThreadViews, nil
}

func (pr *PostgresRegistry) GetOldestPresentParent(ctx context.Context, postID string) (*Post, error) {
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

	var sentiment *string
	if postView.Sentiment.Valid {
		sentiment = &postView.Sentiment.String
	}

	var sentimentConfidence *float64
	if postView.SentimentConfidence.Valid {
		sentimentConfidence = &postView.SentimentConfidence.Float64
	}

	return &Post{
		ID:                  postView.ID,
		Text:                postView.Text,
		ParentPostID:        parentPostIDPtr,
		RootPostID:          rootPostIDPtr,
		AuthorDID:           postView.AuthorDid,
		CreatedAt:           postView.CreatedAt,
		HasEmbeddedMedia:    postView.HasEmbeddedMedia,
		ParentRelationship:  parentRelationshipPtr,
		Sentiment:           sentiment,
		SentimentConfidence: sentimentConfidence,
	}, nil
}

func (pr *PostgresRegistry) Close() error {
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

	var sentiment *string
	if p.Sentiment.Valid {
		sentiment = &p.Sentiment.String
	}

	var sentimentConfidence *float64
	if p.SentimentConfidence.Valid {
		sentimentConfidence = &p.SentimentConfidence.Float64
	}

	return &Post{
		ID:                  p.ID,
		Text:                p.Text,
		ParentPostID:        parentPostIDPtr,
		RootPostID:          rootPostIDPtr,
		AuthorDID:           p.AuthorDid,
		CreatedAt:           p.CreatedAt,
		HasEmbeddedMedia:    p.HasEmbeddedMedia,
		ParentRelationship:  parentRelationshipPtr,
		Sentiment:           sentiment,
		SentimentConfidence: sentimentConfidence,
	}
}
