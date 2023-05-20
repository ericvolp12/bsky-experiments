package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	_ "github.com/lib/pq" // postgres driver
	"github.com/tabbed/pqtype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	Images              []*Image  `json:"images,omitempty"`
}

type Image struct {
	CID          string           `json:"cid"`
	PostID       string           `json:"post_id"`
	AuthorDID    string           `json:"author_did"`
	AltText      *string          `json:"alt_text"`
	MimeType     string           `json:"mime_type"`
	FullsizeURL  string           `json:"fullsize_url"`
	ThumbnailURL string           `json:"thumbnail_url"`
	CreatedAt    time.Time        `json:"created_at"`
	CVCompleted  bool             `json:"cv_completed"`
	CVRunAt      *time.Time       `json:"cv_run_at"`
	CVClasses    *json.RawMessage `json:"cv_classes"`
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
	TotalAuthors    int64        `json:"total_authors"`
	TotalPosts      int64        `json:"total_posts"`
	HellthreadPosts int64        `json:"hellthread_posts"`
	MeanPostCount   float64      `json:"mean_post_count"`
	Percentiles     []Percentile `json:"percentiles"`
	Brackets        []Bracket    `json:"brackets"`
	UpdatedAt       time.Time    `json:"updated_at"`
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
		sentiment CHAR(3),
		sentiment_confidence FLOAT,
		FOREIGN KEY (author_did) REFERENCES authors(did)
	)`
	_, err = pr.db.Exec(createPostsTableQuery)
	return err
}

func (pr *PostRegistry) AddPost(ctx context.Context, post *Post) error {
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

func (pr *PostRegistry) AddImage(ctx context.Context, image *Image) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddImage")
	defer span.End()

	altText := sql.NullString{
		String: "",
		Valid:  false,
	}

	if image.AltText != nil {
		altText.String = *image.AltText
		altText.Valid = true
	}

	cvRunAt := sql.NullTime{
		Time:  time.Time{},
		Valid: false,
	}

	if image.CVRunAt != nil {
		cvRunAt.Time = *image.CVRunAt
		cvRunAt.Valid = true
	}

	cvClasses := pqtype.NullRawMessage{
		RawMessage: nil,
		Valid:      false,
	}

	if image.CVClasses != nil {
		cvClasses.RawMessage = *image.CVClasses
		cvClasses.Valid = true
	}

	err := pr.queries.AddImage(ctx, search_queries.AddImageParams{
		Cid:          image.CID,
		PostID:       image.PostID,
		AuthorDid:    image.AuthorDID,
		AltText:      altText,
		MimeType:     image.MimeType,
		FullsizeUrl:  image.FullsizeURL,
		ThumbnailUrl: image.ThumbnailURL,
		CreatedAt:    image.CreatedAt,
		CvCompleted:  image.CVCompleted,
		CvRunAt:      cvRunAt,
		CvClasses:    cvClasses,
	})

	return err
}

func (pr *PostRegistry) AddCVDataToImage(ctx context.Context, cid string, postID string, cvRunAt time.Time, cvClasses json.RawMessage) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddCVDataToImage")
	defer span.End()

	image, err := pr.queries.GetImage(ctx, search_queries.GetImageParams{
		Cid:    cid,
		PostID: postID,
	})
	if err != nil {
		return fmt.Errorf("failed to get image for update: %w", err)
	}

	cvClassesNullRawMessage := pqtype.NullRawMessage{
		RawMessage: cvClasses,
		Valid:      true,
	}

	cvRunAtNullTime := sql.NullTime{
		Time:  cvRunAt,
		Valid: true,
	}

	err = pr.queries.UpdateImage(ctx, search_queries.UpdateImageParams{
		Cid:          image.Cid,
		PostID:       image.PostID,
		AuthorDid:    image.AuthorDid,
		AltText:      image.AltText,
		MimeType:     image.MimeType,
		FullsizeUrl:  image.FullsizeUrl,
		ThumbnailUrl: image.ThumbnailUrl,
		CreatedAt:    image.CreatedAt,
		CvCompleted:  true,
		CvRunAt:      cvRunAtNullTime,
		CvClasses:    cvClassesNullRawMessage,
	})

	return err
}

func (pr *PostRegistry) GetImage(ctx context.Context, imageCID string, postID string) (*Image, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetImage")
	defer span.End()

	image, err := pr.queries.GetImage(ctx, search_queries.GetImageParams{
		Cid:    imageCID,
		PostID: postID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("image not found")}
		}
		return nil, err
	}

	var altTextPtr *string
	if image.AltText.Valid {
		altText := image.AltText.String
		altTextPtr = &altText
	}

	var cvRunAtPtr *time.Time
	if image.CvRunAt.Valid {
		cvRunAt := image.CvRunAt.Time
		cvRunAtPtr = &cvRunAt
	}

	var cvClassesPtr *json.RawMessage
	if image.CvClasses.Valid {
		cvClasses := image.CvClasses.RawMessage
		cvClassesPtr = &cvClasses
	}

	return &Image{
		CID:          image.Cid,
		PostID:       image.PostID,
		AuthorDID:    image.AuthorDid,
		AltText:      altTextPtr,
		MimeType:     image.MimeType,
		FullsizeURL:  image.FullsizeUrl,
		ThumbnailURL: image.ThumbnailUrl,
		CreatedAt:    image.CreatedAt,
		CVCompleted:  image.CvCompleted,
		CVRunAt:      cvRunAtPtr,
		CVClasses:    cvClassesPtr,
	}, nil
}

func (pr *PostRegistry) GetImagesForPost(ctx context.Context, postID string) ([]*Image, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetImagesForPost")
	defer span.End()

	span.SetAttributes(attribute.String("post_id", postID))

	images, err := pr.queries.GetImagesForPost(ctx, postID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("images not found")}
		}
		return nil, err
	}

	retImages := make([]*Image, len(images))
	for i, image := range images {
		var altTextPtr *string
		if image.AltText.Valid {
			altText := image.AltText.String
			altTextPtr = &altText
		}

		var cvRunAtPtr *time.Time
		if image.CvRunAt.Valid {
			cvRunAt := image.CvRunAt.Time
			cvRunAtPtr = &cvRunAt
		}

		var cvClassesPtr *json.RawMessage
		if image.CvClasses.Valid {
			cvClasses := image.CvClasses.RawMessage
			cvClassesPtr = &cvClasses
		}

		retImages[i] = &Image{
			CID:          image.Cid,
			PostID:       image.PostID,
			AuthorDID:    image.AuthorDid,
			AltText:      altTextPtr,
			MimeType:     image.MimeType,
			FullsizeURL:  image.FullsizeUrl,
			ThumbnailURL: image.ThumbnailUrl,
			CreatedAt:    image.CreatedAt,
			CVCompleted:  image.CvCompleted,
			CVRunAt:      cvRunAtPtr,
			CVClasses:    cvClassesPtr,
		}
	}

	return retImages, nil
}

func (pr *PostRegistry) GetUnprocessedImages(ctx context.Context, limit int32) ([]*Image, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetUnprocessedImages")
	defer span.End()

	images, err := pr.queries.GetUnprocessedImages(ctx, limit)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("images not found")}
		}
		return nil, err
	}

	retImages := make([]*Image, len(images))
	for i, image := range images {
		var altTextPtr *string
		if image.AltText.Valid {
			altText := image.AltText.String
			altTextPtr = &altText
		}

		var cvRunAtPtr *time.Time
		if image.CvRunAt.Valid {
			cvRunAt := image.CvRunAt.Time
			cvRunAtPtr = &cvRunAt
		}

		var cvClassesPtr *json.RawMessage
		if image.CvClasses.Valid {
			cvClasses := image.CvClasses.RawMessage
			cvClassesPtr = &cvClasses
		}

		retImages[i] = &Image{
			CID:          image.Cid,
			PostID:       image.PostID,
			AuthorDID:    image.AuthorDid,
			AltText:      altTextPtr,
			MimeType:     image.MimeType,
			FullsizeURL:  image.FullsizeUrl,
			ThumbnailURL: image.ThumbnailUrl,
			CreatedAt:    image.CreatedAt,
			CVCompleted:  image.CvCompleted,
			CVRunAt:      cvRunAtPtr,
			CVClasses:    cvClassesPtr,
		}
	}

	return retImages, nil
}

func (pr *PostRegistry) AddAuthor(ctx context.Context, author *Author) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddAuthor")
	defer span.End()

	err := pr.queries.AddAuthor(ctx, search_queries.AddAuthorParams{
		Did:    author.DID,
		Handle: author.Handle,
	})
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

	enrichedPost, err := postFromQueryPost(post)

	return enrichedPost, err
}

func (pr *PostRegistry) GetAuthorStats(ctx context.Context) (*AuthorStats, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAuthorStats")
	defer span.End()
	authorStats, err := pr.queries.GetAuthorStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get author stats: %w", err)
	}

	return &AuthorStats{
		TotalAuthors:    authorStats.TotalAuthors,
		TotalPosts:      authorStats.TotalPosts,
		HellthreadPosts: authorStats.HellthreadPostCount,
		// Parse mean as a float64 from string
		MeanPostCount: authorStats.MeanPostsPerAuthor,
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

func (pr *PostRegistry) GetTopPosters(ctx context.Context, count int32) ([]search_queries.GetTopPostersRow, error) {
	tracer := otel.Tracer("search")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetTopPosters")
	defer span.End()

	topPosters, err := pr.queries.GetTopPosters(ctx, count)
	if err != nil {
		return nil, fmt.Errorf("failed to get top posters: %w", err)
	}

	return topPosters, nil
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

func (pr *PostRegistry) Close() error {
	return pr.db.Close()
}

// postFromQueryPost turns a queries.Post into a search.Post
func postFromQueryPost(p search_queries.GetPostRow) (*Post, error) {
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

	var images []*Image
	switch v := p.Images.(type) {
	case []byte:
		// Try to unmarshal if it's a slice of bytes
		var imagesData []map[string]interface{}
		if err := json.Unmarshal(v, &imagesData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal images: %v", err)
		}
		// If unmarshaling is successful, loop through the data and convert each item to an Image
		for _, data := range imagesData {
			imageData, _ := json.Marshal(data)
			var image Image
			if err := json.Unmarshal(imageData, &image); err != nil {
				log.Printf("failed to convert data to image: %v", err)
				continue
			}
			images = append(images, &image)
		}

	default:
		return nil, fmt.Errorf("unexpected type for images: %T", v)
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
		Images:              images,
	}, nil
}
