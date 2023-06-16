package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // postgres driver

	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	"go.opentelemetry.io/otel"
)

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
	Hotness             *float64  `json:"hotness,omitempty"`
	Labels              []string  `json:"labels,omitempty"`
}

type PostView struct {
	Post         `json:"post"`
	AuthorHandle string `json:"author_handle"`
	Depth        int    `json:"depth"`
}

func (pr *PostRegistry) AddPost(ctx context.Context, post *Post) error {
	tracer := otel.Tracer("post-registry")
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

func (pr *PostRegistry) GetPost(ctx context.Context, postID string) (*Post, error) {
	tracer := otel.Tracer("post-registry")
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

func (pr *PostRegistry) AddLikeToPost(ctx context.Context, postID string) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddLikeToPost")
	defer span.End()

	err := pr.queries.AddLikeToPost(ctx, postID)
	return err
}

func (pr *PostRegistry) RemoveLikeFromPost(ctx context.Context, postID string) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:RemoveLikeFromPost")
	defer span.End()

	err := pr.queries.RemoveLikeFromPost(ctx, postID)
	return err
}

func (pr *PostRegistry) GetThreadView(ctx context.Context, postID, authorID string) ([]PostView, error) {
	tracer := otel.Tracer("post-registry")
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
	tracer := otel.Tracer("post-registry")
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

func (pr *PostRegistry) GetPostPage(ctx context.Context, limit int32, offset int32) ([]Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostPage")
	defer span.End()

	posts, err := pr.queries.GetPostPage(ctx, search_queries.GetPostPageParams{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		return nil, err
	}

	retPosts := []Post{}
	for _, post := range posts {
		newPost, err := postFromPagePost(post)
		if err != nil {
			return nil, err
		}
		if newPost == nil {
			continue
		}
		retPosts = append(retPosts, *newPost)
	}

	if len(retPosts) == 0 {
		return nil, NotFoundError{fmt.Errorf("no posts found")}
	}

	return retPosts, nil
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

func postFromPagePost(p search_queries.GetPostPageRow) (*Post, error) {
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

	var labels []string
	switch v := p.Labels.(type) {
	case []byte:
		// Convert labels from an array of strings to a slice of strings
		if err := json.Unmarshal(v, &labels); err != nil {
			return nil, fmt.Errorf("failed to unmarshal labels: %v", err)
		}
	default:
		return nil, fmt.Errorf("unexpected type for labels: %T", v)
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
		Labels:              labels,
	}, nil
}
