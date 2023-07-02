package sentiment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/pemistahl/lingua-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	POSITIVE = "positive"
	NEGATIVE = "negative"
	NEUTRAL  = "neutral"
)

type Sentiment struct {
	SentimentServiceHost string
	LanguageDetector     lingua.LanguageDetector
	Client               *http.Client
}

type sentimentRequest struct {
	Posts []*search.Post `json:"posts"`
}

type sentimentDecision struct {
	Sentiment  string  `json:"sentiment"`
	Confidence float64 `json:"confidence_score"`
}

type sentimentPost struct {
	ID                 string            `json:"id"`
	Text               string            `json:"text"`
	ParentPostID       *string           `json:"parent_post_id"`
	RootPostID         *string           `json:"root_post_id"`
	AuthorDID          string            `json:"author_did"`
	CreatedAt          time.Time         `json:"created_at"`
	HasEmbeddedMedia   bool              `json:"has_embedded_media"`
	ParentRelationship *string           `json:"parent_relationship"` // null, "r", "q"
	Decision           sentimentDecision `json:"decision"`
}

type sentimentResponse struct {
	Posts []sentimentPost `json:"posts"`
}

func NewSentiment(sentimentServiceHost string) *Sentiment {

	// Build an English langauge detector since our sentiment service only supports English
	languages := []lingua.Language{
		lingua.English,
		lingua.Spanish,
		lingua.Portuguese,
		lingua.Korean,
		lingua.Japanese,
		lingua.Persian,
	}

	client := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	detector := lingua.NewLanguageDetectorBuilder().
		FromLanguages(languages...).
		Build()

	return &Sentiment{
		SentimentServiceHost: sentimentServiceHost,
		LanguageDetector:     detector,
		Client:               &client,
	}
}

func (s *Sentiment) GetPostsSentiment(ctx context.Context, posts []*search.Post) ([]*search.Post, error) {
	tracer := otel.Tracer("bsky-search")
	ctx, span := tracer.Start(ctx, "Sentiment:GetPostsSentiment")
	defer span.End()

	nonEnglishPosts := make([]*search.Post, 0)

	// Return early if all of the posts are not in English
	for _, post := range posts {
		confidence := s.LanguageDetector.ComputeLanguageConfidence(post.Text, lingua.English)
		if confidence < 0.5 {
			span.SetAttributes(attribute.String("language", "non-english"))
			span.SetAttributes(attribute.Float64("language.confidence", confidence))
			span.SetAttributes(attribute.Bool("skipping_sentiment", true))
			nonEnglishPosts = append(nonEnglishPosts, post)
		}
	}

	if len(nonEnglishPosts) >= len(posts) {
		return posts, nil
	}

	url := fmt.Sprintf("%s/analyze_sentiment", s.SentimentServiceHost)

	reqBody := sentimentRequest{Posts: posts}
	jsonReqBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal sentiment request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonReqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create sentiment request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send sentiment request: %w", err)
	}
	defer resp.Body.Close()

	var respBody sentimentResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return nil, fmt.Errorf("failed to decode sentiment response body: %w", err)
	}

	for i, post := range respBody.Posts {
		if post.Decision.Sentiment == POSITIVE {
			sentiment := strings.Clone(search.PositiveSentiment)
			posts[i].Sentiment = &sentiment
		} else if post.Decision.Sentiment == NEGATIVE {
			sentiment := strings.Clone(search.NegativeSentiment)
			posts[i].Sentiment = &sentiment
		} else if post.Decision.Sentiment == NEUTRAL {
			sentiment := strings.Clone(search.NeutralSentiment)
			posts[i].Sentiment = &sentiment
		} else {
			log.Printf("unknown sentiment: %s\n", post.Decision.Sentiment)
		}
		var confidence float64
		confidence = post.Decision.Confidence
		posts[i].SentimentConfidence = &confidence
	}

	return posts, nil
}
