package sentiment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pemistahl/lingua-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
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
	Posts []*SentimentPost `json:"posts"`
}

type sentimentDecision struct {
	Sentiment  string  `json:"sentiment"`
	Confidence float64 `json:"confidence_score"`
}

type SentimentPost struct {
	ActorDID string             `json:"actor_did"`
	Rkey     string             `json:"rkey"`
	Text     string             `json:"text"`
	Decision *sentimentDecision `json:"decision"`
	Langs    []string           `json:"langs"`
}

type sentimentResponse struct {
	Posts []SentimentPost `json:"posts"`
}

var tracer = otel.Tracer("sentiment")

var languagePostCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "sentiment_language_post_count",
	Help: "The number of posts in a given language",
}, []string{"lang"})

func NewSentiment(sentimentServiceHost string) *Sentiment {
	client := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	detector := lingua.NewLanguageDetectorBuilder().
		FromAllLanguages().
		Build()

	return &Sentiment{
		SentimentServiceHost: sentimentServiceHost,
		LanguageDetector:     detector,
		Client:               &client,
	}
}

func (s *Sentiment) GetPostsSentiment(ctx context.Context, posts []*SentimentPost) ([]*SentimentPost, error) {
	ctx, span := tracer.Start(ctx, "GetPostsSentiment")
	defer span.End()

	englishPosts := make([]*SentimentPost, 0, len(posts))

	// Return early if all of the posts are not in English
	for i, post := range posts {
		// Record the likely language of each post
		lang, ok := s.LanguageDetector.DetectLanguageOf(post.Text)
		if ok {
			posts[i].Langs = append(posts[i].Langs, strings.ToLower(lang.IsoCode639_1().String()))
			languagePostCount.WithLabelValues(lang.String()).Inc()
		} else {
			languagePostCount.WithLabelValues("unknown").Inc()
		}
		confidence := s.LanguageDetector.ComputeLanguageConfidence(post.Text, lingua.English)
		if confidence > 0.5 {
			englishPosts = append(englishPosts, posts[i])
		}
	}

	if len(englishPosts) == 0 {
		return posts, nil
	}

	url := fmt.Sprintf("%s/analyze_sentiment", s.SentimentServiceHost)

	reqBody := sentimentRequest{Posts: englishPosts}
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

	// Merge the english posts back into the original posts slice
	for i, post := range posts {
		for j, englishPost := range respBody.Posts {
			if post.Rkey == englishPost.Rkey && post.ActorDID == englishPost.ActorDID {
				posts[i].Decision = respBody.Posts[j].Decision
			}
		}
	}

	return posts, nil
}
