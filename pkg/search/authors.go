package search

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // postgres driver

	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	"go.opentelemetry.io/otel"
)

type Author struct {
	DID           string `json:"did"`
	Handle        string `json:"handle"`
	ClusterOptOut bool   `json:"cluster_opt_out"`
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

type AuthorBlock struct {
	ActorDID  string    `json:"author_did"`
	TargetDID string    `json:"target_did"`
	CreatedAt time.Time `json:"created_at"`
}

func (pr *PostRegistry) AddAuthor(ctx context.Context, author *Author) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddAuthor")
	defer span.End()

	err := pr.queries.AddAuthor(ctx, search_queries.AddAuthorParams{
		Did:    author.DID,
		Handle: author.Handle,
	})
	return err
}

func (pr *PostRegistry) GetAuthor(ctx context.Context, did string) (*Author, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAuthor")
	defer span.End()
	author, err := pr.queries.GetAuthor(ctx, did)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("author not found")}
		}
		return nil, err
	}
	return &Author{DID: author.Did, Handle: author.Handle, ClusterOptOut: author.ClusterOptOut}, nil
}

func (pr *PostRegistry) GetOptedOutAuthors(ctx context.Context) ([]*Author, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetOptedOutAuthors")
	defer span.End()
	authors, err := pr.queries.GetOptedOutAuthors(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("authors not found")}
		}
		return nil, err
	}

	retAuthors := make([]*Author, len(authors))
	for i, author := range authors {
		retAuthors[i] = &Author{DID: author.Did, Handle: author.Handle, ClusterOptOut: author.ClusterOptOut}
	}

	return retAuthors, nil
}

func (pr *PostRegistry) UpdateAuthorOptOut(ctx context.Context, did string, optOut bool) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:UpdateAuthorOptOut")
	defer span.End()

	err := pr.queries.UpdateAuthorOptOut(ctx, search_queries.UpdateAuthorOptOutParams{
		Did:           did,
		ClusterOptOut: optOut,
	})
	if err != nil {
		return err
	}
	return nil
}

func (pr *PostRegistry) GetAuthorsByHandle(ctx context.Context, handle string) ([]*Author, error) {
	tracer := otel.Tracer("post-registry")
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

func (pr *PostRegistry) GetAuthorStats(ctx context.Context) (*AuthorStats, error) {
	tracer := otel.Tracer("post-registry")
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
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetTopPosters")
	defer span.End()

	topPosters, err := pr.queries.GetTopPosters(ctx, count)
	if err != nil {
		return nil, fmt.Errorf("failed to get top posters: %w", err)
	}

	return topPosters, nil
}

func (pr *PostRegistry) AddAuthorBlock(ctx context.Context, authorDID string, targetDID string, createdAt time.Time) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddAuthorBlock")
	defer span.End()

	err := pr.queries.AddAuthorBlock(ctx, search_queries.AddAuthorBlockParams{
		ActorDid:  authorDID,
		TargetDid: targetDID,
		CreatedAt: createdAt,
	})
	return err
}

func (pr *PostRegistry) RemoveBlock(ctx context.Context, actorDID string, targetDID string) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:RemoveBlock")
	defer span.End()

	err := pr.queries.RemoveAuthorBlock(ctx, search_queries.RemoveAuthorBlockParams{
		ActorDid:  actorDID,
		TargetDid: targetDID,
	})
	return err
}

func (pr *PostRegistry) GetBlockedByCount(ctx context.Context, targetDID string) (int64, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetBlockedByCount")
	defer span.End()

	count, err := pr.queries.GetBlockedByCountForTarget(ctx, targetDID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, NotFoundError{fmt.Errorf("target not found")}
		}
		return 0, err
	}

	return count, nil
}

func (pr *PostRegistry) GetBlocksPageForTarget(ctx context.Context, targetDID string, limit int32, offset int32) ([]*AuthorBlock, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetBlocksPageForTarget")
	defer span.End()

	blocks, err := pr.queries.GetBlocksForTarget(ctx, search_queries.GetBlocksForTargetParams{
		TargetDid: targetDID,
		Limit:     limit,
		Offset:    offset,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("blocks not found")}
		}
		return nil, err
	}

	retBlocks := make([]*AuthorBlock, len(blocks))
	for i, block := range blocks {
		retBlocks[i] = &AuthorBlock{
			ActorDID:  block.ActorDid,
			TargetDID: block.TargetDid,
			CreatedAt: block.CreatedAt,
		}
	}

	return retBlocks, nil
}
