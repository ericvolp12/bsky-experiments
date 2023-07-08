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

type Cluster struct {
	ID          int32  `json:"id"`
	LookupAlias string `json:"lookup_alias"`
	Name        string `json:"name"`
}

func (pr *PostRegistry) AssignAuthorToCluster(ctx context.Context, authorDID string, clusterID int32) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AssignAuthorToCluster")
	defer span.End()

	err := pr.queries.AddAuthorToCluster(ctx, search_queries.AddAuthorToClusterParams{
		AuthorDid: authorDID,
		ClusterID: clusterID,
	})

	return err
}

func (pr *PostRegistry) GetClusters(ctx context.Context) ([]*Cluster, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetClusters")
	defer span.End()

	clusters, err := pr.queries.GetClusters(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("clusters not found")}
		}
		return nil, err
	}

	retClusters := make([]*Cluster, len(clusters))
	for i, cluster := range clusters {
		retClusters[i] = &Cluster{
			ID:          cluster.ID,
			LookupAlias: cluster.LookupAlias,
			Name:        cluster.Name,
		}
	}

	return retClusters, nil
}

func (pr *PostRegistry) GetPostsPageForCluster(
	ctx context.Context,
	clusterAlias string,
	hoursAgo int32,
	limit int32,
	cursor time.Time,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageForCluster")
	defer span.End()

	posts, err := pr.queries.GetPostsPageByClusterAlias(ctx, search_queries.GetPostsPageByClusterAliasParams{
		LookupAlias: clusterAlias,
		Limit:       limit,
		HoursAgo:    hoursAgo,
		CreatedAt:   cursor,
	})

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("posts not found")}
		}
		return nil, err
	}

	retPosts := make([]*Post, len(posts))
	for i, p := range posts {
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

		retPosts[i] = &Post{
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

	return retPosts, nil
}

func (pr *PostRegistry) GetPostsPageFromViewForCluster(
	ctx context.Context,
	clusterAlias string,
	hoursAgo int32,
	limit int32,
	cursor time.Time,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageFromViewForCluster")
	defer span.End()

	posts, err := pr.queries.GetPostsPageByClusterAliasFromView(ctx, search_queries.GetPostsPageByClusterAliasFromViewParams{
		ClusterLabel: sql.NullString{String: clusterAlias, Valid: true},
		Limit:        limit,
		CreatedAt:    cursor,
	})

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("posts not found")}
		}
		return nil, err
	}

	retPosts := make([]*Post, len(posts))
	for i, p := range posts {
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

		retPosts[i] = &Post{
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

	return retPosts, nil
}
