package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq" // postgres driver

	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type Label struct {
	ID          string `json:"id"`
	LookupAlias string `json:"lookup_alias"`
	Name        string `json:"name"`
}

func (pr *PostRegistry) AddPostLabel(ctx context.Context, postID string, authorDid string, label string) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddPostLabel")
	defer span.End()

	err := pr.queries.AddPostLabel(ctx, search_queries.AddPostLabelParams{
		PostID:    postID,
		AuthorDid: authorDid,
		Label:     label,
	})
	return err
}

func (pr *PostRegistry) AddOneLabelPerPost(ctx context.Context, labels []string, postIDs []string, authorDIDs []string) error {
	tracer := otel.Tracer("PostRegistry")
	ctx, span := tracer.Start(ctx, "AddOneLabelPerPost")
	defer span.End()

	if len(labels) != len(postIDs) || len(labels) != len(authorDIDs) {
		return fmt.Errorf("labels, postIDs, and authorDIDs must be the same length")
	}

	labelMap := make([]map[string]interface{}, len(labels))

	for i, postID := range postIDs {
		labelMap[i] = map[string]interface{}{
			"post_id":    postID,
			"author_did": authorDIDs[i],
			"label":      labels[i],
		}
	}

	b, err := json.Marshal(labelMap)
	if err != nil {
		return fmt.Errorf("error marshalling labels: %w", err)
	}

	err = pr.queries.AddLabelsToPosts(ctx, b)

	return err
}

func (pr *PostRegistry) CreateLabel(ctx context.Context, labelAlias string, labelName string) (*Label, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:CreateLabel")
	defer span.End()

	labelFromDB, err := pr.queries.AddLabel(ctx, search_queries.AddLabelParams{
		LookupAlias: labelAlias,
		Name:        labelName,
	})

	if err != nil {
		return nil, err
	}

	return &Label{
		ID:          fmt.Sprintf("%d", labelFromDB.ID),
		LookupAlias: labelFromDB.LookupAlias,
		Name:        labelFromDB.Name,
	}, nil
}

func (pr *PostRegistry) GetAllLabels(ctx context.Context, limit int32, offset int32) ([]Label, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetAllLabels")
	defer span.End()

	labels, err := pr.queries.GetAllLabels(ctx, search_queries.GetAllLabelsParams{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		return nil, err
	}

	retLabels := make([]Label, len(labels))
	for i, label := range labels {
		retLabels[i] = Label{
			ID:          fmt.Sprintf("%d", label.ID),
			LookupAlias: label.LookupAlias,
			Name:        label.Name,
		}
	}

	return retLabels, nil
}

func (pr *PostRegistry) AssignLabelToAuthorByAlias(ctx context.Context, authorDID string, labelAlias string) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AssignLabelToAuthorByAlias")
	defer span.End()

	label, err := pr.queries.GetLabelByAlias(ctx, labelAlias)
	if err != nil {
		return NotFoundError{fmt.Errorf("label not found")}
	}

	err = pr.queries.AssignLabelToAuthor(ctx, search_queries.AssignLabelToAuthorParams{
		AuthorDid: authorDID,
		LabelID:   label.ID,
	})

	if err != nil {
		span.SetAttributes(attribute.String("db.error", err.Error()))
	}

	return err
}

func (pr *PostRegistry) GetLabelsForAuthor(ctx context.Context, authorDID string) ([]*Label, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetLabelsForAuthor")
	defer span.End()

	labels, err := pr.queries.GetLabelsForAuthor(ctx, authorDID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("author not found")}
		}
		span.SetAttributes(attribute.String("db.error", err.Error()))
	}

	retLabels := make([]*Label, len(labels))
	for i, l := range labels {
		retLabels[i] = &Label{
			ID:          fmt.Sprintf("%d", l.ID),
			LookupAlias: l.LookupAlias,
			Name:        l.Name,
		}
	}

	return retLabels, nil
}

func (pr *PostRegistry) GetMembersOfAuthorLabel(ctx context.Context, labelAlias string) ([]*Author, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetMembersOfAuthorLabel")
	defer span.End()

	label, err := pr.queries.GetLabelByAlias(ctx, labelAlias)
	if err != nil {
		return nil, NotFoundError{fmt.Errorf("label not found")}
	}

	authors, err := pr.queries.GetMembersOfAuthorLabel(ctx, label.ID)
	if err != nil {
		span.SetAttributes(attribute.String("db.error", err.Error()))
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("label not found")}
		}

		return nil, err
	}

	retAuthors := make([]*Author, len(authors))
	for i, a := range authors {
		retAuthors[i] = &Author{
			DID:    a.Did,
			Handle: a.Handle,
		}
	}

	return retAuthors, nil
}

func (pr *PostRegistry) UnassignLabelFromAuthorByAlias(ctx context.Context, authorDID string, labelAlias string) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:UnassignLabelFromAuthorByAlias")
	defer span.End()

	label, err := pr.queries.GetLabelByAlias(ctx, labelAlias)
	if err != nil {
		return NotFoundError{fmt.Errorf("label not found")}
	}

	err = pr.queries.UnassignLabelFromAuthor(ctx, search_queries.UnassignLabelFromAuthorParams{
		AuthorDid: authorDID,
		LabelID:   label.ID,
	})

	if err != nil {
		span.SetAttributes(attribute.String("db.error", err.Error()))
	}

	if err == sql.ErrNoRows {
		return NotFoundError{fmt.Errorf("author was not assigned label")}
	}

	return err
}

func (pr *PostRegistry) GetLabelPage(ctx context.Context, limit int32, offset int32) ([]*Label, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetLabelPage")
	defer span.End()

	labels, err := pr.queries.GetLabels(ctx, search_queries.GetLabelsParams{
		Limit:  limit,
		Offset: offset,
	})

	if err != nil {
		return nil, err
	}

	retLabels := make([]*Label, len(labels))
	for i, l := range labels {
		retLabels[i] = &Label{
			ID:          fmt.Sprintf("%d", l.ID),
			LookupAlias: l.LookupAlias,
			Name:        l.Name,
		}
	}

	return retLabels, nil
}

func (pr *PostRegistry) GetPostsPageForPostLabel(
	ctx context.Context,
	postLabel string,
	hoursAgo int32,
	limit int32,
	cursor string,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageForPostLabel")
	defer span.End()

	posts, err := pr.queries.GetPostsPageWithPostLabel(ctx, search_queries.GetPostsPageWithPostLabelParams{
		Label:    postLabel,
		Limit:    limit,
		HoursAgo: hoursAgo,
		Cursor:   cursor,
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

func (pr *PostRegistry) GetPostsPageForPostLabelsByHotness(
	ctx context.Context,
	postLabels []string,
	limit int32,
	cursor float64,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageForPostLabelsByHotness")
	defer span.End()

	posts, err := pr.queries.GetPostsPageWithAnyPostLabelSortedByHotness(ctx, search_queries.GetPostsPageWithAnyPostLabelSortedByHotnessParams{
		Labels: postLabels,
		Limit:  limit,
		Cursor: cursor,
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

		hotness := p.Hotness

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
			Hotness:             &hotness,
		}

	}

	return retPosts, nil
}

func (pr *PostRegistry) GetPostsPageForPostLabelByHotness(
	ctx context.Context,
	postLabel string,
	limit int32,
	cursor float64,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageForPostLabelByHotness")
	defer span.End()

	posts, err := pr.queries.GetPostsPageWithPostLabelSortedByHotness(ctx, search_queries.GetPostsPageWithPostLabelSortedByHotnessParams{
		Label:  postLabel,
		Limit:  limit,
		Cursor: cursor,
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

		hotness := p.Hotness

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
			Hotness:             &hotness,
		}

	}

	return retPosts, nil
}

func (pr *PostRegistry) GetPostsPageForPostLabelChronological(
	ctx context.Context,
	postLabel string,
	limit int32,
	cursor time.Time,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageForPostLabelChronological")
	defer span.End()

	posts, err := pr.queries.GetPostsPageWithPostLabelChronological(ctx, search_queries.GetPostsPageWithPostLabelChronologicalParams{
		Label:     postLabel,
		Limit:     limit,
		CreatedAt: cursor,
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

func (pr *PostRegistry) GetUniquePostLabels(ctx context.Context) ([]string, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetUniquePostLabels")
	defer span.End()

	labels, err := pr.queries.GetAllUniquePostLabels(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("labels not found")}
		}
		return nil, err
	}

	return labels, nil
}

func (pr *PostRegistry) GetPostsPageForAuthorLabel(
	ctx context.Context,
	authorLabel string,
	hoursAgo int32,
	limit int32,
	cursor string,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageForAuthorLabel")
	defer span.End()

	posts, err := pr.queries.GetPostsPageByAuthorLabelAlias(ctx, search_queries.GetPostsPageByAuthorLabelAliasParams{
		LookupAlias: authorLabel,
		Limit:       limit,
		HoursAgo:    hoursAgo,
		Cursor:      cursor,
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

func (pr *PostRegistry) GetPostsPageForAuthorLabelFromView(
	ctx context.Context,
	authorLabel string,
	hoursAgo int32,
	limit int32,
	cursor string,
) ([]*Post, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetPostsPageForAuthorLabelFromView")
	defer span.End()

	posts, err := pr.queries.GetPostsPageByAuthorLabelAliasFromView(ctx, search_queries.GetPostsPageByAuthorLabelAliasFromViewParams{
		LookupAlias: authorLabel,
		Limit:       limit,
		Cursor:      cursor,
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
