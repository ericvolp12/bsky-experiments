// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: get_posts_from_label_view.sql

package search_queries

import (
	"context"
)

const getOnlyPostsPageByAuthorLabelAliasFromView = `-- name: GetOnlyPostsPageByAuthorLabelAliasFromView :many
SELECT h.id, h.text, h.parent_post_id, h.root_post_id, h.author_did, h.created_at, h.has_embedded_media, h.parent_relationship, h.sentiment, h.sentiment_confidence, h.post_labels, h.cluster_label, h.author_labels, h.hotness
FROM post_hotness h
WHERE $1 = ANY(h.author_labels)
    AND (
        (h.parent_relationship IS NULL)
        OR (h.parent_relationship <> 'r'::text)
    )
    AND (
        CASE
            WHEN $2 = '' THEN TRUE
            ELSE h.id < $2
        END
    )
ORDER BY h.id DESC
LIMIT $3
`

type GetOnlyPostsPageByAuthorLabelAliasFromViewParams struct {
	LookupAlias interface{} `json:"lookup_alias"`
	Cursor      interface{} `json:"cursor"`
	Limit       int32       `json:"limit"`
}

func (q *Queries) GetOnlyPostsPageByAuthorLabelAliasFromView(ctx context.Context, arg GetOnlyPostsPageByAuthorLabelAliasFromViewParams) ([]PostHotness, error) {
	rows, err := q.query(ctx, q.getOnlyPostsPageByAuthorLabelAliasFromViewStmt, getOnlyPostsPageByAuthorLabelAliasFromView, arg.LookupAlias, arg.Cursor, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []PostHotness
	for rows.Next() {
		var i PostHotness
		if err := rows.Scan(
			&i.ID,
			&i.Text,
			&i.ParentPostID,
			&i.RootPostID,
			&i.AuthorDid,
			&i.CreatedAt,
			&i.HasEmbeddedMedia,
			&i.ParentRelationship,
			&i.Sentiment,
			&i.SentimentConfidence,
			&i.PostLabels,
			&i.ClusterLabel,
			&i.AuthorLabels,
			&i.Hotness,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getPostsPageByAuthorLabelAliasFromView = `-- name: GetPostsPageByAuthorLabelAliasFromView :many
SELECT h.id, h.text, h.parent_post_id, h.root_post_id, h.author_did, h.created_at, h.has_embedded_media, h.parent_relationship, h.sentiment, h.sentiment_confidence, h.post_labels, h.cluster_label, h.author_labels, h.hotness
FROM post_hotness h
WHERE $1 = ANY(h.author_labels)
    AND (
        CASE
            WHEN $2 = '' THEN TRUE
            ELSE h.id < $2
        END
    )
ORDER BY h.id DESC
LIMIT $3
`

type GetPostsPageByAuthorLabelAliasFromViewParams struct {
	LookupAlias interface{} `json:"lookup_alias"`
	Cursor      interface{} `json:"cursor"`
	Limit       int32       `json:"limit"`
}

func (q *Queries) GetPostsPageByAuthorLabelAliasFromView(ctx context.Context, arg GetPostsPageByAuthorLabelAliasFromViewParams) ([]PostHotness, error) {
	rows, err := q.query(ctx, q.getPostsPageByAuthorLabelAliasFromViewStmt, getPostsPageByAuthorLabelAliasFromView, arg.LookupAlias, arg.Cursor, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []PostHotness
	for rows.Next() {
		var i PostHotness
		if err := rows.Scan(
			&i.ID,
			&i.Text,
			&i.ParentPostID,
			&i.RootPostID,
			&i.AuthorDid,
			&i.CreatedAt,
			&i.HasEmbeddedMedia,
			&i.ParentRelationship,
			&i.Sentiment,
			&i.SentimentConfidence,
			&i.PostLabels,
			&i.ClusterLabel,
			&i.AuthorLabels,
			&i.Hotness,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
