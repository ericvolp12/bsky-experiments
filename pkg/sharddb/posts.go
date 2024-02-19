package sharddb

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"go.opentelemetry.io/otel/attribute"
)

type Post struct {
	ActorDID  string
	Rkey      string
	IndexedAt time.Time
	Bucket    int
	Raw       []byte
	Langs     []string
	Tags      []string
	HasMedia  bool
	IsReply   bool
}

func (s *ShardDB) CreatePostTable(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "CreatePostTable")
	defer span.End()

	createTableQuery := `
		CREATE TABLE IF NOT EXISTS posts_revchron (
			actor_did text,
			rkey text,
			indexed_at timestamp,
			bucket int,
			raw blob,
			langs list<text>,
			tags list<text>,
			has_media boolean,
			is_reply boolean,
			PRIMARY KEY ((bucket), indexed_at, actor_did, rkey)
		) WITH CLUSTERING ORDER BY (indexed_at DESC, actor_did ASC, rkey ASC);
	`
	return s.session.Query(createTableQuery).WithContext(ctx).Exec()
}

func GetBucketFromRKey(rkey string) (int, error) {
	tid, err := syntax.ParseTID(rkey)
	if err != nil {
		return 0, fmt.Errorf("failed to parse TID: %w", err)
	}

	// We want one bucket per 30 minute window, int casting floors the result
	return int(tid.Time().Unix() / (30 * 60)), nil
}

func GetHighestRKeyForBucket(bucket int) string {
	newTime := time.Unix(int64((bucket+1)*(30*60)), 0).Add(-1 * time.Second)
	return syntax.NewTID(newTime.UnixMicro(), 0).String()
}

func GetLowestRKeyForBucket(bucket int) string {
	newTime := time.Unix(int64(bucket*(30*60)), 0)
	return syntax.NewTID(newTime.UnixMicro(), 0).String()
}

func (s *ShardDB) InsertPost(ctx context.Context, post Post) error {
	ctx, span := tracer.Start(ctx, "InsertPost")
	defer span.End()

	span.SetAttributes(
		attribute.String("actor_did", post.ActorDID),
		attribute.String("rkey", post.Rkey),
		attribute.Int("bucket", post.Bucket),
		attribute.Bool("has_media", post.HasMedia),
		attribute.Bool("is_reply", post.IsReply),
	)

	insertPostQuery := `
		INSERT INTO posts_revchron (actor_did, rkey, indexed_at, bucket, raw, langs, tags, has_media, is_reply)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	start := time.Now()
	err := s.session.Query(insertPostQuery,
		post.ActorDID,
		post.Rkey,
		post.IndexedAt,
		post.Bucket,
		post.Raw,
		post.Langs,
		post.Tags,
		post.HasMedia,
		post.IsReply,
	).WithContext(ctx).Exec()

	if err != nil {
		QueryDuration.WithLabelValues("insert_post", "error").Observe(time.Since(start).Seconds())
		span.RecordError(err)
		return fmt.Errorf("failed to insert post: %w", err)
	}
	QueryDuration.WithLabelValues("insert_post", "success").Observe(time.Since(start).Seconds())

	return nil
}

func (s *ShardDB) DeletePost(ctx context.Context, actorDID, rkey string, indexedAt time.Time, bucket int) error {
	ctx, span := tracer.Start(ctx, "DeletePost")
	defer span.End()

	span.SetAttributes(
		attribute.String("actor_did", actorDID),
		attribute.String("rkey", rkey),
		attribute.Int("bucket", bucket),
		attribute.String("indexed_at", indexedAt.Format(time.RFC3339)),
	)

	deletePostQuery := `
		DELETE FROM posts_revchron
		WHERE actor_did = ? AND rkey = ? AND indexed_at = ? AND bucket = ?
		IF EXISTS
	`

	start := time.Now()
	applied, err := s.session.Query(deletePostQuery, actorDID, rkey, indexedAt, bucket).WithContext(ctx).MapScanCAS(map[string]interface{}{})
	if err != nil {
		QueryDuration.WithLabelValues("delete_post", "error").Observe(time.Since(start).Seconds())
		span.RecordError(err)
		return fmt.Errorf("failed to delete post: %w", err)
	}

	if !applied {
		QueryDuration.WithLabelValues("delete_post", "not_found").Observe(time.Since(start).Seconds())
		span.SetAttributes(attribute.Bool("not_found", true))
		return nil
	}

	QueryDuration.WithLabelValues("delete_post", "success").Observe(time.Since(start).Seconds())

	return nil
}

func (s *ShardDB) GetPosts(ctx context.Context, bucket, limit int, cursor time.Time) ([]*Post, time.Time, error) {
	ctx, span := tracer.Start(ctx, "GetPosts")
	defer span.End()

	span.SetAttributes(
		attribute.Int("bucket", bucket),
		attribute.Int("limit", limit),
		attribute.String("cursor", cursor.Format(time.RFC3339)),
	)

	getPostsQuery := `
		SELECT actor_did, rkey, indexed_at, bucket, raw, langs, tags, has_media, is_reply
		FROM posts_revchron
		WHERE bucket = ? AND indexed_at < ?
		LIMIT ?
	`

	start := time.Now()

	scanner := s.session.Query(getPostsQuery, bucket, cursor, limit).WithContext(ctx).Iter().Scanner()

	var posts []*Post
	for scanner.Next() {
		var actorDID, rkey string
		var indexedAt time.Time
		var hasMedia, isReply bool
		var raw []byte
		var langs, tags []string
		err := scanner.Scan(
			&actorDID,
			&rkey,
			&indexedAt,
			&bucket,
			&raw,
			&langs,
			&tags,
			&hasMedia,
			&isReply,
		)

		if err != nil {
			QueryDuration.WithLabelValues("get_posts", "error").Observe(time.Since(start).Seconds())
			span.RecordError(err)
			return nil, time.Time{}, fmt.Errorf("failed to get posts: %w", err)
		}

		posts = append(posts, &Post{
			ActorDID:  actorDID,
			Rkey:      rkey,
			IndexedAt: indexedAt,
			Bucket:    bucket,
			Raw:       raw,
			Langs:     langs,
			Tags:      tags,
			HasMedia:  hasMedia,
			IsReply:   isReply,
		})
		cursor = indexedAt
	}

	QueryDuration.WithLabelValues("get_posts", "success").Observe(time.Since(start).Seconds())

	if len(posts) < limit {
		cursor = time.Time{}
	}

	span.SetAttributes(
		attribute.Int("num_results", len(posts)),
	)

	return posts, cursor, nil
}

// GetPostMetas returns a page of posts without the raw content
func (s *ShardDB) GetPostMetas(ctx context.Context, bucket, limit int, cursor time.Time) ([]*Post, time.Time, error) {
	ctx, span := tracer.Start(ctx, "GetPostMetas")
	defer span.End()

	span.SetAttributes(
		attribute.Int("bucket", bucket),
		attribute.Int("limit", limit),
		attribute.String("cursor", cursor.Format(time.RFC3339)),
	)

	getPostsQuery := `
		SELECT actor_did, rkey, indexed_at, bucket, langs, tags, has_media, is_reply
		FROM posts_revchron
		WHERE bucket = ? AND indexed_at < ?
		LIMIT ?
	`

	start := time.Now()

	scanner := s.session.Query(getPostsQuery, bucket, cursor, limit).WithContext(ctx).Iter().Scanner()

	var posts []*Post
	for scanner.Next() {
		var actorDID, rkey string
		var indexedAt time.Time
		var hasMedia, isReply bool
		var langs, tags []string
		err := scanner.Scan(
			&actorDID,
			&rkey,
			&indexedAt,
			&bucket,
			&langs,
			&tags,
			&hasMedia,
			&isReply,
		)

		if err != nil {
			QueryDuration.WithLabelValues("get_post_metas", "error").Observe(time.Since(start).Seconds())
			span.RecordError(err)
			return nil, time.Time{}, fmt.Errorf("failed to get post metas: %w", err)
		}

		posts = append(posts, &Post{
			ActorDID:  actorDID,
			Rkey:      rkey,
			IndexedAt: indexedAt,
			Bucket:    bucket,
			Langs:     langs,
			Tags:      tags,
			HasMedia:  hasMedia,
			IsReply:   isReply,
		})
		cursor = indexedAt
	}
	QueryDuration.WithLabelValues("get_post_metas", "success").Observe(time.Since(start).Seconds())

	if len(posts) < limit {
		cursor = time.Time{}
	}

	span.SetAttributes(
		attribute.Int("num_results", len(posts)),
	)

	return posts, cursor, nil
}
