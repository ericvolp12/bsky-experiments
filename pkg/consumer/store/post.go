package store

import "context"

// CREATE TABLE posts (
//     uri text PRIMARY KEY,
//     content text,
//     parent_post_uri text,
//     parent_relationship text,
//     root_post_uri text,
//     has_embedded_media tinyint,
//     created_at timestamp,
//     inserted_at timestamp
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Post represents a post in the database
type Post struct {
	URI                string
	Content            string
	ParentPostURI      string
	ParentRelationship string
	RootPostURI        string
	HasEmbeddedMedia   bool
	CreatedAt          int64
	InsertedAt         int64
}

// CreatePost creates a new post in the database
func (s *Store) CreatePost(ctx context.Context, post *Post) error {
	hasEmbeddedMedia := 0
	if post.HasEmbeddedMedia {
		hasEmbeddedMedia = 1
	}
	return s.ScyllaSession.Query(`
		INSERT INTO posts (
			uri,
			content,
			parent_post_uri,
			parent_relationship,
			root_post_uri,
			has_embedded_media,
			created_at,
			inserted_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, post.URI, post.Content, post.ParentPostURI, post.ParentRelationship, post.RootPostURI, hasEmbeddedMedia, post.CreatedAt, post.InsertedAt).WithContext(ctx).Exec()
}

// GetPost gets a post from the database
func (s *Store) GetPost(ctx context.Context, uri string) (*Post, error) {
	var post Post
	err := s.ScyllaSession.Query(`
		SELECT uri,
			content,
			parent_post_uri,
			parent_relationship,
			root_post_uri,
			has_embedded_media,
			created_at,
			inserted_at
		FROM posts
		WHERE uri = ?
	`, uri).WithContext(ctx).Scan(&post.URI, &post.Content, &post.ParentPostURI, &post.ParentRelationship, &post.RootPostURI, &post.HasEmbeddedMedia, &post.CreatedAt, &post.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &post, nil
}

// DeletePost deletes a post from the database
func (s *Store) DeletePost(ctx context.Context, uri string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM posts
		WHERE uri = ?
	`, uri).WithContext(ctx).Exec()
}
