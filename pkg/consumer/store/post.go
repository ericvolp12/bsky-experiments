package store

import "context"

// CREATE TABLE posts (
//     actor_did text,
//     rkey text,
//     content text,
//     parent_post_uri text,
//     parent_relationship text,
//     root_post_uri text,
//     has_embedded_media tinyint,
//     created_at timestamp,
//     inserted_at timestamp,
//     PRIMARY KEY ((actor_did, rkey), created_at)
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Post represents a post in the database
type Post struct {
	ActorDID           string `json:"actor_did"`
	RKey               string `json:"rkey"`
	Content            string `json:"content"`
	ParentPostURI      string `json:"parent_post_uri"`
	ParentRelationship string `json:"parent_relationship"`
	RootPostURI        string `json:"root_post_uri"`
	HasEmbeddedMedia   bool   `json:"has_embedded_media"`
	CreatedAt          int64  `json:"created_at"`
	InsertedAt         int64  `json:"inserted_at"`
}

// CreatePost creates a new post in the database
func (s *Store) CreatePost(ctx context.Context, post *Post) error {
	hasEmbeddedMedia := 0
	if post.HasEmbeddedMedia {
		hasEmbeddedMedia = 1
	}
	return s.ScyllaSession.Query(`
		INSERT INTO posts (actor_did, rkey, content, parent_post_uri, parent_relationship, root_post_uri, has_embedded_media, created_at, inserted_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, post.ActorDID, post.RKey, post.Content, post.ParentPostURI, post.ParentRelationship, post.RootPostURI, hasEmbeddedMedia, post.CreatedAt, post.InsertedAt).WithContext(ctx).Exec()
}

// GetPostsByActorDID returns all posts for a given actor DID
func (s *Store) GetPostsByActorDID(ctx context.Context, actorDID string) ([]*Post, error) {
	var posts []*Post
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, content, parent_post_uri, parent_relationship, root_post_uri, has_embedded_media, created_at, inserted_at
		FROM posts
		WHERE actor_did = ?
	`, actorDID).WithContext(ctx).Scan(&posts); err != nil {
		return nil, err
	}
	return posts, nil
}

// GetPostsByRootPostURI returns all posts for a given root post URI
func (s *Store) GetPostsByRootPostURI(ctx context.Context, rootPostURI string) ([]*Post, error) {
	var posts []*Post
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, content, parent_post_uri, parent_relationship, root_post_uri, has_embedded_media, created_at, inserted_at
		FROM posts
		WHERE root_post_uri = ?
	`, rootPostURI).WithContext(ctx).Scan(&posts); err != nil {
		return nil, err
	}
	return posts, nil
}

// DeletePost deletes a post from the database
func (s *Store) DeletePost(ctx context.Context, actorDID, rKey string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM posts
		WHERE actor_did = ? AND rkey = ?
	`, actorDID, rKey).WithContext(ctx).Exec()
}
