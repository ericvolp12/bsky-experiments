package store

import "context"

// CREATE TABLE likes (
//     uri text PRIMARY KEY,
//     post_uri text,
//     created_at timestamp,
//     inserted_at timestamp
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Like represents a like in the database
type Like struct {
	URI        string
	PostURI    string
	CreatedAt  int64
	InsertedAt int64
}

// CreateLike creates a new like in the database
func (s *Store) CreateLike(ctx context.Context, like *Like) error {
	return s.ScyllaSession.Query(`
		INSERT INTO likes (
			uri,
			post_uri,
			created_at,
			inserted_at
		) VALUES (?, ?, ?, ?)
	`, like.URI, like.PostURI, like.CreatedAt, like.InsertedAt).WithContext(ctx).Exec()
}

// GetLike gets a like from the database
func (s *Store) GetLike(ctx context.Context, uri string) (*Like, error) {
	var like Like
	err := s.ScyllaSession.Query(`
		SELECT uri,
			post_uri,
			created_at,
			inserted_at
		FROM likes
		WHERE uri = ?
	`, uri).WithContext(ctx).Scan(&like.URI, &like.PostURI, &like.CreatedAt, &like.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &like, nil
}

// DeleteLike deletes a like from the database
func (s *Store) DeleteLike(ctx context.Context, uri string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM likes
		WHERE uri = ?
	`, uri).WithContext(ctx).Exec()
}

// GetLikesByPostURI gets likes by post uri from the database
func (s *Store) GetLikesByPostURI(ctx context.Context, postURI string) ([]*Like, error) {
	var likes []*Like
	iter := s.ScyllaSession.Query(`
		SELECT uri,
			post_uri,
			created_at,
			inserted_at
		FROM likes
		WHERE post_uri = ?
	`, postURI).WithContext(ctx).Iter()
	for {
		var like Like
		if !iter.Scan(&like.URI, &like.PostURI, &like.CreatedAt, &like.InsertedAt) {
			break
		}

		likes = append(likes, &like)
	}

	return likes, nil
}

// GetLikeCountByPostURI gets like count by post uri from the database
func (s *Store) GetLikeCountByPostURI(ctx context.Context, postURI string) (int, error) {
	var count int
	err := s.ScyllaSession.Query(`
		SELECT count(*)
		FROM likes
		WHERE post_uri = ?
	`, postURI).WithContext(ctx).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
