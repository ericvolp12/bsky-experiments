package store

import "context"

// CREATE TABLE follows (
//     uri text PRIMARY KEY,
//     target_did text,
//     created_at timestamp,
//     inserted_at timestamp
// ) WITH default_time_to_live = 0

// Follow represents a follow in the database
type Follow struct {
	URI        string
	TargetDID  string
	CreatedAt  int64
	InsertedAt int64
}

// CreateFollow creates a new follow in the database
func (s *Store) CreateFollow(ctx context.Context, follow *Follow) error {
	return s.ScyllaSession.Query(`
		INSERT INTO follows (
			uri,
			target_did,
			created_at,
			inserted_at
		) VALUES (?, ?, ?, ?)
	`, follow.URI, follow.TargetDID, follow.CreatedAt, follow.InsertedAt).WithContext(ctx).Exec()
}

// GetFollow gets a follow from the database
func (s *Store) GetFollow(ctx context.Context, uri string) (*Follow, error) {
	var follow Follow
	err := s.ScyllaSession.Query(`
		SELECT uri,
			target_did,
			created_at,
			inserted_at
		FROM follows
		WHERE uri = ?
	`, uri).WithContext(ctx).Scan(&follow.URI, &follow.TargetDID, &follow.CreatedAt, &follow.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &follow, nil
}

// DeleteFollow deletes a follow from the database
func (s *Store) DeleteFollow(ctx context.Context, uri string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM follows
		WHERE uri = ?
	`, uri).WithContext(ctx).Exec()
}

// GetFollowsByTargetDID gets follows by target did from the database
func (s *Store) GetFollowsByTargetDID(ctx context.Context, targetDID string) ([]*Follow, error) {
	var follows []*Follow
	iter := s.ScyllaSession.Query(`
		SELECT uri,
			target_did,
			created_at,
			inserted_at
		FROM follows
		WHERE target_did = ?
	`, targetDID).WithContext(ctx).Iter()
	for {
		var follow Follow
		if !iter.Scan(&follow.URI, &follow.TargetDID, &follow.CreatedAt, &follow.InsertedAt) {
			break
		}

		follows = append(follows, &follow)
	}

	return follows, nil
}

// GetFollowerCountByTargetDID gets follower count by target did from the database
func (s *Store) GetFollowerCountByTargetDID(ctx context.Context, targetDID string) (int, error) {
	var count int
	err := s.ScyllaSession.Query(`
		SELECT COUNT(*) FROM follows
		WHERE target_did = ?
	`, targetDID).WithContext(ctx).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
