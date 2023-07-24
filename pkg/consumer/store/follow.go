package store

import "context"

// CREATE TABLE follows (
//     actor_did text,
//     rkey text,
//     target_did text,
//     created_at timestamp,
//     inserted_at timestamp,
//     PRIMARY KEY ( (actor_did, rkey), target_did, created_at )
// ) WITH default_time_to_live = 0

// Follow represents a follow in the database
type Follow struct {
	ActorDID   string `json:"actor_did"`
	RKey       string `json:"rkey"`
	TargetDID  string `json:"target_did"`
	CreatedAt  int64  `json:"created_at"`
	InsertedAt int64  `json:"inserted_at"`
}

// CreateFollow creates a new follow in the database
func (s *Store) CreateFollow(ctx context.Context, follow *Follow) error {
	ctx, span := tracer.Start(ctx, "CreateFollow")
	defer span.End()

	return s.ScyllaSession.Query(`
		INSERT INTO follows (actor_did, rkey, target_did, created_at, inserted_at)
		VALUES (?, ?, ?, ?, ?)
	`, follow.ActorDID, follow.RKey, follow.TargetDID, follow.CreatedAt, follow.InsertedAt).WithContext(ctx).Exec()
}

// GetFollowsByActorDID returns all follows for a given actor DID
func (s *Store) GetFollowsByActorDID(ctx context.Context, actorDID string) ([]*Follow, error) {
	ctx, span := tracer.Start(ctx, "GetFollowsByActorDID")
	defer span.End()

	var follows []*Follow
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, target_did, created_at, inserted_at
		FROM follows
		WHERE actor_did = ?
	`, actorDID).WithContext(ctx).Scan(&follows); err != nil {
		return nil, err
	}
	return follows, nil
}

// GetFollowsByTargetDID returns all follows for a given target DID
func (s *Store) GetFollowsByTargetDID(ctx context.Context, targetDID string) ([]*Follow, error) {
	ctx, span := tracer.Start(ctx, "GetFollowsByTargetDID")
	defer span.End()

	var follows []*Follow
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, target_did, created_at, inserted_at
		FROM follows
		WHERE target_did = ?
	`, targetDID).WithContext(ctx).Scan(&follows); err != nil {
		return nil, err
	}
	return follows, nil
}

// DeleteFollow deletes a follow from the database
func (s *Store) DeleteFollow(ctx context.Context, actorDID, rKey string) error {
	ctx, span := tracer.Start(ctx, "DeleteFollow")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM follows
		WHERE actor_did = ? AND rkey = ?
	`, actorDID, rKey).WithContext(ctx).Exec()
}
