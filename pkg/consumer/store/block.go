package store

import "context"

// CREATE TABLE actor_blocks (
//     actor_did text,
//     rkey text,
//     target_did text,
//     created_at timestamp,
//     inserted_at timestamp,
//     PRIMARY KEY ((actor_did, rkey), target_did, created_at)
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// ActorBlock represents an actor block in the database
type ActorBlock struct {
	ActorDID   string `json:"actor_did"`
	RKey       string `json:"rkey"`
	TargetDID  string `json:"target_did"`
	CreatedAt  int64  `json:"created_at"`
	InsertedAt int64  `json:"inserted_at"`
}

// CreateActorBlock creates a new actor block in the database
func (s *Store) CreateActorBlock(ctx context.Context, actorBlock *ActorBlock) error {
	ctx, span := tracer.Start(ctx, "CreateActorBlock")
	defer span.End()

	return s.ScyllaSession.Query(`
		INSERT INTO actor_blocks (actor_did, rkey, target_did, created_at, inserted_at)
		VALUES (?, ?, ?, ?, ?)
	`, actorBlock.ActorDID, actorBlock.RKey, actorBlock.TargetDID, actorBlock.CreatedAt, actorBlock.InsertedAt).WithContext(ctx).Exec()
}

// GetActorBlocksByActorDID returns all actor blocks for a given actor DID
func (s *Store) GetActorBlocksByActorDID(ctx context.Context, actorDID string) ([]*ActorBlock, error) {
	ctx, span := tracer.Start(ctx, "GetActorBlocksByActorDID")
	defer span.End()

	var actorBlocks []*ActorBlock
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, target_did, created_at, inserted_at
		FROM actor_blocks
		WHERE actor_did = ?
	`, actorDID).WithContext(ctx).Scan(&actorBlocks); err != nil {
		return nil, err
	}
	return actorBlocks, nil
}

// GetActorBlocksByTargetDID returns all actor blocks for a given target DID
func (s *Store) GetActorBlocksByTargetDID(ctx context.Context, targetDID string) ([]*ActorBlock, error) {
	ctx, span := tracer.Start(ctx, "GetActorBlocksByTargetDID")
	defer span.End()

	var actorBlocks []*ActorBlock
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, target_did, created_at, inserted_at
		FROM actor_blocks
		WHERE target_did = ?
	`, targetDID).WithContext(ctx).Scan(&actorBlocks); err != nil {
		return nil, err
	}
	return actorBlocks, nil
}

// DeleteActorBlock deletes an actor block from the database
func (s *Store) DeleteActorBlock(ctx context.Context, actorDID, rKey string) error {
	ctx, span := tracer.Start(ctx, "DeleteActorBlock")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM actor_blocks
		WHERE actor_did = ? AND rkey = ?
	`, actorDID, rKey).WithContext(ctx).Exec()
}
