package store

import "context"

// CREATE TABLE actor_blocks (
//     uri text PRIMARY KEY,
//     target_did text,
//     created_at timestamp,
//     inserted_at timestamp,
//     PRIMARY KEY (actor_did, target_did)
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// ActorBlock represents an actor block in the database
type ActorBlock struct {
	URI        string
	TargetDID  string
	CreatedAt  int64
	InsertedAt int64
}

// CreateActorBlock creates a new actor block in the database
func (s *Store) CreateActorBlock(ctx context.Context, actorBlock *ActorBlock) error {
	return s.ScyllaSession.Query(`
		INSERT INTO actor_blocks (
			uri,
			target_did,
			created_at,
			inserted_at
		) VALUES (?, ?, ?, ?)
	`, actorBlock.URI, actorBlock.TargetDID, actorBlock.CreatedAt, actorBlock.InsertedAt).WithContext(ctx).Exec()
}

// GetActorBlock gets an actor block from the database
func (s *Store) GetActorBlock(ctx context.Context, uri string) (*ActorBlock, error) {
	var actorBlock ActorBlock
	err := s.ScyllaSession.Query(`
		SELECT uri,
			target_did,
			created_at,
			inserted_at
		FROM actor_blocks
		WHERE uri = ?
	`, uri).WithContext(ctx).Scan(&actorBlock.URI, &actorBlock.TargetDID, &actorBlock.CreatedAt, &actorBlock.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &actorBlock, nil
}

// DeleteActorBlock deletes an actor block from the database
func (s *Store) DeleteActorBlock(ctx context.Context, uri string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM actor_blocks
		WHERE uri = ?
	`, uri).WithContext(ctx).Exec()
}

// GetActorBlocksByTargetDID gets actor blocks by target did from the database
func (s *Store) GetActorBlocksByTargetDID(ctx context.Context, targetDID string) ([]*ActorBlock, error) {
	var actorBlocks []*ActorBlock
	err := s.ScyllaSession.Query(`
		SELECT uri,
			target_did,
			created_at,
			inserted_at
		FROM actor_blocks
		WHERE target_did = ?
	`, targetDID).WithContext(ctx).Scan(&actorBlocks)
	if err != nil {
		return nil, err
	}

	return actorBlocks, nil
}

// GetActorBlockCountByTargetDID gets actor block count by target did from the database
func (s *Store) GetActorBlockCountByTargetDID(ctx context.Context, targetDID string) (int64, error) {
	var count int64
	err := s.ScyllaSession.Query(`
		SELECT COUNT(*)
		FROM actor_blocks
		WHERE target_did = ?
	`, targetDID).WithContext(ctx).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
