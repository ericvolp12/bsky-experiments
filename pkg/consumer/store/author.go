package store

import "context"

// CREATE TABLE actors (did text PRIMARY KEY, handle text) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Actor represents an actor in the database
type Actor struct {
	DID    string
	Handle string
}

// CreateActor creates a new actor in the database
func (s *Store) CreateActor(ctx context.Context, actor *Actor) error {
	ctx, span := tracer.Start(ctx, "CreateActor")
	defer span.End()

	return s.ScyllaSession.Query(`
		INSERT INTO actors (
			did,
			handle
		) VALUES (?, ?)
	`, actor.DID, actor.Handle).WithContext(ctx).Exec()
}

// GetActor gets an actor from the database
func (s *Store) GetActor(ctx context.Context, did string) (*Actor, error) {
	ctx, span := tracer.Start(ctx, "GetActor")
	defer span.End()

	var actor Actor
	err := s.ScyllaSession.Query(`
		SELECT did,
			handle
		FROM actors
		WHERE did = ?
	`, did).WithContext(ctx).Scan(&actor.DID, &actor.Handle)
	if err != nil {
		return nil, err
	}

	return &actor, nil
}

// GetActorByHandle gets an actor from the database by handle
func (s *Store) GetActorByHandle(ctx context.Context, handle string) (*Actor, error) {
	ctx, span := tracer.Start(ctx, "GetActorByHandle")
	defer span.End()

	var actor Actor
	err := s.ScyllaSession.Query(`
		SELECT did,
			handle
		FROM actors
		WHERE handle = ?
	`, handle).WithContext(ctx).Scan(&actor.DID, &actor.Handle)
	if err != nil {
		return nil, err
	}

	return &actor, nil
}

// UpdateActor updates an actor in the database
func (s *Store) UpdateActor(ctx context.Context, actor *Actor) error {
	ctx, span := tracer.Start(ctx, "UpdateActor")
	defer span.End()

	return s.ScyllaSession.Query(`
		UPDATE actors
		SET handle = ?
		WHERE did = ?
	`, actor.Handle, actor.DID).WithContext(ctx).Exec()
}

// DeleteActor deletes an actor from the database
func (s *Store) DeleteActor(ctx context.Context, did string) error {
	ctx, span := tracer.Start(ctx, "DeleteActor")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM actors
		WHERE did = ?
	`, did).WithContext(ctx).Exec()
}
