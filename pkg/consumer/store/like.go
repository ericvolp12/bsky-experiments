package store

import "context"

// CREATE TABLE likes (
//     actor_did text,
//     rkey text,
//     subject_uri text,
//     created_at timestamp,
//     inserted_at timestamp,
//     PRIMARY KEY ((actor_did, rkey), subject_uri, created_at)
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// CREATE TABLE like_counts (
//     subject_uri text,
//     count counter,
//     PRIMARY KEY (subject_uri)
// ) WITH default_time_to_live = 0

// Like represents a like in the database
type Like struct {
	ActorDID   string `json:"actor_did"`
	RKey       string `json:"rkey"`
	SubjectURI string `json:"subject_uri"`
	CreatedAt  int64  `json:"created_at"`
	InsertedAt int64  `json:"inserted_at"`
}

// LikeCount represents a like count in the database
type LikeCount struct {
	SubjectURI string `json:"subject_uri"`
	Count      int64  `json:"count"`
}

// CreateLike creates a new like in the database
func (s *Store) CreateLike(ctx context.Context, like *Like) error {
	return s.ScyllaSession.Query(`
		INSERT INTO likes (actor_did, rkey, subject_uri, created_at, inserted_at)
		VALUES (?, ?, ?, ?, ?)
	`, like.ActorDID, like.RKey, like.SubjectURI, like.CreatedAt, like.InsertedAt).WithContext(ctx).Exec()
}

// GetLikesByActorDID returns all likes for a given actor DID
func (s *Store) GetLikesByActorDID(ctx context.Context, actorDID string) ([]*Like, error) {
	var likes []*Like
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, subject_uri, created_at, inserted_at
		FROM likes
		WHERE actor_did = ?
	`, actorDID).WithContext(ctx).Scan(&likes); err != nil {
		return nil, err
	}
	return likes, nil
}

// GetLikesBySubjectURI returns all likes for a given subject URI
func (s *Store) GetLikesBySubjectURI(ctx context.Context, subjectURI string) ([]*Like, error) {
	var likes []*Like
	if err := s.ScyllaSession.Query(`
		SELECT actor_did, rkey, subject_uri, created_at, inserted_at
		FROM likes
		WHERE subject_uri = ?
	`, subjectURI).WithContext(ctx).Scan(&likes); err != nil {
		return nil, err
	}
	return likes, nil
}

// DeleteLike deletes a like from the database
func (s *Store) DeleteLike(ctx context.Context, actorDID, rKey string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM likes
		WHERE actor_did = ? AND rkey = ?
	`, actorDID, rKey).WithContext(ctx).Exec()
}

// DeleteLikesByActorDID deletes all likes for a given actor DID
func (s *Store) DeleteLikesByActorDID(ctx context.Context, actorDID string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM likes
		WHERE actor_did = ?
	`, actorDID).WithContext(ctx).Exec()
}

// DeleteLikesBySubjectURI deletes all likes for a given subject URI
func (s *Store) DeleteLikesBySubjectURI(ctx context.Context, subjectURI string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM likes
		WHERE subject_uri = ?
	`, subjectURI).WithContext(ctx).Exec()
}

// IncrementLikeCount increments the like count for a given subject URI
func (s *Store) IncrementLikeCount(ctx context.Context, subjectURI string) error {
	return s.ScyllaSession.Query(`
		UPDATE like_counts
		SET count = count + 1
		WHERE subject_uri = ?
	`, subjectURI).WithContext(ctx).Exec()
}

// DecrementLikeCount decrements the like count for a given subject URI
func (s *Store) DecrementLikeCount(ctx context.Context, subjectURI string) error {
	return s.ScyllaSession.Query(`
		UPDATE like_counts
		SET count = count - 1
		WHERE subject_uri = ?
	`, subjectURI).WithContext(ctx).Exec()
}

// GetLikeCount returns the like count for a given subject URI
func (s *Store) GetLikeCount(ctx context.Context, subjectURI string) (*LikeCount, error) {
	var likeCount LikeCount
	if err := s.ScyllaSession.Query(`
		SELECT subject_uri, count
		FROM like_counts
		WHERE subject_uri = ?
	`, subjectURI).WithContext(ctx).Scan(&likeCount); err != nil {
		return nil, err
	}
	return &likeCount, nil
}

// DeleteLikeCount deletes the like count for a given subject URI
func (s *Store) DeleteLikeCount(ctx context.Context, subjectURI string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM like_counts
		WHERE subject_uri = ?
	`, subjectURI).WithContext(ctx).Exec()
}
