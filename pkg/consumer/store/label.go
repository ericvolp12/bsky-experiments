package store

import "context"

// CREATE TABLE labels (
//     alias text,
//     display_name text,
//     description text,
//     inserted_at timestamp,
//     PRIMARY KEY ( alias, inserted_at )
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// CREATE TABLE label_assignments (
//     uri text,
//     label text,
//     inserted_at timestamp,
//     PRIMARY KEY ((uri, label), inserted_at)
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Label represents a label in the database
type Label struct {
	Alias       string `json:"alias"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	InsertedAt  int64  `json:"inserted_at"`
}

// CreateLabel creates a new label in the database
func (s *Store) CreateLabel(ctx context.Context, label *Label) error {
	ctx, span := tracer.Start(ctx, "CreateLabel")
	defer span.End()

	return s.ScyllaSession.Query(`
		INSERT INTO labels (alias, display_name, description, inserted_at)
		VALUES (?, ?, ?, ?)
	`, label.Alias, label.DisplayName, label.Description, label.InsertedAt).WithContext(ctx).Exec()
}

// GetLabelByAlias returns a label for a given alias
func (s *Store) GetLabelByAlias(ctx context.Context, alias string) (*Label, error) {
	ctx, span := tracer.Start(ctx, "GetLabelByAlias")
	defer span.End()

	var label Label
	if err := s.ScyllaSession.Query(`
		SELECT alias, display_name, description, inserted_at
		FROM labels
		WHERE alias = ?
	`, alias).WithContext(ctx).Scan(&label); err != nil {
		return nil, err
	}
	return &label, nil
}

// GetLabelPage returns a page of labels
func (s *Store) GetLabelPage(ctx context.Context, limit int, startAfter string) ([]*Label, error) {
	ctx, span := tracer.Start(ctx, "GetLabelPage")
	defer span.End()

	var labels []*Label
	if err := s.ScyllaSession.Query(`
		SELECT alias, display_name, description, inserted_at
		FROM labels
		WHERE inserted_at > ?
		LIMIT ?
	`, startAfter, limit).WithContext(ctx).Scan(&labels); err != nil {
		return nil, err
	}
	return labels, nil
}

// DeleteLabel deletes a label from the database
func (s *Store) DeleteLabel(ctx context.Context, alias string) error {
	ctx, span := tracer.Start(ctx, "DeleteLabel")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM labels
		WHERE alias = ?
	`, alias).WithContext(ctx).Exec()
}

// LabelAssignment represents a label assignment in the database
type LabelAssignment struct {
	URI        string `json:"uri"`
	Label      string `json:"label"`
	InsertedAt int64  `json:"inserted_at"`
}

// CreateLabelAssignment creates a new label assignment in the database
func (s *Store) CreateLabelAssignment(ctx context.Context, labelAssignment *LabelAssignment) error {
	ctx, span := tracer.Start(ctx, "CreateLabelAssignment")
	defer span.End()

	return s.ScyllaSession.Query(`
		INSERT INTO label_assignments (uri, label, inserted_at)
		VALUES (?, ?, ?)
	`, labelAssignment.URI, labelAssignment.Label, labelAssignment.InsertedAt).WithContext(ctx).Exec()
}

// GetLabelAssignmentsByURI returns all label assignments for a given URI
func (s *Store) GetLabelAssignmentsByURI(ctx context.Context, uri string) ([]*LabelAssignment, error) {
	ctx, span := tracer.Start(ctx, "GetLabelAssignmentsByURI")
	defer span.End()

	var labelAssignments []*LabelAssignment
	if err := s.ScyllaSession.Query(`
		SELECT uri, label, inserted_at
		FROM label_assignments
		WHERE uri = ?
	`, uri).WithContext(ctx).Scan(&labelAssignments); err != nil {
		return nil, err
	}
	return labelAssignments, nil
}

// GetLabelAssignmentsByLabel returns all label assignments for a given label
func (s *Store) GetLabelAssignmentsByLabel(ctx context.Context, label string) ([]*LabelAssignment, error) {
	ctx, span := tracer.Start(ctx, "GetLabelAssignmentsByLabel")
	defer span.End()

	var labelAssignments []*LabelAssignment
	if err := s.ScyllaSession.Query(`
		SELECT uri, label, inserted_at
		FROM label_assignments
		WHERE label = ?
	`, label).WithContext(ctx).Scan(&labelAssignments); err != nil {
		return nil, err
	}
	return labelAssignments, nil
}

// DeleteLabelAssignment deletes a label assignment from the database
func (s *Store) DeleteLabelAssignment(ctx context.Context, uri, label string) error {
	ctx, span := tracer.Start(ctx, "DeleteLabelAssignment")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM label_assignments
		WHERE uri = ? AND label = ?
	`, uri, label).WithContext(ctx).Exec()
}

// DeleteLabelAssignmentsByURI deletes all label assignments for a given URI
func (s *Store) DeleteLabelAssignmentsByURI(ctx context.Context, uri string) error {
	ctx, span := tracer.Start(ctx, "DeleteLabelAssignmentsByURI")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM label_assignments
		WHERE uri = ?
	`, uri).WithContext(ctx).Exec()
}
