package store

import "context"

// CREATE TABLE labels (
//     id uuid PRIMARY KEY,
//     lookup_alias text,
//     display_name text,
//     description text,
//     inserted_at timestamp,
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// CREATE TABLE label_assignments (
//     uri text,
//     label text,
//     inserted_at timestamp,
//     PRIMARY KEY (uri, label)
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Label represents a label in the database
type Label struct {
	ID          string
	LookupAlias string
	DisplayName string
	Description string
	InsertedAt  int64
}

// CreateLabel creates a new label in the database
func (s *Store) CreateLabel(ctx context.Context, label *Label) error {
	return s.ScyllaSession.Query(`
		INSERT INTO labels (
			id,
			lookup_alias,
			display_name,
			description,
			inserted_at
		) VALUES (?, ?, ?, ?, ?)
	`, label.ID, label.LookupAlias, label.DisplayName, label.Description, label.InsertedAt).WithContext(ctx).Exec()
}

// GetLabel gets a label from the database
func (s *Store) GetLabel(ctx context.Context, id string) (*Label, error) {
	var label Label
	err := s.ScyllaSession.Query(`
		SELECT id,
			lookup_alias,
			display_name,
			description,
			inserted_at
		FROM labels
		WHERE id = ?
	`, id).WithContext(ctx).Scan(&label.ID, &label.LookupAlias, &label.DisplayName, &label.Description, &label.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &label, nil
}

// DeleteLabel deletes a label from the database
func (s *Store) DeleteLabel(ctx context.Context, id string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM labels
		WHERE id = ?
	`, id).WithContext(ctx).Exec()
}

// GetLabels gets labels from the database
func (s *Store) GetLabels(ctx context.Context) ([]*Label, error) {
	var labels []*Label
	err := s.ScyllaSession.Query(`
		SELECT id,
			lookup_alias,
			display_name,
			description,
			inserted_at
		FROM labels
	`).WithContext(ctx).Scan(&labels)
	if err != nil {
		return nil, err
	}

	return labels, nil
}

// LabelAssignment represents a label assignment in the database
type LabelAssignment struct {
	URI        string
	Label      string
	InsertedAt int64
}

// CreateLabelAssignment creates a new label assignment in the database
func (s *Store) CreateLabelAssignment(ctx context.Context, labelAssignment *LabelAssignment) error {
	return s.ScyllaSession.Query(`
		INSERT INTO label_assignments (
			uri,
			label,
			inserted_at
		) VALUES (?, ?, ?)
	`, labelAssignment.URI, labelAssignment.Label, labelAssignment.InsertedAt).WithContext(ctx).Exec()
}

// GetLabelAssignment gets a label assignment from the database
func (s *Store) GetLabelAssignment(ctx context.Context, uri, label string) (*LabelAssignment, error) {
	var labelAssignment LabelAssignment
	err := s.ScyllaSession.Query(`
		SELECT uri,
			label,
			inserted_at
		FROM label_assignments
		WHERE uri = ? AND label = ?
	`, uri, label).WithContext(ctx).Scan(&labelAssignment.URI, &labelAssignment.Label, &labelAssignment.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &labelAssignment, nil
}

// DeleteLabelAssignment deletes a label assignment from the database
func (s *Store) DeleteLabelAssignment(ctx context.Context, uri, label string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM label_assignments
		WHERE uri = ? AND label = ?
	`, uri, label).WithContext(ctx).Exec()
}

// GetLabelAssignmentsByURI gets label assignments by uri from the database
func (s *Store) GetLabelAssignmentsByURI(ctx context.Context, uri string) ([]*LabelAssignment, error) {
	var labelAssignments []*LabelAssignment
	err := s.ScyllaSession.Query(`
		SELECT uri,
			label,
			inserted_at
		FROM label_assignments
		WHERE uri = ?
	`, uri).WithContext(ctx).Scan(&labelAssignments)
	if err != nil {
		return nil, err
	}

	return labelAssignments, nil
}
