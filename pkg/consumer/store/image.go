package store

import "context"

// CREATE table images (
//     cid text,
//     post_uri text,
//     alt_text text,
//     mime_type text,
//     fullsize_url text,
//     thumbnail_url text,
//     inserted_at timestamp,
//     PRIMARY KEY (cid, post_uri)
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Image represents an image in the database
type Image struct {
	CID          string
	PostURI      string
	AltText      string
	MimeType     string
	FullsizeURL  string
	ThumbnailURL string
	InsertedAt   int64
}

// CreateImage creates a new image in the database
func (s *Store) CreateImage(ctx context.Context, image *Image) error {
	return s.ScyllaSession.Query(`
		INSERT INTO images (
			cid,
			post_uri,
			alt_text,
			mime_type,
			fullsize_url,
			thumbnail_url,
			inserted_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, image.CID, image.PostURI, image.AltText, image.MimeType, image.FullsizeURL, image.ThumbnailURL, image.InsertedAt).WithContext(ctx).Exec()
}

// GetImage gets an image from the database
func (s *Store) GetImage(ctx context.Context, cid string, postURI string) (*Image, error) {
	var image Image
	err := s.ScyllaSession.Query(`
		SELECT cid,
			post_uri,
			alt_text,
			mime_type,
			fullsize_url,
			thumbnail_url,
			inserted_at
		FROM images
		WHERE cid = ? AND post_uri = ?
	`, cid, postURI).WithContext(ctx).Scan(&image.CID, &image.PostURI, &image.AltText, &image.MimeType, &image.FullsizeURL, &image.ThumbnailURL, &image.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &image, nil
}

// GetImagesByPostURI gets images by post uri from the database
func (s *Store) GetImagesByPostURI(ctx context.Context, postURI string) ([]*Image, error) {
	var images []*Image
	err := s.ScyllaSession.Query(`
		SELECT cid,
			post_uri,
			alt_text,
			mime_type,
			fullsize_url,
			thumbnail_url,
			inserted_at
		FROM images
		WHERE post_uri = ?
	`, postURI).WithContext(ctx).Scan(&images)
	if err != nil {
		return nil, err
	}

	return images, nil
}

// DeleteImage deletes an image from the database
func (s *Store) DeleteImage(ctx context.Context, cid string, postURI string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM images
		WHERE cid = ? AND post_uri = ?
	`, cid, postURI).WithContext(ctx).Exec()
}

// DeleteImagesByPostURI deletes images by post uri from the database
func (s *Store) DeleteImagesByPostURI(ctx context.Context, postURI string) error {
	return s.ScyllaSession.Query(`
		DELETE FROM images
		WHERE post_uri = ?
	`, postURI).WithContext(ctx).Exec()
}
