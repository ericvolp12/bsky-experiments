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
//     PRIMARY KEY ((post_uri, cid))
// ) WITH default_time_to_live = 0
// AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

// Image represents an image in the database
type Image struct {
	CID          string `json:"cid"`
	PostURI      string `json:"post_uri"`
	AltText      string `json:"alt_text"`
	MimeType     string `json:"mime_type"`
	FullsizeURL  string `json:"fullsize_url"`
	ThumbnailURL string `json:"thumbnail_url"`
	InsertedAt   int64  `json:"inserted_at"`
}

// CreateImage creates a new image in the database
func (s *Store) CreateImage(ctx context.Context, image *Image) error {
	ctx, span := tracer.Start(ctx, "CreateImage")
	defer span.End()

	return s.ScyllaSession.Query(`
		INSERT INTO images (cid, post_uri, alt_text, mime_type, fullsize_url, thumbnail_url, inserted_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, image.CID, image.PostURI, image.AltText, image.MimeType, image.FullsizeURL, image.ThumbnailURL, image.InsertedAt).WithContext(ctx).Exec()
}

// GetImagesByPostURI returns all images for a given post URI
func (s *Store) GetImagesByPostURI(ctx context.Context, postURI string) ([]*Image, error) {
	ctx, span := tracer.Start(ctx, "GetImagesByPostURI")
	defer span.End()

	var images []*Image
	if err := s.ScyllaSession.Query(`
		SELECT cid, post_uri, alt_text, mime_type, fullsize_url, thumbnail_url, inserted_at
		FROM images
		WHERE post_uri = ?
	`, postURI).WithContext(ctx).Scan(&images); err != nil {
		return nil, err
	}
	return images, nil
}

// DeleteImage deletes an image from the database
func (s *Store) DeleteImage(ctx context.Context, postURI, cid string) error {
	ctx, span := tracer.Start(ctx, "DeleteImage")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM images
		WHERE post_uri = ? AND cid = ?
	`, postURI, cid).WithContext(ctx).Exec()
}

// DeleteImagesByPostURI deletes all images for a given post URI
func (s *Store) DeleteImagesByPostURI(ctx context.Context, postURI string) error {
	ctx, span := tracer.Start(ctx, "DeleteImagesByPostURI")
	defer span.End()

	return s.ScyllaSession.Query(`
		DELETE FROM images
		WHERE post_uri = ?
	`, postURI).WithContext(ctx).Exec()
}
