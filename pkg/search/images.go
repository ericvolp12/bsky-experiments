package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq" // postgres driver
	"github.com/sqlc-dev/pqtype"

	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type Image struct {
	CID          string           `json:"cid"`
	PostID       string           `json:"post_id"`
	AuthorDID    string           `json:"author_did"`
	AltText      *string          `json:"alt_text"`
	MimeType     string           `json:"mime_type"`
	FullsizeURL  string           `json:"fullsize_url"`
	ThumbnailURL string           `json:"thumbnail_url"`
	CreatedAt    time.Time        `json:"created_at"`
	CVCompleted  bool             `json:"cv_completed"`
	CVRunAt      *time.Time       `json:"cv_run_at"`
	CVClasses    *json.RawMessage `json:"cv_classes"`
}

func (pr *PostRegistry) AddImage(ctx context.Context, image *Image) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddImage")
	defer span.End()

	altText := sql.NullString{
		String: "",
		Valid:  false,
	}

	if image.AltText != nil {
		altText.String = *image.AltText
		altText.Valid = true
	}

	cvRunAt := sql.NullTime{
		Time:  time.Time{},
		Valid: false,
	}

	if image.CVRunAt != nil {
		cvRunAt.Time = *image.CVRunAt
		cvRunAt.Valid = true
	}

	cvClasses := pqtype.NullRawMessage{
		RawMessage: nil,
		Valid:      false,
	}

	if image.CVClasses != nil {
		cvClasses.RawMessage = *image.CVClasses
		cvClasses.Valid = true
	}

	err := pr.queries.AddImage(ctx, search_queries.AddImageParams{
		Cid:          image.CID,
		PostID:       image.PostID,
		AuthorDid:    image.AuthorDID,
		AltText:      altText,
		MimeType:     image.MimeType,
		FullsizeUrl:  image.FullsizeURL,
		ThumbnailUrl: image.ThumbnailURL,
		CreatedAt:    image.CreatedAt,
		CvCompleted:  image.CVCompleted,
		CvRunAt:      cvRunAt,
		CvClasses:    cvClasses,
	})

	return err
}

func (pr *PostRegistry) AddCVDataToImage(ctx context.Context, cid string, postID string, cvRunAt time.Time, cvClasses json.RawMessage) error {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:AddCVDataToImage")
	defer span.End()

	image, err := pr.queries.GetImage(ctx, search_queries.GetImageParams{
		Cid:    cid,
		PostID: postID,
	})
	if err != nil {
		return fmt.Errorf("failed to get image for update: %w", err)
	}

	cvClassesNullRawMessage := pqtype.NullRawMessage{
		RawMessage: cvClasses,
		Valid:      true,
	}

	cvRunAtNullTime := sql.NullTime{
		Time:  cvRunAt,
		Valid: true,
	}

	err = pr.queries.UpdateImage(ctx, search_queries.UpdateImageParams{
		Cid:          image.Cid,
		PostID:       image.PostID,
		AuthorDid:    image.AuthorDid,
		AltText:      image.AltText,
		MimeType:     image.MimeType,
		FullsizeUrl:  image.FullsizeUrl,
		ThumbnailUrl: image.ThumbnailUrl,
		CreatedAt:    image.CreatedAt,
		CvCompleted:  true,
		CvRunAt:      cvRunAtNullTime,
		CvClasses:    cvClassesNullRawMessage,
	})

	return err
}

func (pr *PostRegistry) GetImage(ctx context.Context, imageCID string, postID string) (*Image, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetImage")
	defer span.End()

	image, err := pr.queries.GetImage(ctx, search_queries.GetImageParams{
		Cid:    imageCID,
		PostID: postID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("image not found")}
		}
		return nil, err
	}

	var altTextPtr *string
	if image.AltText.Valid {
		altText := image.AltText.String
		altTextPtr = &altText
	}

	var cvRunAtPtr *time.Time
	if image.CvRunAt.Valid {
		cvRunAt := image.CvRunAt.Time
		cvRunAtPtr = &cvRunAt
	}

	var cvClassesPtr *json.RawMessage
	if image.CvClasses.Valid {
		cvClasses := image.CvClasses.RawMessage
		cvClassesPtr = &cvClasses
	}

	return &Image{
		CID:          image.Cid,
		PostID:       image.PostID,
		AuthorDID:    image.AuthorDid,
		AltText:      altTextPtr,
		MimeType:     image.MimeType,
		FullsizeURL:  image.FullsizeUrl,
		ThumbnailURL: image.ThumbnailUrl,
		CreatedAt:    image.CreatedAt,
		CVCompleted:  image.CvCompleted,
		CVRunAt:      cvRunAtPtr,
		CVClasses:    cvClassesPtr,
	}, nil
}

func (pr *PostRegistry) GetImagesForPost(ctx context.Context, postID string) ([]*Image, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetImagesForPost")
	defer span.End()

	span.SetAttributes(attribute.String("post_id", postID))

	images, err := pr.queries.GetImagesForPost(ctx, postID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("images not found")}
		}
		return nil, err
	}

	retImages := make([]*Image, len(images))
	for i, image := range images {
		var altTextPtr *string
		if image.AltText.Valid {
			altText := image.AltText.String
			altTextPtr = &altText
		}

		var cvRunAtPtr *time.Time
		if image.CvRunAt.Valid {
			cvRunAt := image.CvRunAt.Time
			cvRunAtPtr = &cvRunAt
		}

		var cvClassesPtr *json.RawMessage
		if image.CvClasses.Valid {
			cvClasses := image.CvClasses.RawMessage
			cvClassesPtr = &cvClasses
		}

		retImages[i] = &Image{
			CID:          image.Cid,
			PostID:       image.PostID,
			AuthorDID:    image.AuthorDid,
			AltText:      altTextPtr,
			MimeType:     image.MimeType,
			FullsizeURL:  image.FullsizeUrl,
			ThumbnailURL: image.ThumbnailUrl,
			CreatedAt:    image.CreatedAt,
			CVCompleted:  image.CvCompleted,
			CVRunAt:      cvRunAtPtr,
			CVClasses:    cvClassesPtr,
		}
	}

	return retImages, nil
}

func (pr *PostRegistry) GetUnprocessedImages(ctx context.Context, limit int32) ([]*Image, error) {
	tracer := otel.Tracer("post-registry")
	ctx, span := tracer.Start(ctx, "PostRegistry:GetUnprocessedImages")
	defer span.End()

	images, err := pr.queries.GetUnprocessedImages(ctx, limit)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundError{fmt.Errorf("images not found")}
		}
		return nil, err
	}

	retImages := make([]*Image, len(images))
	for i, image := range images {
		var altTextPtr *string
		if image.AltText.Valid {
			altText := image.AltText.String
			altTextPtr = &altText
		}

		var cvRunAtPtr *time.Time
		if image.CvRunAt.Valid {
			cvRunAt := image.CvRunAt.Time
			cvRunAtPtr = &cvRunAt
		}

		var cvClassesPtr *json.RawMessage
		if image.CvClasses.Valid {
			cvClasses := image.CvClasses.RawMessage
			cvClassesPtr = &cvClasses
		}

		retImages[i] = &Image{
			CID:          image.Cid,
			PostID:       image.PostID,
			AuthorDID:    image.AuthorDid,
			AltText:      altTextPtr,
			MimeType:     image.MimeType,
			FullsizeURL:  image.FullsizeUrl,
			ThumbnailURL: image.ThumbnailUrl,
			CreatedAt:    image.CreatedAt,
			CVCompleted:  image.CvCompleted,
			CVRunAt:      cvRunAtPtr,
			CVClasses:    cvClassesPtr,
		}
	}

	return retImages, nil
}
