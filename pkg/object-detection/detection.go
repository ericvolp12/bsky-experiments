package objectdetection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

type ImageMeta struct {
	PostID    string    `json:"post_id"`
	ActorDID  string    `json:"actor_did"`
	CID       string    `json:"cid"`
	MimeType  string    `json:"mime_type"`
	CreatedAt time.Time `json:"created_at"`
	Data      string    `json:"data"`
}

type DetectionResult struct {
	Label      string    `json:"label"`
	Confidence float64   `json:"confidence"`
	Box        []float64 `json:"box"`
}

type ImageResult struct {
	Meta    ImageMeta         `json:"meta"`
	Results []DetectionResult `json:"results"`
}

type ImageProcessor interface {
	SubmitImages(ctx context.Context, imageMetas []*ImageMeta) error
	ReapResults(ctx context.Context, count int64) ([]*ImageResult, error)
}

type ObjectDetectionImpl struct {
	RedisClient  *redis.Client
	WorkStream   string
	ResultStream string
}

func NewObjectDetection(redisAddr, workStream, resultStream string) *ObjectDetectionImpl {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr, // e.g., "localhost:6379"
	})

	return &ObjectDetectionImpl{
		RedisClient:  rdb,
		WorkStream:   workStream,
		ResultStream: resultStream,
	}
}

// SubmitImages submits image metadata to the Redis work stream
func (o *ObjectDetectionImpl) SubmitImages(ctx context.Context, imageMetas []*ImageMeta) error {
	tracer := otel.Tracer("ObjectDetection")
	ctx, span := tracer.Start(ctx, "SubmitImages")
	defer span.End()

	for _, imgMeta := range imageMetas {
		jsonImgMeta, err := json.Marshal(imgMeta)
		if err != nil {
			return fmt.Errorf("failed to marshal ImageMeta: %w", err)
		}
		_, err = o.RedisClient.XAdd(ctx, &redis.XAddArgs{
			Stream: o.WorkStream,
			Values: map[string]interface{}{"image_meta": jsonImgMeta},
		}).Result()

		if err != nil {
			return fmt.Errorf("failed to write ImageMeta to Redis stream: %w", err)
		}
	}

	return nil
}

// ReapResults reads processed image results from the Redis result stream
func (o *ObjectDetectionImpl) ReapResults(ctx context.Context, count int64) ([]*ImageResult, error) {
	tracer := otel.Tracer("ObjectDetection")
	ctx, span := tracer.Start(ctx, "ReapResults")
	defer span.End()

	result, err := o.RedisClient.XRead(ctx, &redis.XReadArgs{
		Streams: []string{o.ResultStream, "0"},
		Count:   count,
		Block:   100,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to read from result stream: %w", err)
	}

	var imageResults []*ImageResult
	for _, stream := range result {
		for _, message := range stream.Messages {
			var imgResult ImageResult
			if err := json.Unmarshal([]byte(message.Values["result"].(string)), &imgResult); err != nil {
				return nil, fmt.Errorf("failed to unmarshal ImageResult: %w", err)
			}
			imageResults = append(imageResults, &imgResult)
		}
	}

	return imageResults, nil
}
