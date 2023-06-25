package objectdetection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
)

type ImageMeta struct {
	CID       string    `json:"cid"`
	URL       string    `json:"url"`
	MimeType  string    `json:"mime_type"`
	CreatedAt time.Time `json:"created_at"`
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
	ProcessImages([]*ImageMeta) ([]*ImageResult, error)
}

type ObjectDetectionImpl struct {
	ObjectDetectionServiceHost string
	Client                     *http.Client
}

func NewObjectDetection(objectDetectionServiceHost string) *ObjectDetectionImpl {
	client := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	return &ObjectDetectionImpl{
		ObjectDetectionServiceHost: objectDetectionServiceHost,
		Client:                     &client,
	}
}

func (o *ObjectDetectionImpl) ProcessImages(ctx context.Context, imageMetas []*ImageMeta) ([]*ImageResult, error) {
	tracer := otel.Tracer("ObjectDetection")
	ctx, span := tracer.Start(ctx, "ProcessImages")
	defer span.End()

	url := fmt.Sprintf("%s/detect_objects", o.ObjectDetectionServiceHost)

	reqBody := imageMetas
	jsonReqBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal object-detection request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonReqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create object-detection request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send object-detection request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("object-detection request failed with status code %d", resp.StatusCode)
	}

	var imageResults []*ImageResult
	if err := json.NewDecoder(resp.Body).Decode(&imageResults); err != nil {
		return nil, fmt.Errorf("failed to decode object-detection response body: %w", err)
	}

	return imageResults, nil
}
