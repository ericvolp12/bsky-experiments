package handlers

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/ericvolp12/bsky-experiments/pkg/feeddb"
	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
)

type Handlers struct {
	db *feeddb.DB
}

var tracer = otel.Tracer("feeddb_handlers")

func NewHandlers(db *feeddb.DB) *Handlers {
	return &Handlers{
		db: db,
	}
}

type HealthStatus struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	Message string `json:"msg,omitempty"`
}

func (h *Handlers) Health(c echo.Context) error {
	s := HealthStatus{
		Status:  "ok",
		Version: "0.0.1",
	}

	return c.JSON(200, s)
}

func (h *Handlers) GetPostsPageByRegex(c echo.Context) error {
	ctx, span := tracer.Start(c.Request().Context(), "GetPostsPageByRegex")
	defer span.End()

	limitQ := c.QueryParam("limit")
	limit := 50

	var err error

	if limitQ != "" {
		limit, err = strconv.Atoi(limitQ)
		if err != nil {
			return c.JSON(400, fmt.Errorf("invalid limit: %w", err))
		}
	}

	cursorQ := c.QueryParam("cursor")
	cursor := 0

	if cursorQ != "" {
		cursor, err = strconv.Atoi(cursorQ)
		if err != nil {
			return c.JSON(400, fmt.Errorf("invalid cursor: %w", err))
		}
	}

	regex := c.QueryParam("regex")
	re, err := regexp.Compile(regex)
	if err != nil {
		return c.JSON(400, fmt.Errorf("invalid regex: %w", err))
	}

	posts := h.db.GetPostsMatchingRegex(ctx, re, cursor, limit)
	return c.JSON(200, posts)
}

func (h *Handlers) GetPostsPageByAuthors(c echo.Context) error {
	ctx, span := tracer.Start(c.Request().Context(), "GetPostsPageByRegex")
	defer span.End()

	limitQ := c.QueryParam("limit")
	limit := 50

	var err error

	if limitQ != "" {
		limit, err = strconv.Atoi(limitQ)
		if err != nil {
			return c.JSON(400, fmt.Errorf("invalid limit: %w", err))
		}
	}

	cursorQ := c.QueryParam("cursor")
	cursor := 0

	if cursorQ != "" {
		cursor, err = strconv.Atoi(cursorQ)
		if err != nil {
			return c.JSON(400, fmt.Errorf("invalid cursor: %w", err))
		}
	}

	authorDIDs := c.Request().URL.Query()["authors"]
	if len(authorDIDs) == 0 {
		return c.JSON(400, fmt.Errorf("no authors provided"))
	}

	posts := h.db.GetPostsFromAuthors(ctx, authorDIDs, cursor, limit)
	return c.JSON(200, posts)
}
