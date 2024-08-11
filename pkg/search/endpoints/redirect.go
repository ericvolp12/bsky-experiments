package endpoints

import (
	"fmt"
	"net/http"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gin-gonic/gin"
)

func (api *API) RedirectAtURI(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "RedirectAtURI")
	defer span.End()

	atURIParam := c.Query("q")
	atURI, err := syntax.ParseATURI(atURIParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("failed to parse atUri: %w", err).Error()})
		return
	}

	switch atURI.Collection() {
	case "":
		c.Redirect(http.StatusFound, fmt.Sprintf("https://bsky.app/profile/%s", atURI.Authority().String()))
		return
	case "app.bsky.feed.post":
		c.Redirect(http.StatusFound,
			fmt.Sprintf(
				"https://bsky.app/profile/%s/post/%s",
				atURI.Authority().String(),
				atURI.RecordKey().String(),
			),
		)
		return
	case "app.bsky.feed.generator":
		c.Redirect(http.StatusFound,
			fmt.Sprintf(
				"https://bsky.app/profile/%s/feed/%s",
				atURI.Authority().String(),
				atURI.RecordKey().String(),
			),
		)
		return
	case "app.bsky.graph.list":
		c.Redirect(http.StatusFound,
			fmt.Sprintf(
				"https://bsky.app/profile/%s/lists/%s",
				atURI.Authority().String(),
				atURI.RecordKey().String(),
			),
		)
		return
	case "app.bsky.graph.starterpack":
		c.Redirect(http.StatusFound,
			fmt.Sprintf(
				"https://bsky.app/starter-pack/%s/%s",
				atURI.Authority().String(),
				atURI.RecordKey().String(),
			),
		)
		return
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("invalid atUri: %s", atURI.String()).Error()})
	}
}
