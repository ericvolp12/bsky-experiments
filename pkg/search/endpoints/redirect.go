package endpoints

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gin-gonic/gin"
)

func (api *API) RedirectAtURI(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "RedirectAtURI")
	defer span.End()

	query := c.Query("q")

	if strings.HasPrefix(query, "https://") {
		// Turn the bsky URL into an atURI
		u, err := url.Parse(query)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("failed to parse url: %w", err).Error()})
			return
		}

		// Profile: https://bsky.app/profile/{handle_or_did}
		// Post: https://bsky.app/profile/{handle_or_did}/post/{post_rkey}
		// Feed: https://bsky.app/profile/{handle_or_did}/feed/{feed_rkey}
		// List: https://bsky.app/profile/{handle_or_did}/lists/{list_rkey}
		// Starter Pack: https://bsky.app/starter-pack/{handle_or_did}/{starter_pack_rkey}

		// Ignore the host to support URLs from staging environments
		path := strings.TrimPrefix(u.Path, "/")
		parts := strings.Split(path, "/")

		if len(parts) < 2 {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("unsupported url: %s", query).Error()})
			return
		}

		var identString string
		var collection string
		var recordKey string
		switch parts[0] {
		case "profile":
			identString = parts[1]
			if len(parts) > 3 {
				switch parts[2] {
				case "post":
					collection = "app.bsky.feed.post"
				case "feed":
					collection = "app.bsky.feed.generator"
				case "lists":
					collection = "app.bsky.graph.list"
				default:
					c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("unsupported url: %s", query).Error()})
					return
				}
				recordKey = parts[3]
			}
		case "starter-pack":
			if len(parts) < 3 {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("unsupported url: %s", query).Error()})
				return
			}
			identString = parts[1]
			collection = "app.bsky.graph.starterpack"
			recordKey = parts[2]
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("unsupported url: %s", query).Error()})
			return
		}

		identifier, err := syntax.ParseAtIdentifier(identString)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("failed to parse identifier: %w", err).Error()})
			return
		} else if identifier == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("invalid identifier: %s", identString).Error()})
			return
		}

		ident, err := api.Directory.Lookup(ctx, *identifier)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("failed to lookup identity: %w", err).Error()})
			return
		} else if ident == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Errorf("identity not found: %s", identifier.String()).Error()})
			return
		}

		ret := ident.DID.String()
		if collection != "" && recordKey != "" {
			ret = fmt.Sprintf("at://%s/%s/%s", ident.DID.String(), collection, recordKey)
		}
		c.String(http.StatusOK, ret)
		return
	}

	atURI, err := syntax.ParseATURI(query)
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
