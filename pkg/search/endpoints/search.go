package endpoints

import (
	"net/http"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
)

type TypeaheadMatch struct {
	Handle string  `json:"handle"`
	DID    string  `json:"did"`
	Score  float64 `json:"score"`
}

func (api *API) SearchActorTypeAhead(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "SearchActorTypeAhead")
	defer span.End()

	// Get the query and actor DID from the query string
	query := c.Query("query")
	actorDid := c.Query("actorDid")

	span.SetAttributes(
		attribute.String("query", query),
		attribute.String("actorDid", actorDid),
	)

	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "query must be provided"})
		return
	}

	if actorDid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "actorDID must be provided"})
		return
	}

	// Get the typeahead results
	rows, err := api.Store.Queries.GetActorTypeAhead(ctx, store_queries.GetActorTypeAheadParams{
		Query:    query,
		ActorDid: actorDid,
		Limit:    25,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	results := []TypeaheadMatch{}
	for _, row := range rows {
		results = append(results, TypeaheadMatch{
			Handle: row.Handle,
			DID:    row.Did,
			Score:  row.Score,
		})
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}
