package endpoints

import (
	"fmt"
	"net/http"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func (api *API) GetClusterForHandle(c *gin.Context) {
	handle := c.Param("handle")
	cluster, err := api.ClusterManager.GetClusterForHandle(c.Request.Context(), handle)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if cluster == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("handle '%s' not found or not assigned to a labeled cluster", handle)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"cluster_id": cluster.ID, "cluster_name": cluster.Name})
}

func (api *API) GetClusterForDID(c *gin.Context) {
	did := c.Param("did")
	cluster, err := api.ClusterManager.GetClusterForDID(c.Request.Context(), did)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if cluster == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("did '%s' not found or not assigned to a labeled cluster", did)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"cluster_id": cluster.ID, "cluster_name": cluster.Name})
}

func (api *API) GetClusterList(c *gin.Context) {
	c.JSON(http.StatusOK, api.ClusterManager.Clusters)
}

type GraphOptRequest struct {
	Username    string `json:"username"`
	AppPassword string `json:"appPassword"`
}

func (api *API) GraphOptOut(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GraphOptOut")
	defer span.End()

	// get Username and appPassword from Post Body
	var optOutRequest GraphOptRequest
	if err := c.ShouldBindJSON(&optOutRequest); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create an instrumented transport for OTEL Tracing of HTTP Requests
	instrumentedTransport := otelhttp.NewTransport(&http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	// Create the XRPC Client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: instrumentedTransport,
		},
		Host: "https://bsky.social",
	}

	// Create a new XRPC Client authenticated as the user
	ses, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: optOutRequest.Username,
		Password:   optOutRequest.AppPassword,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error creating authenticated ATProto session: %w\nYour username and/or AppPassword may be incorrect", err).Error()})
		return
	}

	client.Auth = &xrpc.AuthInfo{
		Handle:     ses.Handle,
		Did:        ses.Did,
		RefreshJwt: ses.RefreshJwt,
		AccessJwt:  ses.AccessJwt,
	}

	// Try to Get the user's profile to confirm that the user is authenticated
	profile, err := bsky.ActorGetProfile(ctx, &client, ses.Handle)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error getting user profile while confirming identity: %w\n"+
			"There may have been a problem communicating with the BSky API, "+
			"as we can't confirm your identity without that info, we are unable to process your opt-out right now.", err).Error()})
		return
	}

	if profile == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to get your profile from the BSky API when confirming your identity, we can't process your opt-out right now."})
		return
	}

	// Create an OptOut record for the user
	err = api.PostRegistry.UpdateAuthorOptOut(ctx, ses.Did, true)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error while updating author opt out record in the Atlas Database: %w\n"+
			"Please feel free to @mention jaz.bsky.social on the Skyline for support for this error, since it's likely an issue with something Jaz can fix.", err).Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "You have successfully opted out of the Atlas"})
}

func (api *API) GraphOptIn(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GraphOptIn")
	defer span.End()

	// get Username and appPassword from Post Body
	var optInRequest GraphOptRequest
	if err := c.ShouldBindJSON(&optInRequest); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create an instrumented transport for OTEL Tracing of HTTP Requests
	instrumentedTransport := otelhttp.NewTransport(&http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	// Create the XRPC Client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: instrumentedTransport,
		},
		Host: "https://bsky.social",
	}

	// Create a new XRPC Client authenticated as the user
	ses, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: optInRequest.Username,
		Password:   optInRequest.AppPassword,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error creating authenticated ATProto session: %w\nYour username and/or AppPassword may be incorrect", err).Error()})
		return
	}

	client.Auth = &xrpc.AuthInfo{
		Handle:     ses.Handle,
		Did:        ses.Did,
		RefreshJwt: ses.RefreshJwt,
		AccessJwt:  ses.AccessJwt,
	}

	// Try to Get the user's profile to confirm that the user is authenticated
	profile, err := bsky.ActorGetProfile(ctx, &client, ses.Handle)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error getting user profile while confirming identity: %w\n"+
			"There may have been a problem communicating with the BSky API, "+
			"as we can't confirm your identity without that info, we are unable to process your opt-in right now.", err).Error()})
		return
	}

	if profile == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to get your profile from the BSky API when confirming your identity, we can't process your opt-in right now."})
		return
	}

	// Set the user's opt-out record to false
	err = api.PostRegistry.UpdateAuthorOptOut(ctx, ses.Did, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error while updating author opt out record in the Atlas Database: %w\n"+
			"Please feel free to @mention jaz.bsky.social on the Skyline for support for this error, since it's likely an issue with something Jaz can fix.", err).Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "You have successfully opted back into the Atlas"})
}

func (api *API) GetOptedOutAuthors(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetOptedOutAuthors")
	defer span.End()

	authors, err := api.PostRegistry.GetOptedOutAuthors(ctx)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error getting opted out authors from the Atlas Database: %w\n"+
			"Please feel free to @mention jaz.bsky.social on the Skyline for support for this error, since it's likely an issue with something Jaz can fix.", err).Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"authors": authors})
}
