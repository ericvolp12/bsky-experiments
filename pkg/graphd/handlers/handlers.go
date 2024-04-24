package handlers

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/bsky-experiments/pkg/graphd"
	"github.com/ericvolp12/bsky-experiments/pkg/graphd/bitmapper"
	"github.com/labstack/echo/v4"
)

type Handlers struct {
	graph *graphd.Graph
}

func NewHandlers(graph *graphd.Graph) *Handlers {
	return &Handlers{
		graph: graph,
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

func (h *Handlers) GetFollowers(c echo.Context) error {
	ctx := c.Request().Context()
	did := c.QueryParam("did")
	uid, _, err := h.graph.GetUID(ctx, did)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "uid not found")
		}
		slog.Error("failed to get uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get uid"))
	}

	followers, err := h.graph.GetFollowers(ctx, uid)
	if err != nil {
		slog.Error("failed to get followers", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get followers"))
	}

	dids, err := h.graph.GetDIDs(ctx, followers)
	if err != nil {
		slog.Error("failed to get dids", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get dids"))
	}

	return c.JSON(200, dids)
}

func (h *Handlers) GetFollowing(c echo.Context) error {
	ctx := c.Request().Context()
	did := c.QueryParam("did")
	uid, _, err := h.graph.GetUID(ctx, did)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "uid not found")
		}
		slog.Error("failed to get uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get uid"))
	}

	following, err := h.graph.GetFollowing(ctx, uid)
	if err != nil {
		slog.Error("failed to get following", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get following"))
	}

	dids, err := h.graph.GetDIDs(ctx, following)
	if err != nil {
		slog.Error("failed to get dids", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get dids"))
	}

	return c.JSON(200, dids)
}

func (h *Handlers) GetMoots(c echo.Context) error {
	ctx := c.Request().Context()
	did := c.QueryParam("did")
	uid, _, err := h.graph.GetUID(ctx, did)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "uid not found")
		}
		slog.Error("failed to get uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get uid"))
	}

	moots, err := h.graph.GetMoots(ctx, uid)
	if err != nil {
		slog.Error("failed to get moots", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get moots"))
	}

	dids, err := h.graph.GetDIDs(ctx, moots)
	if err != nil {
		slog.Error("failed to get dids", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get dids"))
	}

	return c.JSON(200, dids)
}

type DidsResponse struct {
	DIDs []string `json:"dids"`
}

func (h *Handlers) GetFollowersNotFollowing(c echo.Context) error {
	ctx := c.Request().Context()
	did := c.QueryParam("did")
	uid, _, err := h.graph.GetUID(ctx, did)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "uid not found")
		}
		slog.Error("failed to get uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get uid"))
	}

	followersNotFollowing, err := h.graph.GetFollowersNotFollowing(ctx, uid)
	if err != nil {
		slog.Error("failed to get followers not following", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get followers not following"))
	}

	dids, err := h.graph.GetDIDs(ctx, followersNotFollowing)
	if err != nil {
		slog.Error("failed to get dids", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get dids"))
	}

	resp := DidsResponse{
		DIDs: dids,
	}

	return c.JSON(200, resp)
}

func (h *Handlers) GetDoesFollow(c echo.Context) error {
	ctx := c.Request().Context()
	if !c.QueryParams().Has("actor_did") || !c.QueryParams().Has("target_did") {
		return c.JSON(400, "actor_did and target_did query params are required")
	}

	actorDid := c.QueryParam("actor_did")
	targetDid := c.QueryParam("target_did")

	actorUID, _, err := h.graph.GetUID(ctx, actorDid)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "actor uid not found")
		}
		slog.Error("failed to get actor uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get actor uid"))
	}

	targetUID, _, err := h.graph.GetUID(ctx, targetDid)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "target uid not found")
		}
		slog.Error("failed to get target uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get target uid"))
	}

	doesFollow, err := h.graph.DoesFollow(ctx, actorUID, targetUID)
	if err != nil {
		slog.Error("failed to check if follows", "err", err)
		return c.JSON(500, "failed to check if follows")
	}

	return c.JSON(200, doesFollow)
}

func (h *Handlers) GetAreMoots(c echo.Context) error {
	ctx := c.Request().Context()
	if !c.QueryParams().Has("actor_did") || !c.QueryParams().Has("target_did") {
		return c.JSON(400, "actor_did and target_did query params are required")
	}

	actorDid := c.QueryParam("actor_did")
	targetDid := c.QueryParam("target_did")

	actorUID, _, err := h.graph.GetUID(ctx, actorDid)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "actor uid not found")
		}
		slog.Error("failed to get actor uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get actor uid"))
	}

	targetUID, _, err := h.graph.GetUID(ctx, targetDid)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "target uid not found")
		}
		slog.Error("failed to get target uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get target uid"))
	}

	actorFollowsTarget := false
	targetFollowsActor := false

	actorFollowsTarget, err = h.graph.DoesFollow(ctx, actorUID, targetUID)
	if err != nil {
		slog.Error("failed to check follows", "err", err)
		return c.JSON(500, "failed to check follows")
	}

	targetFollowsActor, err = h.graph.DoesFollow(ctx, targetUID, actorUID)
	if err != nil {
		slog.Error("failed to check follows", "err", err)
		return c.JSON(500, "failed to check follows")
	}

	return c.JSON(200, actorFollowsTarget && targetFollowsActor)
}

func (h *Handlers) GetFollowsFollowing(c echo.Context) error {
	ctx := c.Request().Context()
	if !c.QueryParams().Has("actor_did") || !c.QueryParams().Has("target_did") {
		return c.JSON(400, "actor_did and target_did query params are required")
	}

	actorDid := c.QueryParam("actor_did")
	targetDid := c.QueryParam("target_did")

	actorUID, _, err := h.graph.GetUID(ctx, actorDid)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "actor uid not found")
		}
		slog.Error("failed to get actor uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get actor uid"))
	}

	targetUID, _, err := h.graph.GetUID(ctx, targetDid)
	if err != nil {
		if errors.Is(err, bitmapper.ErrorUIDNotFound) {
			return c.JSON(404, "target uid not found")
		}
		slog.Error("failed to get target uid", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get target uid"))
	}

	intersect, err := h.graph.IntersectFollowingAndFollowers(ctx, actorUID, targetUID)
	if err != nil {
		slog.Error("failed to intersect following and followers", "err", err)
		return c.JSON(500, "failed to intersect following and followers")
	}

	dids, err := h.graph.GetDIDs(ctx, intersect)
	if err != nil {
		slog.Error("failed to get dids", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get dids"))
	}

	return c.JSON(200, dids)
}

func (h *Handlers) GetIntersectFollowers(c echo.Context) error {
	ctx := c.Request().Context()
	if !c.QueryParams().Has("did") {
		return c.JSON(400, "did query param is required")
	}
	qDIDs := c.QueryParams()["did"]
	uids := make([]uint32, 0)
	for _, qDID := range qDIDs {
		uid, _, err := h.graph.GetUID(ctx, qDID)
		if err != nil {
			if errors.Is(err, bitmapper.ErrorUIDNotFound) {
				return c.JSON(404, fmt.Sprintf("uid not found for did %s", qDID))
			}
			slog.Error("failed to get uid", "err", err)
			return c.JSON(500, fmt.Errorf("failed to get uid"))
		}
		uids = append(uids, uid)
	}

	intersect, err := h.graph.IntersectFollowers(ctx, uids)
	if err != nil {
		slog.Error("failed to intersect followers", "err", err)
		return c.JSON(500, "failed to intersect followers")
	}

	dids, err := h.graph.GetDIDs(ctx, intersect)
	if err != nil {
		slog.Error("failed to get dids", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get dids"))
	}

	return c.JSON(200, dids)
}

func (h *Handlers) GetIntersectFollowing(c echo.Context) error {
	ctx := c.Request().Context()
	if !c.QueryParams().Has("did") {
		return c.JSON(400, "did query param is required")
	}
	qDIDs := c.QueryParams()["did"]
	uids := make([]uint32, 0)
	for _, qDID := range qDIDs {
		uid, _, err := h.graph.GetUID(ctx, qDID)
		if err != nil {
			if errors.Is(err, bitmapper.ErrorUIDNotFound) {
				return c.JSON(404, fmt.Sprintf("uid not found for did %s", qDID))
			}
			slog.Error("failed to get uid", "err", err)
			return c.JSON(500, fmt.Errorf("failed to get uid"))
		}
		uids = append(uids, uid)
	}

	intersect, err := h.graph.IntersectFollowing(ctx, uids)
	if err != nil {
		slog.Error("failed to intersect following", "err", err)
		return c.JSON(500, "failed to intersect following")
	}

	dids, err := h.graph.GetDIDs(ctx, intersect)
	if err != nil {
		slog.Error("failed to get dids", "err", err)
		return c.JSON(500, fmt.Errorf("failed to get dids"))
	}

	return c.JSON(200, dids)
}

type Follow struct {
	ActorDid  string `json:"actor_did"`
	TargetDid string `json:"target_did"`
}

func (h *Handlers) PostFollow(c echo.Context) error {
	ctx := c.Request().Context()
	var body Follow
	if err := c.Bind(&body); err != nil {
		return c.JSON(400, fmt.Sprintf("invalid body: %s", err))
	}

	actorDid, err := syntax.ParseDID(body.ActorDid)
	if err != nil {
		return c.JSON(400, fmt.Sprintf("invalid actor did %q: %s", body.ActorDid, err))
	}

	targetDid, err := syntax.ParseDID(body.TargetDid)
	if err != nil {
		return c.JSON(400, fmt.Sprintf("invalid target did %q: %s", body.TargetDid, err))
	}

	actor := actorDid.String()
	target := targetDid.String()

	actorUID, _, err := h.graph.AcquireDID(ctx, actor, true)
	if err != nil {
		return c.JSON(500, fmt.Errorf("failed to acquire actor UID: %w", err))
	}

	targetUID, _, err := h.graph.AcquireDID(ctx, target, true)
	if err != nil {
		return c.JSON(500, fmt.Errorf("failed to acquire target UID: %w", err))
	}

	err = h.graph.AddFollow(ctx, actorUID, targetUID, true)
	if err != nil {
		return c.JSON(500, fmt.Errorf("failed to add follow: %w", err))
	}

	return c.JSON(200, "ok")
}

type PostFollowsBody struct {
	Follows []Follow `json:"follows"`
}

func (h *Handlers) PostFollows(c echo.Context) error {
	ctx := c.Request().Context()
	var body PostFollowsBody
	if err := c.Bind(&body); err != nil {
		return c.JSON(400, fmt.Sprintf("invalid body: %s", err))
	}

	// Validate all the DIDs before adding any of them
	for i, follow := range body.Follows {
		_, err := syntax.ParseDID(follow.ActorDid)
		if err != nil {
			return c.JSON(400, fmt.Sprintf("invalid actor did[%d]: %s", i, err))
		}

		_, err = syntax.ParseDID(follow.TargetDid)
		if err != nil {
			return c.JSON(400, fmt.Sprintf("invalid target did[%d]: %s", i, err))
		}
	}

	for _, follow := range body.Follows {
		actorUID, _, err := h.graph.AcquireDID(ctx, follow.ActorDid, true)
		if err != nil {
			return c.JSON(500, fmt.Errorf("failed to acquire actor UID: %w", err))
		}

		targetUID, _, err := h.graph.AcquireDID(ctx, follow.TargetDid, true)
		if err != nil {
			return c.JSON(500, fmt.Errorf("failed to acquire target UID: %w", err))
		}

		err = h.graph.AddFollow(ctx, actorUID, targetUID, true)
		if err != nil {
			return c.JSON(500, fmt.Errorf("failed to add follow: %w", err))
		}
	}

	return c.JSON(200, "ok")
}

type Unfollow struct {
	ActorDid  string `json:"actor_did"`
	TargetDid string `json:"target_did"`
}

func (h *Handlers) PostUnfollow(c echo.Context) error {
	ctx := c.Request().Context()
	var body Unfollow
	if err := c.Bind(&body); err != nil {
		return c.JSON(400, fmt.Sprintf("invalid body: %s", err))
	}

	actorDid, err := syntax.ParseDID(body.ActorDid)
	if err != nil {
		return c.JSON(400, fmt.Sprintf("invalid actor did: %s", err))
	}

	targetDid, err := syntax.ParseDID(body.TargetDid)
	if err != nil {
		return c.JSON(400, fmt.Sprintf("invalid target did: %s", err))
	}

	actor := actorDid.String()
	target := targetDid.String()

	actorUID, _, err := h.graph.AcquireDID(ctx, actor, true)
	if err != nil {
		return c.JSON(500, fmt.Errorf("failed to acquire actor UID: %w", err))
	}

	targetUID, _, err := h.graph.AcquireDID(ctx, target, true)
	if err != nil {
		return c.JSON(500, fmt.Errorf("failed to acquire target UID: %w", err))
	}

	err = h.graph.RemoveFollow(ctx, actorUID, targetUID, true)
	if err != nil {
		return c.JSON(500, fmt.Errorf("failed to remove follow: %w", err))
	}

	return c.JSON(200, "ok")
}

type PostUnfollowsBody struct {
	Unfollows []Unfollow `json:"unfollows"`
}

func (h *Handlers) PostUnfollows(c echo.Context) error {
	ctx := c.Request().Context()
	var body PostUnfollowsBody
	if err := c.Bind(&body); err != nil {
		return c.JSON(400, fmt.Sprintf("invalid body: %s", err))
	}

	// Validate all the DIDs before adding any of them
	for i, unfollow := range body.Unfollows {
		_, err := syntax.ParseDID(unfollow.ActorDid)
		if err != nil {
			return c.JSON(400, fmt.Sprintf("invalid actor did[%d]: %s", i, err))
		}

		_, err = syntax.ParseDID(unfollow.TargetDid)
		if err != nil {
			return c.JSON(400, fmt.Sprintf("invalid target did[%d]: %s", i, err))
		}
	}

	for _, unfollow := range body.Unfollows {
		actorUID, _, err := h.graph.AcquireDID(ctx, unfollow.ActorDid, true)
		if err != nil {
			return c.JSON(500, fmt.Errorf("failed to acquire actor UID: %w", err))
		}

		targetUID, _, err := h.graph.AcquireDID(ctx, unfollow.TargetDid, true)
		if err != nil {
			return c.JSON(500, fmt.Errorf("failed to acquire target UID: %w", err))
		}

		err = h.graph.RemoveFollow(ctx, actorUID, targetUID, true)
		if err != nil {
			return c.JSON(500, fmt.Errorf("failed to remove follow: %w", err))
		}
	}

	return c.JSON(200, "ok")
}
