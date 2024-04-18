package server

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/bsky-experiments/pkg/graphd"
	graphdGen "github.com/ericvolp12/bsky-experiments/pkg/graphd/gen"

	graphdconnect "github.com/ericvolp12/bsky-experiments/pkg/graphd/gen/graphdconnect"
)

type GraphDServer struct {
	graph *graphd.Graph
}

func NewGraphDServer(graph *graphd.Graph) *GraphDServer {
	return &GraphDServer{
		graph: graph,
	}
}

func (s *GraphDServer) GetHealthStatus(
	ctx context.Context,
	req *connect.Request[graphdGen.GetHealthStatusRequest],
) (*connect.Response[graphdGen.GetHealthStatusResponse], error) {
	resp := connect.NewResponse(&graphdGen.GetHealthStatusResponse{
		Status:  "ok",
		Version: "0.0.1",
	})

	if req.Msg.IncludeStats {
		userCount := s.graph.GetUsercount()
		resp.Msg.UserCount = userCount

		followCount := s.graph.GetFollowcount()
		resp.Msg.FollowCount = followCount
	}

	return resp, nil
}

func (s *GraphDServer) PostFollow(
	ctx context.Context,
	req *connect.Request[graphdGen.PostFollowRequest],
) (*connect.Response[graphdGen.PostFollowResponse], error) {
	actorDid, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", req.Msg.ActorDid, err))
	}

	targetDid, err := syntax.ParseDID(req.Msg.TargetDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid target did %q: %s", req.Msg.TargetDid, err))
	}

	actor := actorDid.String()
	target := targetDid.String()

	actorUID := s.graph.AcquireDID(actor)
	targetUID := s.graph.AcquireDID(target)
	s.graph.AddFollow(actorUID, targetUID)

	return connect.NewResponse(&graphdGen.PostFollowResponse{}), nil
}

func (s *GraphDServer) PostUnfollow(
	ctx context.Context,
	req *connect.Request[graphdGen.PostUnfollowRequest],
) (*connect.Response[graphdGen.PostUnfollowResponse], error) {
	actorDid, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", req.Msg.ActorDid, err))
	}

	targetDid, err := syntax.ParseDID(req.Msg.TargetDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid target did %q: %s", req.Msg.TargetDid, err))
	}

	actor := actorDid.String()
	target := targetDid.String()

	actorUID := s.graph.AcquireDID(actor)
	targetUID := s.graph.AcquireDID(target)
	s.graph.RemoveFollow(actorUID, targetUID)

	return connect.NewResponse(&graphdGen.PostUnfollowResponse{}), nil
}

func (s *GraphDServer) PostFollows(
	ctx context.Context,
	req *connect.Request[graphdGen.PostFollowsRequest],
) (*connect.Response[graphdGen.PostFollowsResponse], error) {
	for _, follow := range req.Msg.Follows {
		actorDid, err := syntax.ParseDID(follow.ActorDid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", follow.ActorDid, err))
		}

		targetDid, err := syntax.ParseDID(follow.TargetDid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid target did %q: %s", follow.TargetDid, err))
		}

		actor := actorDid.String()
		target := targetDid.String()

		actorUID := s.graph.AcquireDID(actor)
		targetUID := s.graph.AcquireDID(target)
		s.graph.AddFollow(actorUID, targetUID)
	}

	return connect.NewResponse(&graphdGen.PostFollowsResponse{}), nil
}

func (s *GraphDServer) PostUnfollows(
	ctx context.Context,
	req *connect.Request[graphdGen.PostUnfollowsRequest],
) (*connect.Response[graphdGen.PostUnfollowsResponse], error) {
	for _, unfollow := range req.Msg.Unfollows {
		actorDid, err := syntax.ParseDID(unfollow.ActorDid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", unfollow.ActorDid, err))
		}

		targetDid, err := syntax.ParseDID(unfollow.TargetDid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid target did %q: %s", unfollow.TargetDid, err))
		}

		actor := actorDid.String()
		target := targetDid.String()

		actorUID := s.graph.AcquireDID(actor)
		targetUID := s.graph.AcquireDID(target)
		s.graph.RemoveFollow(actorUID, targetUID)
	}

	return connect.NewResponse(&graphdGen.PostUnfollowsResponse{}), nil
}

func (s *GraphDServer) GetFollowing(
	ctx context.Context,
	req *connect.Request[graphdGen.GetFollowingRequest],
) (*connect.Response[graphdGen.GetFollowingResponse], error) {
	did, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid did %q: %s", req.Msg.ActorDid, err))
	}

	uid := s.graph.AcquireDID(did.String())
	following, err := s.graph.GetFollowing(uid)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get following: %s", err))
	}

	dids, err := s.graph.GetDIDs(following)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get dids: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetFollowingResponse{
		Dids: dids,
	}), nil
}

func (s *GraphDServer) GetFollowers(
	ctx context.Context,
	req *connect.Request[graphdGen.GetFollowersRequest],
) (*connect.Response[graphdGen.GetFollowersResponse], error) {
	did, err := syntax.ParseDID(req.Msg.TargetDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid did %q: %s", req.Msg.TargetDid, err))
	}

	uid := s.graph.AcquireDID(did.String())
	followers, err := s.graph.GetFollowers(uid)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get followers: %s", err))
	}

	dids, err := s.graph.GetDIDs(followers)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get dids: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetFollowersResponse{
		Dids: dids,
	}), nil
}

func (s *GraphDServer) GetFollowersNotFollowing(
	ctx context.Context,
	req *connect.Request[graphdGen.GetFollowersNotFollowingRequest],
) (*connect.Response[graphdGen.GetFollowersNotFollowingResponse], error) {
	actorDid, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", req.Msg.ActorDid, err))
	}

	actorUID := s.graph.AcquireDID(actorDid.String())
	notFollowing, err := s.graph.GetFollowersNotFollowing(actorUID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get followers not following: %s", err))
	}

	dids, err := s.graph.GetDIDs(notFollowing)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get dids: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetFollowersNotFollowingResponse{
		Dids: dids,
	}), nil
}

func (s *GraphDServer) GetDoesFollow(
	ctx context.Context,
	req *connect.Request[graphdGen.GetDoesFollowRequest],
) (*connect.Response[graphdGen.GetDoesFollowResponse], error) {
	actorDid, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", req.Msg.ActorDid, err))
	}

	targetDid, err := syntax.ParseDID(req.Msg.TargetDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid target did %q: %s", req.Msg.TargetDid, err))
	}

	actorUID := s.graph.AcquireDID(actorDid.String())
	targetUID := s.graph.AcquireDID(targetDid.String())
	doesFollow, err := s.graph.DoesFollow(actorUID, targetUID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get does follow: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetDoesFollowResponse{
		DoesFollow: doesFollow,
	}), nil
}

func (s *GraphDServer) GetAreMoots(
	ctx context.Context,
	req *connect.Request[graphdGen.GetAreMootsRequest],
) (*connect.Response[graphdGen.GetAreMootsResponse], error) {
	actorDid, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", req.Msg.ActorDid, err))
	}

	targetDid, err := syntax.ParseDID(req.Msg.TargetDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid target did %q: %s", req.Msg.TargetDid, err))
	}

	actorUID := s.graph.AcquireDID(actorDid.String())
	targetUID := s.graph.AcquireDID(targetDid.String())

	aFollowsB, err := s.graph.DoesFollow(actorUID, targetUID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get does follow: %s", err))
	}

	bFollowsA, err := s.graph.DoesFollow(targetUID, actorUID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get does follow: %s", err))
	}

	areMoots := aFollowsB && bFollowsA

	return connect.NewResponse(&graphdGen.GetAreMootsResponse{
		AreMoots: areMoots,
	}), nil
}

func (s *GraphDServer) GetIntersectFollowers(
	ctx context.Context,
	req *connect.Request[graphdGen.GetIntersectFollowersRequest],
) (*connect.Response[graphdGen.GetIntersectFollowersResponse], error) {
	uids := make([]uint32, 0, len(req.Msg.TargetDids))
	for _, did := range req.Msg.TargetDids {
		d, err := syntax.ParseDID(did)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid did %q: %s", did, err))
		}
		uids = append(uids, s.graph.AcquireDID(d.String()))
	}

	intersect, err := s.graph.IntersectFollowers(uids)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get intersect followers: %s", err))
	}

	dids, err := s.graph.GetDIDs(intersect)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get dids: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetIntersectFollowersResponse{
		ActorDids: dids,
	}), nil
}

func (s *GraphDServer) GetIntersectFollowing(
	ctx context.Context,
	req *connect.Request[graphdGen.GetIntersectFollowingRequest],
) (*connect.Response[graphdGen.GetIntersectFollowingResponse], error) {
	uids := make([]uint32, 0, len(req.Msg.ActorDids))
	for _, did := range req.Msg.ActorDids {
		d, err := syntax.ParseDID(did)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid did %q: %s", did, err))
		}
		uids = append(uids, s.graph.AcquireDID(d.String()))
	}

	intersect, err := s.graph.IntersectFollowing(uids)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get intersect following: %s", err))
	}

	dids, err := s.graph.GetDIDs(intersect)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get dids: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetIntersectFollowingResponse{
		Dids: dids,
	}), nil
}

func (s *GraphDServer) GetFollowsFollowing(
	ctx context.Context,
	req *connect.Request[graphdGen.GetFollowsFollowingRequest],
) (*connect.Response[graphdGen.GetFollowsFollowingResponse], error) {
	actorDid, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", req.Msg.ActorDid, err))
	}

	targetDid, err := syntax.ParseDID(req.Msg.TargetDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid target did %q: %s", req.Msg.TargetDid, err))
	}

	actorUID := s.graph.AcquireDID(actorDid.String())
	targetUID := s.graph.AcquireDID(targetDid.String())

	uids, err := s.graph.IntersectFollowingAndFollowers(actorUID, targetUID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get follows following: %s", err))
	}

	dids, err := s.graph.GetDIDs(uids)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get dids: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetFollowsFollowingResponse{
		Dids: dids,
	}), nil
}

func (s *GraphDServer) GetMoots(
	ctx context.Context,
	req *connect.Request[graphdGen.GetMootsRequest],
) (*connect.Response[graphdGen.GetMootsResponse], error) {
	actorDid, err := syntax.ParseDID(req.Msg.ActorDid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid actor did %q: %s", req.Msg.ActorDid, err))
	}

	actorUID := s.graph.AcquireDID(actorDid.String())
	moots, err := s.graph.GetMoots(actorUID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("failed to get moots: %s", err))
	}

	dids, err := s.graph.GetDIDs(moots)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get dids: %s", err))
	}

	return connect.NewResponse(&graphdGen.GetMootsResponse{
		Dids: dids,
	}), nil
}

func main() {
	server := &GraphDServer{}
	mux := http.NewServeMux()
	path, handler := graphdconnect.NewGraphDServiceHandler(server)
	mux.Handle(path, handler)
	http.ListenAndServe(
		"localhost:1323",
		// Use h2c so we can serve HTTP/2 without TLS.
		h2c.NewHandler(mux, &http2.Server{}),
	)
}
