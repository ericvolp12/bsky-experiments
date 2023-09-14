package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"golang.org/x/exp/slog"
)

type Graph struct {
	follows   sync.Map
	following sync.Map

	utd   map[uint64]string
	utdLk sync.RWMutex

	dtu   map[string]uint64
	dtuLk sync.RWMutex
}

type FollowMap struct {
	data map[uint64]uint64
	lk   sync.RWMutex
}

func (g *Graph) GetUid(did string) (uint64, bool) {
	g.dtuLk.RLock()
	defer g.dtuLk.RUnlock()
	uid, ok := g.dtu[did]
	return uid, ok
}

func (g *Graph) SetUid(did string, uid uint64) {
	g.dtuLk.Lock()
	defer g.dtuLk.Unlock()
	g.dtu[did] = uid
}

func (g *Graph) GetDid(uid uint64) (string, bool) {
	g.utdLk.RLock()
	defer g.utdLk.RUnlock()
	did, ok := g.utd[uid]
	return did, ok
}

func (g *Graph) GetDids(uids []uint64) ([]string, error) {
	g.utdLk.RLock()
	defer g.utdLk.RUnlock()
	dids := make([]string, len(uids))
	for i, uid := range uids {
		did, ok := g.utd[uid]
		if !ok {
			return nil, fmt.Errorf("uid %d not found", uid)
		}
		dids[i] = did
	}
	return dids, nil
}

func (g *Graph) SetDid(uid uint64, did string) {
	g.utdLk.Lock()
	defer g.utdLk.Unlock()
	g.utd[uid] = did
}

func (g *Graph) AddFollow(actorUID, targetUID uint64) {
	followMap, ok := g.follows.Load(actorUID)
	if !ok {
		followMap = &FollowMap{
			data: map[uint64]uint64{},
		}
		g.follows.Store(actorUID, followMap)
	}
	followMap.(*FollowMap).lk.Lock()
	followMap.(*FollowMap).data[targetUID] = targetUID
	followMap.(*FollowMap).lk.Unlock()

	// Add the follow to the graph
	followMap, ok = g.following.Load(targetUID)
	if !ok {
		followMap = &FollowMap{
			data: map[uint64]uint64{},
		}
		g.following.Store(targetUID, followMap)
	}
	followMap.(*FollowMap).lk.Lock()
	followMap.(*FollowMap).data[actorUID] = actorUID
	followMap.(*FollowMap).lk.Unlock()
}

func (g *Graph) RemoveFollow(actorUID, targetUID uint64) error {
	followMap, ok := g.follows.Load(actorUID)
	if !ok {
		return fmt.Errorf("actor uid %d not found", actorUID)
	}
	followMap.(*FollowMap).lk.Lock()
	delete(followMap.(*FollowMap).data, targetUID)
	followMap.(*FollowMap).lk.Unlock()

	// Remove the follow from the graph
	followMap, ok = g.following.Load(targetUID)
	if !ok {
		return fmt.Errorf("target uid %d not found", targetUID)
	}
	followMap.(*FollowMap).lk.Lock()
	delete(followMap.(*FollowMap).data, actorUID)
	followMap.(*FollowMap).lk.Unlock()

	return nil
}

func (g *Graph) GetFollowers(uid uint64) ([]uint64, error) {
	followMap, ok := g.following.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.(*FollowMap).lk.RLock()
	defer followMap.(*FollowMap).lk.RUnlock()

	followers := make([]uint64, len(followMap.(*FollowMap).data))
	i := 0
	for follower := range followMap.(*FollowMap).data {
		followers[i] = follower
		i++
	}

	return followers, nil
}

func (g *Graph) GetFollowing(uid uint64) ([]uint64, error) {
	followMap, ok := g.follows.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.(*FollowMap).lk.RLock()
	defer followMap.(*FollowMap).lk.RUnlock()

	following := make([]uint64, len(followMap.(*FollowMap).data))
	i := 0
	for follower := range followMap.(*FollowMap).data {
		following[i] = follower
		i++
	}

	return following, nil
}

func (g *Graph) DoesFollow(actorUID, targetUID uint64) (bool, error) {
	followMap, ok := g.follows.Load(actorUID)
	if !ok {
		return false, fmt.Errorf("actor uid %d not found", actorUID)
	}
	followMap.(*FollowMap).lk.RLock()
	defer followMap.(*FollowMap).lk.RUnlock()

	_, ok = followMap.(*FollowMap).data[targetUID]
	return ok, nil
}

func main() {
	//ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout)))

	graph := Graph{
		utd: map[uint64]string{},
		dtu: map[string]uint64{},
	}

	start := time.Now()
	totalFollows := 0

	nextUID := uint64(0)

	// Load all the follows from follows.csv
	f, err := os.Open("follows.csv")
	if err != nil {
		slog.Error("failed to open follows.csv", "err", err)
		return
	}

	fileScanner := bufio.NewScanner(f)

	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		if totalFollows%1_000_000 == 0 {
			slog.Info("loaded follows", "total", totalFollows, "duration", time.Since(start))
		}

		// actorDid,rkey,targetDid,createdAt,insertedAt
		followTxt := fileScanner.Text()
		parts := strings.Split(followTxt, ",")
		actorDID := parts[0]
		targetDID := parts[2]

		actorUID, ok := graph.GetUid(actorDID)
		if !ok {
			actorUID = nextUID
			nextUID++
			graph.SetUid(actorDID, actorUID)
			graph.SetDid(actorUID, actorDID)
		}
		targetUID, ok := graph.GetUid(targetDID)
		if !ok {
			targetUID = nextUID
			nextUID++
			graph.SetUid(targetDID, targetUID)
			graph.SetDid(targetUID, targetDID)
		}

		graph.AddFollow(actorUID, targetUID)

		totalFollows++
	}

	slog.Info("total follows", "total", totalFollows, "duration", time.Since(start))
	// "did:plc:z72i7hdynmk6r22z27h6tvur"

	e := echo.New()
	e.GET("/followers", func(c echo.Context) error {
		did := c.QueryParam("did")
		queryStart := time.Now()

		uid, ok := graph.GetUid(did)
		if !ok {
			slog.Error("uid not found")
			return c.JSON(404, "uid not found")
		}

		uidDone := time.Now()
		followers, err := graph.GetFollowers(uid)
		if err != nil {
			slog.Error("failed to get followers", "err", err)
		}
		membersDone := time.Now()

		// Lookup the dids
		dids, err := graph.GetDids(followers)
		if err != nil {
			slog.Error("failed to get dids", "err", err)
		}

		didsDone := time.Now()
		slog.Info("got followers",
			"followers", len(followers),
			"uid", uid,
			"uidDuration", uidDone.Sub(queryStart),
			"membersDuration", membersDone.Sub(uidDone),
			"didsDuration", didsDone.Sub(membersDone),
			"totalDuration", didsDone.Sub(queryStart),
		)

		return c.JSON(200, dids)
	})

	e.GET("/following", func(c echo.Context) error {
		did := c.QueryParam("did")
		queryStart := time.Now()

		uid, ok := graph.GetUid(did)
		if !ok {
			slog.Error("uid not found")
			return c.JSON(404, "uid not found")
		}

		uidDone := time.Now()
		following, err := graph.GetFollowing(uid)
		if err != nil {
			slog.Error("failed to get following", "err", err)
		}
		membersDone := time.Now()

		// Lookup the dids
		dids, err := graph.GetDids(following)
		if err != nil {
			slog.Error("failed to get dids", "err", err)
		}

		didsDone := time.Now()
		slog.Info("got following",
			"following", len(following),
			"uid", uid,
			"uidDuration", uidDone.Sub(queryStart),
			"membersDuration", membersDone.Sub(uidDone),
			"didsDuration", didsDone.Sub(membersDone),
			"totalDuration", didsDone.Sub(queryStart),
		)

		return c.JSON(200, dids)
	})

	e.GET("/doesFollow", func(c echo.Context) error {
		actorDid := c.QueryParam("actorDid")
		targetDid := c.QueryParam("targetDid")

		start := time.Now()

		actorUID, ok := graph.GetUid(actorDid)
		if !ok {
			slog.Error("actor uid not found")
			return c.JSON(404, "actor uid not found")
		}

		targetUID, ok := graph.GetUid(targetDid)
		if !ok {
			slog.Error("target uid not found")
			return c.JSON(404, "target uid not found")
		}

		doesFollow, err := graph.DoesFollow(actorUID, targetUID)
		if err != nil {
			slog.Error("failed to check if follows", "err", err)
			return c.JSON(500, "failed to check if follows")
		}

		slog.Debug("checked if follows", "doesFollow", doesFollow, "duration", time.Since(start))

		return c.JSON(200, doesFollow)
	})

	e.Logger.Fatal(e.Start(":1323"))
}
