package main

import (
	"bufio"
	"os"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/slog"
)

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
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.GET("/debug/*", echo.WrapHandler(http.DefaultServeMux))

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
		slog.Debug("got followers",
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
		slog.Debug("got following",
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
