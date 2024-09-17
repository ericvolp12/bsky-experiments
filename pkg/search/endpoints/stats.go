package endpoints

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type StatsCacheEntry struct {
	Stats      AuthorStatsResponse
	Expiration time.Time
}

type DailyDatapoint struct {
	Date                    string `json:"date"`
	LikesPerDay             int64  `json:"num_likes"`
	DailyActiveLikers       int64  `json:"num_likers"`
	DailyActivePosters      int64  `json:"num_posters"`
	PostsPerDay             int64  `json:"num_posts"`
	PostsWithImagesPerDay   int64  `json:"num_posts_with_images"`
	ImagesPerDay            int64  `json:"num_images"`
	ImagesWithAltTextPerDay int64  `json:"num_images_with_alt_text"`
	FirstTimePosters        int64  `json:"num_first_time_posters"`
	FollowsPerDay           int64  `json:"num_follows"`
	DailyActiveFollowers    int64  `json:"num_followers"`
	BlocksPerDay            int64  `json:"num_blocks"`
	DailyActiveBlockers     int64  `json:"num_blockers"`
}

type StatPercentile struct {
	Percentile float64 `json:"percentile"`
	Value      float64 `json:"value"`
}

type AuthorStatsResponse struct {
	TotalUsers          int              `json:"total_users"`
	TotalPosts          int64            `json:"total_posts"`
	TotalFollows        int64            `json:"total_follows"`
	TotalLikes          int64            `json:"total_likes"`
	FollowerPercentiles []StatPercentile `json:"follower_percentiles"`
	UpdatedAt           time.Time        `json:"updated_at"`
	DailyData           []DailyDatapoint `json:"daily_data"`
}

func (api *API) GetAuthorStats(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetAuthorStats")
	defer span.End()

	timeout := 30 * time.Second
	timeWaited := 0 * time.Second
	sleepTime := 100 * time.Millisecond

	// Wait for the stats cache to be populated
	if api.StatsCache == nil {
		span.AddEvent("GetAuthorStats:WaitForStatsCache")
		for api.StatsCache == nil {
			if timeWaited > timeout {
				c.JSON(http.StatusRequestTimeout, gin.H{"error": "timed out waiting for stats cache to populate"})
				return
			}

			time.Sleep(sleepTime)
			timeWaited += sleepTime
		}
	}

	// Lock the stats mux for reading
	span.AddEvent("GetAuthorStats:AcquireStatsCacheRLock")
	api.StatsCacheRWMux.RLock()
	span.AddEvent("GetAuthorStats:StatsCacheRLockAcquired")

	statsFromCache := api.StatsCache.Stats

	// Unlock the stats mux for reading
	span.AddEvent("GetAuthorStats:ReleaseStatsCacheRLock")
	api.StatsCacheRWMux.RUnlock()

	c.JSON(http.StatusOK, statsFromCache)
	return
}

func (api *API) RefreshSiteStats(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "RefreshSiteStats")
	defer span.End()

	// Get usercount from UserCount service
	userCount, err := api.UserCount.GetUserCount(ctx)
	if err != nil {
		log.Printf("Error getting user count: %v", err)
		return fmt.Errorf("error getting user count: %w", err)
	}
	totalUsers.Set(float64(userCount))

	dailyDatapointsRaw, err := api.Store.Queries.GetDailySummaries(ctx)
	if err != nil {
		log.Printf("Error getting daily datapoints: %v", err)
		return fmt.Errorf("error getting daily datapoints: %w", err)
	}

	dailyDatapoints := []DailyDatapoint{}

	for _, datapoint := range dailyDatapointsRaw {
		// Filter out datapoints before 2023-03-01 and after tomorrow
		if datapoint.Date.Before(time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)) || datapoint.Date.After(time.Now().AddDate(0, 0, 1)) {
			continue
		}
		dailyDatapoints = append(dailyDatapoints, DailyDatapoint{
			Date:                    datapoint.Date.UTC().Format("2006-01-02"),
			LikesPerDay:             datapoint.LikesPerDay,
			DailyActiveLikers:       datapoint.DailyActiveLikers,
			DailyActivePosters:      datapoint.DailyActivePosters,
			PostsPerDay:             datapoint.PostsPerDay,
			PostsWithImagesPerDay:   datapoint.PostsWithImagesPerDay,
			ImagesPerDay:            datapoint.ImagesPerDay,
			ImagesWithAltTextPerDay: datapoint.ImagesWithAltTextPerDay,
			FirstTimePosters:        datapoint.FirstTimePosters,
			FollowsPerDay:           datapoint.FollowsPerDay,
			DailyActiveFollowers:    datapoint.DailyActiveFollowers,
			BlocksPerDay:            datapoint.BlocksPerDay,
			DailyActiveBlockers:     datapoint.DailyActiveBlockers,
		})
	}

	// Get Follower percentiles
	followerPercentilesRaw, err := api.Store.Queries.GetFollowerPercentiles(ctx)
	if err != nil {
		log.Printf("Error getting follower percentiles: %v", err)
		return fmt.Errorf("error getting follower percentiles: %w", err)
	}

	followerPercentiles := []StatPercentile{
		{Percentile: 0.25, Value: followerPercentilesRaw.P25},
		{Percentile: 0.5, Value: followerPercentilesRaw.P50},
		{Percentile: 0.75, Value: followerPercentilesRaw.P75},
		{Percentile: 0.9, Value: followerPercentilesRaw.P90},
		{Percentile: 0.95, Value: followerPercentilesRaw.P95},
		{Percentile: 0.99, Value: followerPercentilesRaw.P99},
		{Percentile: 0.995, Value: followerPercentilesRaw.P995},
		{Percentile: 0.997, Value: followerPercentilesRaw.P997},
		{Percentile: 0.999, Value: followerPercentilesRaw.P999},
		{Percentile: 0.9999, Value: followerPercentilesRaw.P9999},
	}

	res, err := api.Store.DB.QueryContext(ctx, "SELECT reltuples::bigint AS num_ents, relname as ent_type FROM pg_class WHERE relname in ('posts', 'follows', 'likes');")
	if err != nil {
		log.Printf("Error getting total posts: %v", err)
		return fmt.Errorf("error getting total posts: %w", err)
	}
	defer res.Close()

	var totalPosts, totalFollows, totalLikes int64
	for res.Next() {
		var numEnts int64
		var entType string
		err = res.Scan(&numEnts, &entType)
		if err != nil {
			log.Printf("Error scanning total posts: %v", err)
			return fmt.Errorf("error scanning total posts: %w", err)
		}
		switch entType {
		case "posts":
			totalPosts = numEnts
		case "follows":
			totalFollows = numEnts
		case "likes":
			totalLikes = numEnts
		}
	}

	// Lock the stats mux for writing
	span.AddEvent("RefreshSiteStats:AcquireStatsCacheWLock")
	api.StatsCacheRWMux.Lock()
	span.AddEvent("RefreshSiteStats:StatsCacheWLockAcquired")
	// Update the plain old struct cache
	api.StatsCache = &StatsCacheEntry{
		Stats: AuthorStatsResponse{
			TotalUsers:          userCount,
			TotalPosts:          totalPosts,
			TotalFollows:        totalFollows,
			TotalLikes:          totalLikes,
			FollowerPercentiles: followerPercentiles,
			DailyData:           dailyDatapoints,
			UpdatedAt:           time.Now(),
		},
		Expiration: time.Now().Add(api.StatsCacheTTL),
	}

	// Unlock the stats mux for writing
	span.AddEvent("RefreshSiteStats:ReleaseStatsCacheWLock")
	api.StatsCacheRWMux.Unlock()

	return nil
}

type StatsPeriod struct {
	LookbackWindow  string `json:"lookback_window"`
	UniqueLikers    int64  `json:"unique_likers"`
	UniquePosters   int64  `json:"unique_posters"`
	UniqueReposters int64  `json:"unique_reposters"`
	UniqueBlockers  int64  `json:"unique_blockers"`
	UniqueFollowers int64  `json:"unique_followers"`
	TotalUniques    int64  `json:"total_uniques"`
}

func (api *API) GetStatsPeriod(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetStatsPeriod")
	defer span.End()

	var err error
	lookbackHours := 24 * 30
	lookbackWindow := 30 * 24 * time.Hour
	lookbackWindowStr := c.Query("lookback_window")
	if lookbackWindowStr != "" {
		lookbackWindow, err = time.ParseDuration(lookbackWindowStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid lookback_window"})
			return
		}
		lookbackHours = int(math.Ceil(lookbackWindow.Hours()))
	}

	likeKeys := []string{}
	postKeys := []string{}
	repostKeys := []string{}
	blockKeys := []string{}
	followKeys := []string{}
	allKeys := []string{}
	for i := 0; i < lookbackHours; i++ {
		hourSuffix := time.Now().Add(-time.Duration(i) * time.Hour).Format("2006_01_02_15")
		likeKeys = append(likeKeys, fmt.Sprintf("likes_hourly:%s", hourSuffix))
		postKeys = append(postKeys, fmt.Sprintf("posts_hourly:%s", hourSuffix))
		repostKeys = append(repostKeys, fmt.Sprintf("reposts_hourly:%s", hourSuffix))
		blockKeys = append(blockKeys, fmt.Sprintf("blocks_hourly:%s", hourSuffix))
		followKeys = append(followKeys, fmt.Sprintf("follows_hourly:%s", hourSuffix))
		allKeys = append(allKeys, likeKeys[i], postKeys[i], repostKeys[i], blockKeys[i], followKeys[i])
	}

	// Get the union of all the hourly likers bitmaps
	likersBM, err := api.Bitmapper.GetUnion(ctx, likeKeys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get monthly likers count"})
		return
	}

	// Get the union of all the hourly posters bitmaps
	postsBM, err := api.Bitmapper.GetUnion(ctx, postKeys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get monthly posters count"})
		return
	}

	// Get the union of all the hourly reposters bitmaps
	repostsBM, err := api.Bitmapper.GetUnion(ctx, repostKeys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get monthly reposters count"})
		return
	}

	// Get the union of all the hourly blockers bitmaps
	blocksBM, err := api.Bitmapper.GetUnion(ctx, blockKeys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get monthly blockers count"})
		return
	}

	// Get the union of all the hourly followers bitmaps
	followsBM, err := api.Bitmapper.GetUnion(ctx, followKeys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get monthly followers count"})
		return
	}

	// Get the total number of unique users
	totalUniquesBM, err := api.Bitmapper.GetUnion(ctx, allKeys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get total uniques count"})
		return
	}

	uniqueLikers := int64(likersBM.GetCardinality())
	uniquePosters := int64(postsBM.GetCardinality())
	uniqueReposters := int64(repostsBM.GetCardinality())
	uniqueBlockers := int64(blocksBM.GetCardinality())
	uniqueFollowers := int64(followsBM.GetCardinality())
	totalUniques := int64(totalUniquesBM.GetCardinality())

	c.JSON(http.StatusOK, StatsPeriod{
		LookbackWindow:  lookbackWindow.String(),
		UniqueLikers:    uniqueLikers,
		UniquePosters:   uniquePosters,
		UniqueReposters: uniqueReposters,
		UniqueBlockers:  uniqueBlockers,
		UniqueFollowers: uniqueFollowers,
		TotalUniques:    totalUniques,
	})
}
