package endpoints

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/search"
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
	TotalUsers          int                 `json:"total_users"`
	TotalAuthors        int64               `json:"total_authors"`
	TotalPosts          int64               `json:"total_posts"`
	MeanPostCount       float64             `json:"mean_post_count"`
	Percentiles         []search.Percentile `json:"percentiles"`
	FollowerPercentiles []StatPercentile    `json:"follower_percentiles"`
	Brackets            []search.Bracket    `json:"brackets"`
	UpdatedAt           time.Time           `json:"updated_at"`
	DailyData           []DailyDatapoint    `json:"daily_data"`
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

	authorStats, err := api.PostRegistry.GetAuthorStats(ctx)
	if err != nil {
		log.Printf("Error getting author stats: %v", err)
		return fmt.Errorf("error getting author stats: %w", err)
	}

	if authorStats == nil {
		log.Printf("Author stats returned nil")
		return errors.New("author stats returned nil")
	}

	// Get usercount from UserCount service
	userCount, err := api.UserCount.GetUserCount(ctx)
	if err != nil {
		log.Printf("Error getting user count: %v", err)
		return fmt.Errorf("error getting user count: %w", err)
	}

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

	// Update the metrics
	totalUsers.Set(float64(userCount))
	totalAuthors.Set(float64(authorStats.TotalAuthors))
	meanPostCount.Set(authorStats.MeanPostCount)
	totalPostCount.Set(float64(authorStats.TotalPosts))

	// Lock the stats mux for writing
	span.AddEvent("RefreshSiteStats:AcquireStatsCacheWLock")
	api.StatsCacheRWMux.Lock()
	span.AddEvent("RefreshSiteStats:StatsCacheWLockAcquired")
	// Update the plain old struct cache
	api.StatsCache = &StatsCacheEntry{
		Stats: AuthorStatsResponse{
			TotalUsers:          userCount,
			TotalAuthors:        authorStats.TotalAuthors,
			TotalPosts:          authorStats.TotalPosts,
			MeanPostCount:       authorStats.MeanPostCount,
			Percentiles:         authorStats.Percentiles,
			FollowerPercentiles: followerPercentiles,
			Brackets:            authorStats.Brackets,
			UpdatedAt:           authorStats.UpdatedAt,
			DailyData:           dailyDatapoints,
		},
		Expiration: time.Now().Add(api.StatsCacheTTL),
	}

	// Unlock the stats mux for writing
	span.AddEvent("RefreshSiteStats:ReleaseStatsCacheWLock")
	api.StatsCacheRWMux.Unlock()

	return nil
}

type HourlyLikersResponse struct {
	HourlyLikers int64 `json:"hourly_likers"`
}

func (api *API) GetHourlyLikers(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetHourlyLikers")
	defer span.End()

	// Get the hourly likers count
	hourlyLikeBMKey := fmt.Sprintf("likes_hourly:%s", time.Now().Format("2006_01_02_15"))
	bm, err := api.Bitmapper.GetBitmap(ctx, hourlyLikeBMKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get hourly likers count"})
		return
	}

	hourlyLikers := int64(bm.GetCardinality())

	c.JSON(http.StatusOK, HourlyLikersResponse{
		HourlyLikers: hourlyLikers,
	})
}

type MonthlyLikersResponse struct {
	MonthlyLikers int64 `json:"monthly_likers"`
}

func (api *API) GetMonthlyLikers(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetMonthlyLikers")
	defer span.End()

	// Generate keys for all hours in the last 30 days
	monthlyLikeBMKeys := []string{}
	for i := 0; i < 30; i++ {
		for j := 0; j < 24; j++ {
			monthlyLikeBMKeys = append(monthlyLikeBMKeys, fmt.Sprintf("likes_hourly:%s", time.Now().AddDate(0, 0, -i).Add(-time.Duration(j)*time.Hour).Format("2006_01_02_15")))
		}
	}

	// Get the union of all the hourly likers bitmaps
	bm, err := api.Bitmapper.GetUnion(ctx, monthlyLikeBMKeys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get monthly likers count"})
		return
	}

	monthlyLikers := int64(bm.GetCardinality())

	c.JSON(http.StatusOK, MonthlyLikersResponse{
		MonthlyLikers: monthlyLikers,
	})
}
