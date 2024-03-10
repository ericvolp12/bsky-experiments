package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TagTracker is a service for tracking the use of tags
type TagTracker struct {
	RedisClient *redis.Client
}

// NewTagTracker creates a new TagTracker
func NewTagTracker(redisClient *redis.Client) (*TagTracker, error) {
	return &TagTracker{
		RedisClient: redisClient,
	}, nil
}

var tagUsagePrefix = "consumer:tags"
var userTagPrefix = "consumer:user_tags"

// IncrementTagUseCounts increments the use count of a set of tags in a redis sorted set
func (t *TagTracker) IncrementTagUseCounts(
	ctx context.Context,
	actorDID string,
	tags []string,
) error {
	ctx, span := tracer.Start(ctx, "IncrementTagUseCounts")
	defer span.End()

	for i, tag := range tags {
		// Truncate to 100 characters
		if len(tag) > 100 {
			tags[i] = tag[:100]
		}
	}

	dailySetKey := fmt.Sprintf("%s:%s", tagUsagePrefix, time.Now().Format("2006_01_02"))
	userTagSetKey := fmt.Sprintf("%s:%s:%s", userTagPrefix, actorDID, time.Now().Format("2006_01_02"))

	// Check if the user has already used each tag today
	p := t.RedisClient.Pipeline()

	for _, tag := range tags {
		p.SIsMember(ctx, userTagSetKey, tag)
	}

	res, err := p.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to check user-tag combinations: %+v", err)
	}

	// Increment the use count of each unique user-tag combination
	p = t.RedisClient.Pipeline()

	for i, tag := range tags {
		if res[i].(*redis.BoolCmd).Val() {
			continue
		}

		p.SAdd(ctx, userTagSetKey, tag)
		p.ZIncrBy(ctx, dailySetKey, 1, tag)
	}

	// Set the expiration of the user-tag set to 24 hours (we'll stop bumping the expiration once the date rolls over)
	p.Expire(ctx, userTagSetKey, 24*time.Hour)

	_, err = p.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to increment unique user-tag counts: %+v", err)
	}

	return nil
}
