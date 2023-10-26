package feeddb

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// PostsPerHour is the number of posts we expect to see per hour
	PostsPerHour = 10 * 60 * 60
	// MaxPosts is the maximum number of posts we expect to see in 2 days, the size of our ring buffer
	MaxPosts = PostsPerHour * 24 * 2
)

var tracer = otel.Tracer("feeddb")

type DB struct {
	// Ring buffer of posts.
	buffer [MaxPosts]*store_queries.RecentPost
	// Current index for the ring buffer.
	index int

	pageSize       int32
	lastInsertedAt time.Time
	lastActorDid   string
	lastRkey       string
	// Map for O(1) post retrieval.
	posts map[string]*store_queries.RecentPost
	mu    sync.RWMutex

	cstore *store.Store
}

func NewDB(cstore *store.Store) *DB {
	return &DB{
		posts:          make(map[string]*store_queries.RecentPost),
		cstore:         cstore,
		pageSize:       50_000,
		lastInsertedAt: time.Now().Add(-48 * time.Hour),
	}
}

func (s *DB) LoadAllPosts(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "LoadAllPosts")
	defer span.End()

	slog.Info("Loading all posts from database into memory")

	pagesLoaded := 0

	for {
		newPosts, err := s.cstore.Queries.GetRecentPostsPageByInsertedAt(ctx, store_queries.GetRecentPostsPageByInsertedAtParams{
			InsertedAt: s.lastInsertedAt,
			Limit:      s.pageSize,
		})

		if err != nil {
			return fmt.Errorf("failed to load posts: %w", err)
		}

		newPostPtrs := []*store_queries.RecentPost{}
		for i := range newPosts {
			newPostPtrs = append(newPostPtrs, &newPosts[i])
		}

		// Add posts to the store.
		s.AddPosts(ctx, newPostPtrs)

		// Update last inserted at.
		if len(newPosts) > 0 {
			s.lastInsertedAt = newPosts[len(newPosts)-1].InsertedAt
			s.lastActorDid = newPosts[len(newPosts)-1].ActorDid
			s.lastRkey = newPosts[len(newPosts)-1].Rkey
		}

		pagesLoaded++
		if pagesLoaded%10 == 0 {
			slog.Info("Loaded posts", "pages", pagesLoaded, "page_size", s.pageSize, "total_posts", len(s.posts))
		}

		if len(newPosts) < int(s.pageSize) {
			break
		}
	}

	slog.Info("Finished loading posts", "pages", pagesLoaded, "page_size", s.pageSize, "total_posts", len(s.posts))

	return nil
}

// LoadNewPosts loads posts from the database into memory
func (s *DB) LoadNewPosts(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "LoadNewPosts")
	defer span.End()

	newPosts, err := s.cstore.Queries.GetRecentPostsPageByInsertedAt(ctx, store_queries.GetRecentPostsPageByInsertedAtParams{
		InsertedAt: s.lastInsertedAt,
		Limit:      s.pageSize,
	})

	if err != nil {
		return fmt.Errorf("failed to load posts: %w", err)
	}

	newPostPtrs := []*store_queries.RecentPost{}
	for _, p := range newPosts {
		newPostPtrs = append(newPostPtrs, &p)
	}

	// Add posts to the store.
	s.AddPosts(ctx, newPostPtrs)

	// Update last inserted at.
	if len(newPosts) > 0 {
		s.lastInsertedAt = newPosts[len(newPosts)-1].InsertedAt
		s.lastActorDid = newPosts[len(newPosts)-1].ActorDid
		s.lastRkey = newPosts[len(newPosts)-1].Rkey
	}

	return nil
}

// AddPosts adds posts to the store
func (s *DB) AddPosts(ctx context.Context, posts []*store_queries.RecentPost) {
	ctx, span := tracer.Start(ctx, "AddPosts")
	defer span.End()

	span.SetAttributes(attribute.Int64("num_posts", int64(len(posts))))

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range posts {
		p := posts[i]
		newKey := fmt.Sprintf("%s_%s", p.ActorDid, p.Rkey)

		if _, ok := s.posts[newKey]; ok {
			continue
		}

		// Remove old post if it's being overwritten in the ring buffer.
		if s.buffer[s.index] != nil {
			key := fmt.Sprintf("%s_%s", s.buffer[s.index].ActorDid, s.buffer[s.index].Rkey)
			delete(s.posts, key)
		}

		// Add post to map and ring buffer.
		s.posts[newKey] = p
		s.buffer[s.index] = p

		// Move index.
		s.index = (s.index + 1) % MaxPosts
	}
}

// CleanupOldPosts removes posts older than 3 days
func (s *DB) CleanupOldPosts(ctx context.Context) int {
	ctx, span := tracer.Start(ctx, "CleanupOldPosts")
	defer span.End()

	postsDeleted := 0

	s.mu.Lock()
	defer s.mu.Unlock()

	threeDaysAgo := time.Now().Add(-72 * time.Hour)
	for _, post := range s.buffer {
		if post != nil && post.InsertedAt.Before(threeDaysAgo) {
			key := fmt.Sprintf("%s_%s", post.ActorDid, post.Rkey)
			delete(s.posts, key)
			postsDeleted++
			post = nil
		}
	}

	span.SetAttributes(attribute.Int64("posts_deleted", int64(postsDeleted)))

	return postsDeleted
}

// GetPost returns a post from the store
func (s *DB) GetPost(ctx context.Context, actorDid, rkey string) (*store_queries.RecentPost, bool) {
	ctx, span := tracer.Start(ctx, "GetPost")
	defer span.End()

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := fmt.Sprintf("%s_%s", actorDid, rkey)
	p, ok := s.posts[key]
	return p, ok
}

// GetPostsMatchingRegex returns posts matching a regex in its content
func (s *DB) GetPostsMatchingRegex(ctx context.Context, re *regexp.Regexp, page, pageSize int) []*store_queries.RecentPost {
	ctx, span := tracer.Start(ctx, "GetPostsMatchingRegex")
	defer span.End()

	span.SetAttributes(attribute.Int64("page", int64(page)))
	span.SetAttributes(attribute.Int64("page_size", int64(pageSize)))
	span.SetAttributes(attribute.String("regex", re.String()))

	tStart := time.Now()
	defer func() {
		queryLatency.WithLabelValues("by_regex").Observe(time.Since(tStart).Seconds())
		queriesPerformed.Inc()
	}()

	workerCount := 20

	s.mu.RLock()
	allPosts := make([]*store_queries.RecentPost, len(s.buffer))
	copy(allPosts, s.buffer[:])
	s.mu.RUnlock()

	var mu sync.Mutex
	matchingPosts := []*store_queries.RecentPost{}

	var wg sync.WaitGroup

	// Create a channel to distribute chunks of posts to workers.
	postsChan := make(chan []*store_queries.RecentPost, workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range postsChan {
				localMatching := []*store_queries.RecentPost{}
				for _, post := range chunk {
					if post != nil && post.Content.Valid && re.MatchString(post.Content.String) {
						localMatching = append(localMatching, post)
					}
				}
				mu.Lock()
				matchingPosts = append(matchingPosts, localMatching...)
				mu.Unlock()
			}
		}()
	}

	// Distribute the posts to the channel in chunks.
	chunkSize := (len(allPosts) + workerCount - 1) / workerCount
	for i := 0; i < len(allPosts); i += chunkSize {
		end := i + chunkSize
		if end > len(allPosts) {
			end = len(allPosts)
		}
		postsChan <- allPosts[i:end]
	}
	close(postsChan)

	wg.Wait()

	filterDone := time.Now()

	// Sort posts by InsertedAt in descending order.
	sort.Slice(matchingPosts, func(i, j int) bool {
		return matchingPosts[i].InsertedAt.After(matchingPosts[j].InsertedAt)
	})

	sortDone := time.Now()

	// Pagination logic.
	start := page * pageSize
	if start > len(matchingPosts) {
		return nil
	}
	end := start + pageSize
	if end > len(matchingPosts) {
		end = len(matchingPosts)
	}

	slog.Info("GetPostsMatchingRegex", "filter_time", filterDone.Sub(tStart), "sort_time", sortDone.Sub(filterDone), "total_time", time.Since(tStart))

	span.SetAttributes(attribute.Int64("num_posts", int64(len(matchingPosts))))
	span.SetAttributes(attribute.Int64("num_posts_returned", int64(end-start)))

	return matchingPosts[start:end]
}

// GetPostsFromAuthors returns posts from a list of author DIDs
func (s *DB) GetPostsFromAuthors(ctx context.Context, authorDIDs []string, page, pageSize int) []*store_queries.RecentPost {
	ctx, span := tracer.Start(ctx, "GetPostsFromAuthors")
	defer span.End()

	span.SetAttributes(attribute.Int64("page", int64(page)))
	span.SetAttributes(attribute.Int64("page_size", int64(pageSize)))
	span.SetAttributes(attribute.Int64("num_authors", int64(len(authorDIDs))))

	tStart := time.Now()
	defer func() {
		queryLatency.WithLabelValues("by_author").Observe(time.Since(tStart).Seconds())
		queriesPerformed.Inc()
	}()

	s.mu.RLock()
	allPosts := make([]*store_queries.RecentPost, len(s.buffer))
	copy(allPosts, s.buffer[:])
	s.mu.RUnlock()

	var matchingPosts []*store_queries.RecentPost

	// Iterate backward from the current position in the ring buffer.
	currentIndex := s.index
	for i := 0; i < len(allPosts); i++ {
		currentIndex = (currentIndex - 1 + len(allPosts)) % len(allPosts)
		post := allPosts[currentIndex]
		if post == nil {
			continue
		}
		if slices.Contains(authorDIDs, post.ActorDid) {
			matchingPosts = append(matchingPosts, post)
		}
		if len(matchingPosts) >= page*pageSize+pageSize {
			break // Break once you have enough results.
		}
	}

	// Reverse the matchingPosts to restore the original order.
	slices.Reverse(matchingPosts)

	// Pagination logic.
	start := page * pageSize
	if start > len(matchingPosts) {
		return nil
	}
	end := start + pageSize
	if end > len(matchingPosts) {
		end = len(matchingPosts)
	}

	span.SetAttributes(attribute.Int64("num_posts", int64(len(matchingPosts))))
	span.SetAttributes(attribute.Int64("num_posts_returned", int64(end-start)))

	slog.Info("GetPostsFromAuthors", "total_time", time.Since(tStart))

	return matchingPosts[start:end]
}
