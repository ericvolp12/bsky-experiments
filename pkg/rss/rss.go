package rss

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/util"
	"github.com/mmcdole/gofeed"
	slogGorm "github.com/orandin/slog-gorm"
	"golang.org/x/sync/semaphore"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Feed struct {
	gorm.Model

	DID     string `gorm:"index"`
	FeedURL string `gorm:"index"`

	LastChecked time.Time
	LastGUID    string
}

type User struct {
	gorm.Model
	DID          string `gorm:"uniqueIndex"`
	AccessToken  string
	RefreshToken string
	RefreshedAt  time.Time
}

type FeedConsumer struct {
	lk       sync.Mutex
	logger   *slog.Logger
	shutdown chan chan struct{}
	DB       *gorm.DB
	dir      identity.Directory
	httpC    *http.Client
}

func NewFeedConsumer(logger *slog.Logger, dbPath string) (*FeedConsumer, error) {
	// MkdirAll the dbPath
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create db path: %w", err)
	}

	gormLogger := slogGorm.New()
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger:         gormLogger,
		TranslateError: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	if err := db.Exec("PRAGMA journal_mode=WAL;").Error; err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}

	// Set Pragma Synchronous to NORMAL
	if err := db.Exec("PRAGMA synchronous=NORMAL;").Error; err != nil {
		return nil, fmt.Errorf("failed to set synchronous: %w", err)
	}

	if err := db.AutoMigrate(&Feed{}, &User{}); err != nil {
		return nil, fmt.Errorf("failed to auto migrate: %w", err)
	}

	base := identity.BaseDirectory{
		PLCURL: identity.DefaultPLCURL,
		HTTPClient: http.Client{
			Timeout: time.Second * 15,
		},
		Resolver: net.Resolver{
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: time.Second * 5}
				return d.DialContext(ctx, network, address)
			},
		},
		TryAuthoritativeDNS: true,
		// primary Bluesky PDS instance only supports HTTP resolution method
		SkipDNSDomainSuffixes: []string{".bsky.social"},
	}
	dir := identity.NewCacheDirectory(&base, 10000, time.Hour*24, time.Minute*2, time.Minute*5)

	return &FeedConsumer{
		logger:   logger,
		shutdown: make(chan chan struct{}),
		DB:       db,
		dir:      &dir,
		httpC:    util.RobustHTTPClient(),
	}, nil
}

func (fc *FeedConsumer) AddFeed(feedURL, did string) (*Feed, error) {
	f := Feed{
		FeedURL: feedURL,
		DID:     did,
	}

	// Add the feed to the database
	if err := fc.DB.Create(&f).Error; err != nil {
		return nil, fmt.Errorf("failed to create feed: %w", err)
	}

	return &f, nil
}

func (fc *FeedConsumer) GetFeed(did string) (*Feed, error) {
	f := &Feed{}
	if err := fc.DB.Where("d_id = ?", did).First(f).Error; err != nil {
		return nil, fmt.Errorf("failed to find feed: %w", err)
	}

	return f, nil
}

func (fc *FeedConsumer) RemoveFeed(did string) error {
	f := &Feed{}
	if err := fc.DB.Where("d_id = ?", did).First(f).Error; err != nil {
		return fmt.Errorf("failed to find feed: %w", err)
	}

	if err := fc.DB.Delete(f).Error; err != nil {
		return fmt.Errorf("failed to delete feed: %w", err)
	}

	return nil
}

func (fc *FeedConsumer) GetFeeds() ([]*Feed, error) {
	feeds := make([]*Feed, 0)
	if err := fc.DB.Find(&feeds).Error; err != nil {
		return nil, fmt.Errorf("failed to get feeds: %w", err)
	}

	return feeds, nil
}

func (fc *FeedConsumer) SaveFeed(feed *Feed) error {
	if err := fc.DB.Save(feed).Error; err != nil {
		return fmt.Errorf("failed to save feed: %w", err)
	}

	return nil
}

func (fc *FeedConsumer) AddUser(ctx context.Context, handle, appPassword string) (*User, error) {
	// Lookup the DID
	h, err := syntax.ParseHandle(handle)
	if err != nil {
		return nil, fmt.Errorf("failed to parse handle: %w", err)
	}

	ident, err := fc.dir.LookupHandle(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup handle: %w", err)
	}

	// Check if the user already exists
	u, err := fc.GetUser(ident.DID.String())
	if err == nil {
		return u, nil
	}

	// Create a bsky client to get the access and refresh tokens
	bskyClient, err := fc.NewBskyClient(ctx, ident.DID.String(), appPassword)
	if err != nil {
		return nil, fmt.Errorf("failed to create bsky client: %w", err)
	}

	// Create the user
	u = &User{
		DID:          ident.DID.String(),
		AccessToken:  bskyClient.xrpcc.Auth.AccessJwt,
		RefreshToken: bskyClient.xrpcc.Auth.RefreshJwt,
		RefreshedAt:  time.Now(),
	}

	// Add the user to the database
	if err := fc.DB.Create(u).Error; err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return u, nil
}

func (fc *FeedConsumer) GetUser(did string) (*User, error) {
	u := &User{}
	if err := fc.DB.Where("d_id = ?", did).First(u).Error; err != nil {
		return nil, fmt.Errorf("failed to find user: %w", err)
	}

	return u, nil
}

func (f *Feed) Update() (*Feed, *gofeed.Feed, error) {
	parser := gofeed.NewParser()

	feed, err := parser.ParseURL(f.FeedURL)
	if err != nil {
		return f, nil, err
	}

	// Update the last checked time
	f.LastChecked = time.Now()

	// Update the last ID
	if len(feed.Items) > 0 && feed.Items[0].GUID != f.LastGUID {
		// Select only the new items
		newItems := make([]*gofeed.Item, 0)
		for _, item := range feed.Items {
			if item.GUID == f.LastGUID {
				break
			}
			newItems = append(newItems, item)
		}

		// Update the last GUID
		f.LastGUID = feed.Items[0].GUID
	} else {
		feed.Items = nil
	}

	return f, feed, nil
}

func (fc *FeedConsumer) UpdateFeeds() error {
	ctx := context.Background()

	maxConcurrency := int64(20)
	sem := semaphore.NewWeighted(maxConcurrency)

	feeds, err := fc.GetFeeds()
	if err != nil {
		return fmt.Errorf("failed to get feeds: %w", err)
	}

	for did := range feeds {
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("failed to acquire semaphore: %w", err)
		}

		feed := feeds[did]
		go func(f *Feed) {
			defer sem.Release(1)
			updatedF, newFeed, err := f.Update()
			if err != nil {
				fc.logger.Error("error updating feed", "err", err, "feed", f.FeedURL, "did", f.DID)
				return
			}

			if err := fc.SaveFeed(updatedF); err != nil {
				fc.logger.Error("error saving feed", "err", err, "feed", f.FeedURL, "did", f.DID)
				return
			}

			if len(newFeed.Items) <= 0 {
				return
			}

			user, err := fc.GetUser(f.DID)
			if err != nil {
				fc.logger.Error("error getting user", "err", err, "did", f.DID)
				return
			}

			bskyClient, err := fc.ResumeClient(ctx, user.DID, user.AccessToken, user.RefreshToken)
			if err != nil {
				fc.logger.Error("error creating bsky client", "err", err, "did", f.DID)
				return
			}

			for _, item := range newFeed.Items {
				log := fc.logger.With("title", item.Title, "link", item.Link, "did", f.DID)
				log.Info("found new item")
				post, err := fc.MarshalPost(ctx, bskyClient, newFeed, item)
				if err != nil {
					log.Error("error marshalling post", "err", err)
					return
				}

				postURI, err := bskyClient.CreatePost(ctx, *post)
				if err != nil {
					log.Error("error creating post", "err", err)
					return
				}
				log.Info("created post", "uri", postURI)
			}
		}(feed)
	}

	if err := sem.Acquire(ctx, maxConcurrency); err != nil {
		return fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	return nil
}

func unescapeXML(s string) string {
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.ReplaceAll(s, "&apos;", "'")
	s = strings.ReplaceAll(s, "&quot;", "\"")
	return s
}

func (fc *FeedConsumer) MarshalPost(ctx context.Context, client *Client, feed *gofeed.Feed, item *gofeed.Item) (*PostArgs, error) {
	// Unescape the Title and Description
	item.Title = unescapeXML(item.Title)
	item.Description = unescapeXML(item.Description)

	// Create the post
	postText := fmt.Sprintf("%s\n%s", item.Title, item.Description)
	if len(postText) > 270 {
		postText = postText[:270] + "..."
	}

	langs := []string{}
	if feed.Language != "" {
		langs = append(langs, feed.Language)
	}

	post := PostArgs{
		Text:      postText,
		Tags:      []string{"skypub"},
		Languages: langs,
	}

	if item.PublishedParsed != nil {
		post.CreatedAt = *item.PublishedParsed
	}

	truncTitle := item.Title
	if len(truncTitle) > 100 {
		truncTitle = truncTitle[:100] + "..."
	}

	truncDesc := item.Description
	if len(truncDesc) > 280 {
		truncDesc = truncDesc[:280] + "..."
	}

	truncLink := item.Link
	if len(truncLink) > 500 {
		truncLink = truncLink[:500] + "..."
	}

	embed := &bsky.FeedPost_Embed{
		EmbedExternal: &bsky.EmbedExternal{
			External: &bsky.EmbedExternal_External{
				Description: truncDesc,
				Title:       truncTitle,
				Uri:         truncLink,
			},
		},
	}

	// Download the image if it exists
	if item.Image != nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, item.Image.URL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := fc.httpC.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to get image: %w", err)
		}
		defer resp.Body.Close()

		ref, err := client.UploadImage(ctx, resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to upload image: %w", err)
		}

		embed.EmbedExternal.External.Thumb = ref
	}

	post.Embed = embed

	return &post, nil

}

func (fc *FeedConsumer) Start() {
	t := time.NewTicker(5 * time.Minute)

	fc.logger.Info("updating feeds on startup")

	err := fc.UpdateFeeds()
	if err != nil {
		fc.logger.Error("error updating feeds on startup", "err", err)
	} else {
		fc.logger.Info("feeds updated on startup")
	}

	for {
		select {
		case s := <-fc.shutdown:
			fc.logger.Info("feed consumer got shutdown signal")
			close(s)
			return
		case <-t.C:
			fc.logger.Info("updating feeds")
			err := fc.UpdateFeeds()
			if err != nil {
				fc.logger.Error("error updating feeds", "err", err)
			} else {
				fc.logger.Info("feeds updated")
			}
		}
	}
}

func (fc *FeedConsumer) Shutdown() {
	fc.logger.Info("shutting down feed consumer")
	s := make(chan struct{})
	fc.shutdown <- s
	<-s
	fc.logger.Info("feed consumer shut down")
}
