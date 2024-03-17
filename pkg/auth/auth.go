package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	es256k "github.com/ericvolp12/jwt-go-secp256k1"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/yawning/secp256k1-voi/secec"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

type KeyCacheEntry struct {
	UserDID   string
	Key       any
	ExpiresAt time.Time
}

// Initialize Prometheus Metrics for cache hits and misses
var cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_hits_total",
	Help: "The total number of cache hits",
}, []string{"cache_type"})

var cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_misses_total",
	Help: "The total number of cache misses",
}, []string{"cache_type"})

var cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsky_cache_size_bytes",
	Help: "The size of the cache in bytes",
}, []string{"cache_type"})

type FeedAuthEntity struct {
	FeedAlias string `json:"feed_alias"`
	APIKey    string `json:"api_key"`
	UserDID   string `json:"user_did"`
}

type Auth struct {
	KeyCache    *lru.ARCCache[string, KeyCacheEntry]
	KeyCacheTTL time.Duration
	ServiceDID  string
	Dir         *identity.CacheDirectory
	// A bit of a hack for small-scope authenticated APIs
	KeyProvider APIKeyProvider
}

var ErrAPIKeyNotFound = fmt.Errorf("API key not found")

type APIKeyProvider interface {
	GetEntityFromAPIKey(ctx context.Context, apiKey string) (*FeedAuthEntity, error)
}

// NewAuth creates a new Auth instance with the given key cache size and TTL
// The PLC Directory URL is also required, as well as the DID of the service
// for JWT audience validation
// The key cache is used to cache the public keys of users for a given TTL
// The PLC Directory URL is used to fetch the public keys of users
// The service DID is used to validate the audience of JWTs
// The HTTP client is used to make requests to the PLC Directory
// A rate limiter is used to limit the number of requests to the PLC Directory
func NewAuth(
	keyCacheSize int,
	keyCacheTTL time.Duration,
	requestsPerSecond int,
	serviceDID string,
	keyProvider APIKeyProvider,
) (*Auth, error) {
	keyCache, err := lru.NewARC[string, KeyCacheEntry](keyCacheSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to create key cache: %v", err)
	}

	// Initialize the HTTP client with OpenTelemetry instrumentation
	client := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	baseDir := identity.BaseDirectory{
		PLCURL:              identity.DefaultPLCURL,
		PLCLimiter:          rate.NewLimiter(rate.Limit(float64(requestsPerSecond)), 1),
		HTTPClient:          client,
		TryAuthoritativeDNS: true,
		// primary Bluesky PDS instance only supports HTTP resolution method
		SkipDNSDomainSuffixes: []string{".bsky.social"},
	}
	dir := identity.NewCacheDirectory(&baseDir, keyCacheSize, keyCacheTTL, time.Minute*2, keyCacheTTL)

	return &Auth{
		KeyCache:    keyCache,
		KeyCacheTTL: keyCacheTTL,
		ServiceDID:  serviceDID,
		Dir:         &dir,
		KeyProvider: keyProvider,
	}, nil
}

func (auth *Auth) GetClaimsFromAuthHeader(ctx context.Context, authHeader string, claims jwt.Claims) error {
	tracer := otel.Tracer("auth")
	ctx, span := tracer.Start(ctx, "Auth:GetClaimsFromAuthHeader")
	defer span.End()

	if authHeader == "" {
		span.End()
		return fmt.Errorf("No Authorization header provided")
	}

	authHeaderParts := strings.Split(authHeader, " ")
	if len(authHeaderParts) != 2 {
		return fmt.Errorf("Invalid Authorization header")
	}

	if authHeaderParts[0] != "Bearer" {
		return fmt.Errorf("Invalid Authorization header (expected Bearer)")
	}

	accessToken := authHeaderParts[1]

	parser := jwt.Parser{
		ValidMethods: []string{es256k.SigningMethodES256K.Alg()},
	}

	token, err := parser.ParseWithClaims(accessToken, claims, func(token *jwt.Token) (interface{}, error) {
		if claims, ok := token.Claims.(*jwt.StandardClaims); ok {
			// Get the user's key from PLC Directory
			userDID := claims.Issuer
			entry, ok := auth.KeyCache.Get(userDID)
			if ok && entry.ExpiresAt.After(time.Now()) {
				cacheHits.WithLabelValues("key").Inc()
				span.SetAttributes(attribute.Bool("caches.keys.hit", true))
				return entry.Key, nil
			}

			cacheMisses.WithLabelValues("key").Inc()
			span.SetAttributes(attribute.Bool("caches.keys.hit", false))

			did, err := syntax.ParseDID(userDID)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse user DID: %v", err)
			}

			// Get the user's key from PLC Directory
			id, err := auth.Dir.LookupDID(ctx, did)
			if err != nil {
				return nil, fmt.Errorf("Failed to lookup user DID: %v", err)
			}

			key, err := id.GetPublicKey("atproto")
			if err != nil {
				return nil, fmt.Errorf("Failed to get user public key: %v", err)
			}

			parsedPubkey, err := secec.NewPublicKey(key.UncompressedBytes())
			if err != nil {
				return nil, fmt.Errorf("Failed to parse user public key: %v", err)
			}

			// Add the ECDSA key to the cache
			auth.KeyCache.Add(userDID, KeyCacheEntry{
				Key:       parsedPubkey,
				ExpiresAt: time.Now().Add(auth.KeyCacheTTL),
			})

			return parsedPubkey, nil
		}

		return nil, fmt.Errorf("Invalid authorization token (failed to parse claims)")
	})

	if err != nil {
		return fmt.Errorf("Failed to parse authorization token: %v", err)
	}

	if !token.Valid {
		return fmt.Errorf("Invalid authorization token")
	}

	return nil
}

func (auth *Auth) AuthenticateGinRequestViaJWT(c *gin.Context) {
	tracer := otel.Tracer("auth")
	ctx, span := tracer.Start(c.Request.Context(), "Auth:AuthenticateGinRequestViaJWT")

	authHeader := c.GetHeader("Authorization")
	if authHeader == "" {
		span.End()
		c.Next()
		return
	}

	claims := jwt.StandardClaims{}

	err := auth.GetClaimsFromAuthHeader(ctx, authHeader, &claims)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": fmt.Errorf("Failed to get claims from auth header: %v", err).Error()})
		span.End()
		c.Abort()
		return
	}

	if claims.Audience != auth.ServiceDID {
		c.JSON(http.StatusUnauthorized, gin.H{"error": fmt.Sprintf("Invalid audience (expected %s)", auth.ServiceDID)})
		c.Abort()
		return
	}

	// Set claims Issuer to context as user DID
	c.Set("user_did", claims.Issuer)
	span.SetAttributes(attribute.String("user.did", claims.Issuer))
	span.End()
	c.Next()
}

// AuthenticateGinRequestViaAPIKey authenticates a Gin request via an API key
// statically configured for the app, this is useful for testing and debugging
// or use-case specific scenarios where a DID is not available.
func (auth *Auth) AuthenticateGinRequestViaAPIKey(c *gin.Context) {
	tracer := otel.Tracer("auth")
	_, span := tracer.Start(c.Request.Context(), "Auth:AuthenticateGinRequestViaAPIKey")
	defer span.End()

	keyFromHeader := c.GetHeader("X-API-Key")
	if keyFromHeader == "" {
		span.SetAttributes(attribute.Bool("auth.api_key", false))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing required API key in X-API-Key header"})
		c.Abort()
		return
	}

	authEntity, err := auth.KeyProvider.GetEntityFromAPIKey(c.Request.Context(), keyFromHeader)
	if err == nil {
		span.SetAttributes(attribute.Bool("auth.api_key", true))
		c.Set("feed.auth.entity", authEntity)
		c.Next()
		return
	}

	span.SetAttributes(attribute.Bool("auth.api_key", false))
	span.SetAttributes(attribute.String("auth.api_key.error", err.Error()))
	c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid API key"})
	c.Abort()
	return
}
