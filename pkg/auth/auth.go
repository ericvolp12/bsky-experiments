package auth

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1"
	es256k "github.com/ericvolp12/jwt-go-secp256k1"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/multiformats/go-multibase"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

type PLCEntry struct {
	Context            []string `json:"@context"`
	ID                 string   `json:"id"`
	AlsoKnownAs        []string `json:"alsoKnownAs"`
	VerificationMethod []struct {
		ID                 string `json:"id"`
		Type               string `json:"type"`
		Controller         string `json:"controller"`
		PublicKeyMultibase string `json:"publicKeyMultibase"`
	} `json:"verificationMethod"`
	Service []struct {
		ID              string `json:"id"`
		Type            string `json:"type"`
		ServiceEndpoint string `json:"serviceEndpoint"`
	} `json:"service"`
}

type KeyCacheEntry struct {
	UserDID   string
	Key       *ecdsa.PublicKey
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
	KeyCache     *lru.ARCCache
	KeyCacheTTL  time.Duration
	HTTPClient   *http.Client
	Limiter      *rate.Limiter
	ServiceDID   string
	PLCDirectory string
	// A bit of a hack for small-scope authenticated APIs
	APIKeyFeedMap map[string]*FeedAuthEntity
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
	plcDirectory string,
	requestsPerSecond int,
	serviceDID string,
) (*Auth, error) {
	keyCache, err := lru.NewARC(keyCacheSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to create key cache: %v", err)
	}

	// Initialize the HTTP client with OpenTelemetry instrumentation
	client := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	timeBetweenRequests := time.Duration(float64(time.Second) / float64(requestsPerSecond))

	// Initialize the rate limiter for PLC Directory requests
	limiter := rate.NewLimiter(rate.Every(timeBetweenRequests), 1)

	return &Auth{
		KeyCache:     keyCache,
		KeyCacheTTL:  keyCacheTTL,
		PLCDirectory: plcDirectory,
		HTTPClient:   &client,
		ServiceDID:   serviceDID,
		Limiter:      limiter,
	}, nil
}

func (auth *Auth) UpdateAPIKeyFeedMapping(feedID string, feedAuthEntity *FeedAuthEntity) {
	auth.APIKeyFeedMap[feedID] = feedAuthEntity
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
			if ok {
				cacheEntry := entry.(KeyCacheEntry)
				if cacheEntry.ExpiresAt.After(time.Now()) {
					if cacheEntry.ExpiresAt.After(time.Now()) {
						cacheHits.WithLabelValues("key").Inc()
						span.SetAttributes(attribute.Bool("caches.keys.hit", true))
						return cacheEntry.Key, nil
					}
				}
			}

			cacheMisses.WithLabelValues("key").Inc()
			span.SetAttributes(attribute.Bool("caches.keys.hit", false))

			// Get the user's key from PLC Directory
			plcEntry, err := auth.GetPLCEntry(ctx, userDID)
			if err != nil {
				return nil, fmt.Errorf("Failed to get PLC Entry: %v", err)
			}

			// Check if the PLC Entry has a verification method
			if len(plcEntry.VerificationMethod) == 0 {
				return nil, fmt.Errorf("No verification method found in PLC Entry")
			}

			// Get the multibase key from the PLC Entry's first verification method
			// TODO: Support multiple verification methods
			multibaseKey := plcEntry.VerificationMethod[0].PublicKeyMultibase

			// Decode the multibase key
			_, decodedMultibaseKey, err := multibase.Decode(multibaseKey)
			if err != nil {
				return nil, fmt.Errorf("Failed to decode multibase key: %v", err)
			}

			// Parse the public key from the decoded multibase key
			pub, err := secp256k1.ParsePubKey(decodedMultibaseKey)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse public key from decoded multibase key: %v", err)
			}

			// Convert the public key to an ECDSA public key
			ecdsaPubKey := pub.ToECDSA()

			// Add the ECDSA key to the cache
			auth.KeyCache.Add(userDID, KeyCacheEntry{
				Key:       ecdsaPubKey,
				ExpiresAt: time.Now().Add(auth.KeyCacheTTL),
			})

			return ecdsaPubKey, nil
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

func (auth *Auth) GetPLCEntry(ctx context.Context, did string) (*PLCEntry, error) {
	tracer := otel.Tracer("auth")
	ctx, span := tracer.Start(ctx, "Auth:GetPLCEntry")
	defer span.End()

	// Wait for the rate limiter
	auth.Limiter.Wait(ctx)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/%s", auth.PLCDirectory, did), nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create PLC Directory request: %v", err)
	}

	// Execute the request with the auth's instrumented HTTP client
	resp, err := auth.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to get user's entry from PLC Directory: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Failed to get user's entry from PLC Directory: %v", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read PLC Directory response: %v", err)
	}

	// Unmarshal into a PLC Entry
	plcEntry := &PLCEntry{}
	err = json.Unmarshal(body, plcEntry)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal PLC Entry: %v", err)
	}

	return plcEntry, nil
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

	authHeader := c.GetHeader("X-API-Key")
	if authHeader == "" {
		span.SetAttributes(attribute.Bool("auth.api_key", false))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing required API key in X-API-Key header"})
		c.Abort()
		return
	}

	for key, authEntity := range auth.APIKeyFeedMap {
		if authHeader == key {
			span.SetAttributes(attribute.Bool("auth.api_key", true))
			c.Set("feed.auth.entity", authEntity)
			c.Next()
			return
		}
	}

	span.SetAttributes(attribute.Bool("auth.api_key", false))
	c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid API key"})
	c.Abort()
	return
}
