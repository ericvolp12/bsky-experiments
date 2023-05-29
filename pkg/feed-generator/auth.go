package feedgenerator

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

type Auth struct {
	KeyCache    *lru.ARCCache
	KeyCacheTTL time.Duration
}

func NewAuth(keyCacheSize int, keyCacheTTL time.Duration) (*Auth, error) {
	keyCache, err := lru.NewARC(keyCacheSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to create key cache: %v", err)
	}

	return &Auth{
		KeyCache:    keyCache,
		KeyCacheTTL: keyCacheTTL,
	}, nil
}

func (auth *Auth) AuthenticateRequest(c *gin.Context) {
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:AuthMiddleware")

	authHeader := c.GetHeader("Authorization")
	if authHeader == "" {
		span.End()
		c.Next()
		return
	}

	authHeaderParts := strings.Split(authHeader, " ")
	if len(authHeaderParts) != 2 {
		span.End()
		c.Next()
		return
	}

	if authHeaderParts[0] != "Bearer" {
		span.End()
		c.Next()
		return
	}

	accessToken := authHeaderParts[1]

	claims := &jwt.StandardClaims{}

	parser := jwt.Parser{
		ValidMethods: []string{es256k.SigningMethodES256K.Alg()},
	}

	token, err := parser.ParseWithClaims(accessToken, claims, func(token *jwt.Token) (interface{}, error) {
		if claims, ok := token.Claims.(*jwt.StandardClaims); ok {
			// Get the user's key from PLC Directory: https://plc.directory/{did}
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

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://plc.directory/%s", userDID), nil)
			if err != nil {
				return nil, fmt.Errorf("Failed to create PLC Directory request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, fmt.Errorf("Failed to get user's key from PLC Directory: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("Failed to get user's key from PLC Directory: %v", resp.Status)
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

			// Get the public key from the PLC Entry
			if len(plcEntry.VerificationMethod) == 0 {
				return nil, fmt.Errorf("No verification method found in PLC Entry")
			}

			// Get the public key from the PLC Entry
			publicKey := plcEntry.VerificationMethod[0].PublicKeyMultibase

			// Decode the public key
			_, decodedPublicKey, err := multibase.Decode(publicKey)
			if err != nil {
				return nil, fmt.Errorf("Failed to decode public key: %v", err)
			}

			log.Printf("Decoded public key: %x", decodedPublicKey)

			pub, err := secp256k1.ParsePubKey(decodedPublicKey)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse public key: %v", err)
			}

			ecdsaPubKey := pub.ToECDSA()

			// Add the key to the cache
			auth.KeyCache.Add(userDID, KeyCacheEntry{
				Key:       ecdsaPubKey,
				ExpiresAt: time.Now().Add(auth.KeyCacheTTL),
			})

			return ecdsaPubKey, nil
		}

		return nil, fmt.Errorf("Invalid authorization token")
	})

	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		span.End()
		c.Abort()
		return
	}

	if claims, ok := token.Claims.(*jwt.StandardClaims); ok {
		if claims.Audience != "did:web:feedsky.jazco.io" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid authorization token (invalid audience)"})
			c.Abort()
			return
		}
		// Set claims Issuer to context as user DID
		c.Set("user_did", claims.Issuer)
		span.SetAttributes(attribute.String("user.did", claims.Issuer))
		span.End()
		c.Next()
	} else {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid authorization token (invalid claims)"})
		span.End()
		c.Abort()
	}
}
