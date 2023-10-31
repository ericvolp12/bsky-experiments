package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
)

type StoreProvider struct {
	Store *store.Store
}

func NewStoreProvider(s *store.Store) *StoreProvider {
	return &StoreProvider{
		Store: s,
	}
}

func (p *StoreProvider) UpdateAPIKeyFeedMapping(ctx context.Context, apiKey string, feedAuthEntity *FeedAuthEntity) error {
	authBytes, err := json.Marshal(feedAuthEntity)
	if err != nil {
		return fmt.Errorf("failed to marshal feedAuthEntity: %w", err)
	}

	err = p.Store.Queries.CreateKey(ctx, store_queries.CreateKeyParams{
		ApiKey:       apiKey,
		AuthEntity:   authBytes,
		AssignedUser: feedAuthEntity.UserDID,
	})
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	return nil
}

func (p *StoreProvider) GetEntityFromAPIKey(ctx context.Context, apiKey string) (*FeedAuthEntity, error) {
	key, err := p.Store.Queries.GetKey(ctx, apiKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAPIKeyNotFound
		}
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	var authEntity FeedAuthEntity
	err = json.Unmarshal(key.AuthEntity, &authEntity)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal auth entity: %w", err)
	}

	return &authEntity, nil
}
