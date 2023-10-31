package auth

import (
	"context"
	"sync"
)

type StaticProvider struct {
	lk            sync.RWMutex
	APIKeyFeedMap map[string]*FeedAuthEntity
}

func NewStaticProvider() *StaticProvider {
	return &StaticProvider{
		APIKeyFeedMap: make(map[string]*FeedAuthEntity),
	}
}

func (p *StaticProvider) UpdateAPIKeyFeedMapping(ctx context.Context, apiKey string, feedAuthEntity *FeedAuthEntity) {
	p.lk.Lock()
	defer p.lk.Unlock()
	p.APIKeyFeedMap[apiKey] = feedAuthEntity
}

func (p *StaticProvider) GetEntityFromAPIKey(ctx context.Context, apiKey string) (*FeedAuthEntity, error) {
	p.lk.RLock()
	defer p.lk.RUnlock()
	if entity, ok := p.APIKeyFeedMap[apiKey]; ok {
		return entity, nil
	}
	return nil, ErrAPIKeyNotFound
}
