package util

import (
	"sync"

	"github.com/bluesky-social/indigo/xrpc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type CommandCtx struct {
	Logger    *zap.SugaredLogger
	Client    *xrpc.Client
	ClientMux sync.RWMutex
	Limiter   *rate.Limiter
	BotDid    string
}

func NewCommandCtx(
	logger *zap.SugaredLogger,
	client *xrpc.Client,
	limiter *rate.Limiter,
	botDid string,
) *CommandCtx {
	return &CommandCtx{
		Logger:  logger,
		Client:  client,
		Limiter: limiter,
		BotDid:  botDid,
	}
}
