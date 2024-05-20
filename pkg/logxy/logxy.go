package logxy

import (
	"github.com/go-kit/log"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

// Logxy is a LogxyInterface implementation.
type Logxy struct {
	// Active indicates whether the Logxy is active.
	Active bool
	// Cfg is the Logxy configuration.
	Cfg queryrange.LogxyConfig
}

// New creates a new Logxy instance.
func New(cfg queryrange.LogxyConfig) Logxy {
	return Logxy{
		Active: true,
		Cfg:    cfg,
	}
}

// IsActive returns whether the Logxy is active.
func (l Logxy) IsActive() bool {
	return l.Active
}

// Config returns the Logxy configuration.
func (l Logxy) Config() queryrange.LogxyConfig {
	return l.Cfg
}

// ShardingMiddleware returns a middleware that shards queries across multiple queriers.
func (l Logxy) ShardingMiddleware(
	logger log.Logger,
	confs queryrange.ShardingConfigs,
	engineOpts logql.EngineOpts,
	middlewareMetrics *queryrangebase.InstrumentMiddlewareMetrics,
	shardingMetrics *logql.MapperMetrics,
	limits queryrange.Limits,
	maxShards int,
	statsHandler queryrangebase.Handler,
	shardAggregation []string,
) queryrangebase.Middleware {
	return NewQueryShardMiddleware(
		logger,
		confs,
		engineOpts,
		middlewareMetrics,
		shardingMetrics,
		limits,
		maxShards,
		statsHandler,
		shardAggregation,
		l,
	)
}

func (l Logxy) SplitMiddleware() queryrangebase.Middleware {
	return NewLogxySplitterMiddleware(l)
}

func (l Logxy) LogxyQuerierAPI() *querier.QuerierAPI {
	return nil
}
