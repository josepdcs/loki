package logxy

import (
	"github.com/go-kit/log"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

// logxy is a queryrange.Logxy implementation.
type logxy struct {
	// enabled indicates whether the logxy is enabled.
	enabled bool
	// config is the logxy configuration.
	config queryrange.LogxyConfig
}

// New creates a new queryrange.Logxy instance.
func New(config queryrange.LogxyConfig) queryrange.Logxy {
	return logxy{
		enabled: true,
		config:  config,
	}
}

// Enabled returns whether the logxy is enabled.
func (l logxy) Enabled() bool {
	return l.enabled
}

// Config returns the logxy configuration.
func (l logxy) Config() queryrange.LogxyConfig {
	return l.config
}

// ShardingMiddleware returns a middleware that shards queries across multiple queriers.
func (l logxy) ShardingMiddleware(
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
