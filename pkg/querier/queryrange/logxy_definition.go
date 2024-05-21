package queryrange

import (
	"flag"
	"github.com/grafana/loki/v3/pkg/util/flagext"

	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

// Logxy is the interface that wraps the methods required to implement the logxy middleware
type Logxy interface {
	// Enabled returns whether the logxy is enabled.
	Enabled() bool
	// Config returns the logxy configuration.
	Config() LogxyConfig
	// ShardingMiddleware returns a middleware that shards queries across multiple queriers.
	ShardingMiddleware(
		logger log.Logger,
		confs ShardingConfigs,
		engineOpts logql.EngineOpts,
		middlewareMetrics *queryrangebase.InstrumentMiddlewareMetrics,
		shardingMetrics *logql.MapperMetrics,
		limits Limits,
		maxShards int,
		statsHandler queryrangebase.Handler,
		shardAggregation []string,
	) queryrangebase.Middleware
}

// EmptyLogxy returns an inactive logxy.
func EmptyLogxy() Logxy {
	return inactiveLogxy{}
}

// inactiveLogxy is a Logxy implementation that is not enabled.
type inactiveLogxy struct {
}

// Enabled returns whether the logxy is enabled.
// This implementation always returns false.
func (l inactiveLogxy) Enabled() bool {
	return false
}

// Config returns the logxy configuration.
// This implementation always returns an empty config.
func (l inactiveLogxy) Config() LogxyConfig {
	return LogxyConfig{}
}

// ShardingMiddleware returns a middleware that shards queries across multiple queriers.
func (l inactiveLogxy) ShardingMiddleware(
	log.Logger,
	ShardingConfigs,
	logql.EngineOpts,
	*queryrangebase.InstrumentMiddlewareMetrics,
	*logql.MapperMetrics,
	Limits,
	int,
	queryrangebase.Handler,
	[]string,
) queryrangebase.Middleware {
	return queryrangebase.PassthroughMiddleware
}

// SplitMiddleware returns a middleware that splits queries across multiple queriers.
func (l inactiveLogxy) SplitMiddleware() queryrangebase.Middleware {
	return queryrangebase.PassthroughMiddleware
}

// DownstreamServer represents a downstream server that logxy can query
type DownstreamServer struct {
	Name      string           `yaml:"name"`
	Url       string           `yaml:"url"`
	MaxBytes  flagext.ByteSize `yaml:"max_bytes"`
	IsDefault bool             `yaml:"is_default"`
}

// RegisterFlags registers flags for the downstream server
func (*DownstreamServer) RegisterFlags(*flag.FlagSet) {
	// no flags to register
}

// Validate the logxy config and returns an error if the validation
// doesn't pass
func (*DownstreamServer) Validate() error {
	return nil
}

// LogxyConfig is the configuration for the logxy middleware
type LogxyConfig struct {
	DownstreamServers []DownstreamServer `yaml:"downstream_servers"`
}

// RegisterFlags registers flags for the logxy config
func (*LogxyConfig) RegisterFlags(*flag.FlagSet) {
	// no flags to register
}

// Validate the logxy config and returns an error if the validation
// doesn't pass
func (*LogxyConfig) Validate() error {
	return nil
}

// HasDefaultDownstreamServer returns whether the logxy config has a default downstream server
func (l *LogxyConfig) HasDefaultDownstreamServer() bool {
	return l.GetDefaultDownstreamServer() != nil
}

// GetDefaultDownstreamServer returns the default downstream server
func (l *LogxyConfig) GetDefaultDownstreamServer() *DownstreamServer {
	for _, ds := range l.DownstreamServers {
		if ds.IsDefault {
			return &ds
		}
	}
	return nil
}
