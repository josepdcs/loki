package queryrange

import (
	"flag"
	"github.com/grafana/loki/v3/pkg/util/flagext"

	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

type LogxyInterface interface {
	IsActive() bool
	Config() LogxyConfig
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
	SplitMiddleware() queryrangebase.Middleware
}

func EmptyLogxy() LogxyInterface {
	return inactiveLogxy{}
}

type inactiveLogxy struct {
}

func (l inactiveLogxy) IsActive() bool {
	return false
}

func (l inactiveLogxy) Config() LogxyConfig {
	return EmptyLogxyConfig()
}

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

func (l inactiveLogxy) SplitMiddleware() queryrangebase.Middleware {
	return queryrangebase.PassthroughMiddleware
}

type DownstreamServer struct {
	Name     string           `yaml:"name"`
	Url      string           `yaml:"url"`
	MaxBytes flagext.ByteSize `yaml:"max_bytes"`
}

func (*DownstreamServer) RegisterFlags(*flag.FlagSet) {
	// no flags to register
}

// Validate the logxy config and returns an error if the validation
// doesn't pass
func (*DownstreamServer) Validate() error {
	return nil
}

type LogxyConfig struct {
	DownstreamServers []DownstreamServer `yaml:"downstream_servers"`
}

func (*LogxyConfig) RegisterFlags(*flag.FlagSet) {
	// no flags to register
}

// Validate the logxy config and returns an error if the validation
// doesn't pass
func (*LogxyConfig) Validate() error {
	return nil
}

func EmptyLogxyConfig() LogxyConfig {
	return LogxyConfig{}
}
