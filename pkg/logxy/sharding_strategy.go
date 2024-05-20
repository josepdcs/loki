package logxy

import (
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/util/flagext"
)

const (
	defaultMaxBytesPerDownstreamServer = 600 << 20 // 600MB
)

// shardingStrategy is the sharding strategy for logxy.
type shardingStrategy struct {
	logxyConfig queryrange.LogxyConfig
}

// NewShardingStrategy returns a new sharding strategy for logxy.
func NewShardingStrategy(logxyConfig queryrange.LogxyConfig) logql.ShardingStrategy {
	return shardingStrategy{
		logxyConfig: logxyConfig,
	}
}

// Shards returns the fixed shards according to the logxy configuration.
// We have a shard per downstream server.
func (s shardingStrategy) Shards(syntax.Expr) (logql.Shards, uint64, error) {
	var maxBytes flagext.ByteSize
	shards := make(logql.Shards, 0, len(s.logxyConfig.DownstreamServers))
	for _, d := range s.logxyConfig.DownstreamServers {
		shards = append(shards, logql.Shard{LogxyShard: newLogxyShard(d.Name, d.Url)})
		if d.MaxBytes == 0 {
			d.MaxBytes = defaultMaxBytesPerDownstreamServer
		}
		maxBytes = max(maxBytes, d.MaxBytes)
	}

	return shards, uint64(maxBytes), nil
}

// Resolver returns the shard resolver for the logxy strategy.
func (s shardingStrategy) Resolver() logql.ShardResolver {
	shards, maxBytes, _ := s.Shards(nil)
	return NewShardResolver(shards, maxBytes)
}

// newLogxyShard returns a new logxy shard.
func newLogxyShard(name, url string) *logql.LogxyShard {
	return &logql.LogxyShard{
		Version: logql.LogxySharingV1,
		Url:     url,
		Name:    name,
	}
}
