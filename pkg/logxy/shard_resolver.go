package logxy

import (
	"errors"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/stats"
)

// shardResolver is a fake implementation of logql.ShardResolver
type shardResolver struct {
	shards   logql.Shards
	maxBytes uint64
}

// NewShardResolver returns a new instance of logql.ShardResolver
func NewShardResolver(shards logql.Shards, maxBytes uint64) logql.ShardResolver {
	return shardResolver{
		shards:   shards,
		maxBytes: maxBytes,
	}
}

// GetStats returns the stats for the given expression.
// Our fake implementation of GetStats only returns stats with the maxBytes.
func (r shardResolver) GetStats(syntax.Expr) (stats.Stats, error) {
	return stats.Stats{
		Streams: 0,
		Chunks:  0,
		Bytes:   r.maxBytes, // only this one is used in shardmanager (noOp)
		Entries: 0,
	}, nil
}

// Shards returns the shards already set in the resolver.
// Our fake implementation of Shards returns only the number of shards and maxBytes
// already calculated in the shard strategy.
func (r shardResolver) Shards(syntax.Expr) (int, uint64, error) {
	return len(r.shards), r.maxBytes, nil
}

// ShardingRanges never should be called.
func (r shardResolver) ShardingRanges(syntax.Expr, uint64) ([]logproto.Shard, error) {
	return []logproto.Shard{}, errors.New("logxy ShardingRanges should not be called")
}
