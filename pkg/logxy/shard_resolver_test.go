package logxy

import (
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_shardResolver_GetStats(t *testing.T) {
	resolver := NewShardResolver([]logql.ShardWithChunkRefs{
		{
			Shard: logql.Shard{
				LogxyShard: &logql.LogxyShard{
					Version: "version",
					Name:    "Name",
					Url:     "Url",
				},
			},
		},
	}, 100)

	stats, err := resolver.GetStats(nil)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	assert.Equal(t, stats.Streams, uint64(0))
	assert.Equal(t, stats.Chunks, uint64(0))
	assert.Equal(t, stats.Bytes, uint64(100))
}

func Test_shardResolver_Shards(t *testing.T) {
	resolver := NewShardResolver([]logql.ShardWithChunkRefs{
		{
			Shard: logql.Shard{
				LogxyShard: &logql.LogxyShard{
					Version: "version",
					Name:    "Name",
					Url:     "Url",
				},
			},
		},
	}, 100)

	shards, maxBytes, err := resolver.Shards(nil)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	assert.Equal(t, shards, 1)
	assert.Equal(t, maxBytes, uint64(100))
}

func Test_shardResolver_ShardingRanges(t *testing.T) {
	resolver := NewShardResolver([]logql.ShardWithChunkRefs{
		{
			Shard: logql.Shard{
				LogxyShard: &logql.LogxyShard{
					Version: "version",
					Name:    "Name",
					Url:     "Url",
				},
			},
		},
	}, 100)

	shards, chunkRefGroups, err := resolver.ShardingRanges(nil, 100)
	require.Error(t, err)
	assert.Equal(t, len(shards), 0)
	assert.Equal(t, len(chunkRefGroups), 0)
}
