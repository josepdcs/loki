package logxy

import (
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_shardingStrategy_Shards(t *testing.T) {
	logxyConfig := queryrange.LogxyConfig{
		DownstreamServers: []queryrange.DownstreamServer{
			{
				Name:      "Name",
				Url:       "Url",
				MaxBytes:  defaultMaxBytesPerDownstreamServer,
				IsDefault: true,
			},
			{
				Name:     "Name 2",
				Url:      "Url 2",
				MaxBytes: defaultMaxBytesPerDownstreamServer * 2,
			},
		},
	}

	strategy := NewShardingStrategy(logxyConfig)
	shards, numBytes, err := strategy.Shards(nil)
	assert.Len(t, shards, 2)
	assert.Equal(t, numBytes, uint64(defaultMaxBytesPerDownstreamServer*2))
	assert.NoError(t, err)

}

func Test_shardingStrategy_Resolver(t *testing.T) {
	logxyConfig := queryrange.LogxyConfig{
		DownstreamServers: []queryrange.DownstreamServer{
			{
				Name:      "Name",
				Url:       "Url",
				IsDefault: true,
			},
			{
				Name:     "Name 2",
				Url:      "Url 2",
				MaxBytes: defaultMaxBytesPerDownstreamServer * 2,
			},
		},
	}

	strategy := NewShardingStrategy(logxyConfig)
	resolver := strategy.Resolver()
	shards, numBytes, err := resolver.Shards(nil)
	assert.Equal(t, shards, 2)
	assert.Equal(t, numBytes, uint64(defaultMaxBytesPerDownstreamServer*2))
	assert.NoError(t, err)
}
