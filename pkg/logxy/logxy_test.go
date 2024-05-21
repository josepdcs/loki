package logxy

import (
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogxy(t *testing.T) {
	l := New(queryrange.LogxyConfig{
		DownstreamServers: []queryrange.DownstreamServer{
			{Name: "default", Url: "http://default", IsDefault: true},
			{Name: "shard1", Url: "http://shard1"},
		},
	})
	assert.True(t, l.Enabled())
	assert.Equal(t, queryrange.LogxyConfig{
		DownstreamServers: []queryrange.DownstreamServer{
			{Name: "default", Url: "http://default", IsDefault: true},
			{Name: "shard1", Url: "http://shard1"},
		},
	}, l.Config())
	shardingMiddleware := l.ShardingMiddleware(nil, nil, testEngineOpts, nil, nil, nil, 0, nil, nil)
	assert.NotNil(t, shardingMiddleware)
	assert.Implements(t, (*queryrangebase.Middleware)(nil), shardingMiddleware)
}
