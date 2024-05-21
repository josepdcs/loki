package logxy

import (
	"context"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase/definitions"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/stretchr/testify/require"
	"math"
	"sort"
	"sync"
	"testing"
	"time"
)

// NOTE: These are based of the tests from pkg/queryrange/querysharding_test.go

func Test_shardSplitter_Do(t *testing.T) {
	req := defaultReq().WithStartEnd(start, end)

	for _, tc := range []struct {
		desc        string
		lookback    time.Duration
		shouldShard bool
	}{
		{
			desc:        "should shard when older than lookback",
			lookback:    -time.Minute,
			shouldShard: true,
		},
		{
			desc:        "should shard even when overlaps lookback",
			lookback:    end.Sub(start) / 2, // intersect the request causing it to avoid sharding
			shouldShard: true,
		},
		{
			desc:        "should shard even when never than lookback",
			lookback:    end.Sub(start) + 1,
			shouldShard: true,
		},
		{
			desc:        "default",
			lookback:    0,
			shouldShard: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var didShard bool
			splitter := &shardSplitter{
				shardingware: queryrangebase.HandlerFunc(func(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
					didShard = true
					return mockHandler(lokiResps[0], nil).Do(ctx, req)
				}),
				next: mockHandler(lokiResps[1], nil),
				now:  func() time.Time { return end },
				limits: fakeLimits{
					minShardingLookback: tc.lookback,
					queryTimeout:        time.Minute,
					maxQueryParallelism: 1,
				},
			}

			resp, err := splitter.Do(user.InjectOrgID(context.Background(), "1"), req)
			require.NoError(t, err)

			require.Equal(t, tc.shouldShard, didShard)
			require.Nil(t, err)

			if tc.shouldShard {
				require.Equal(t, lokiResps[0], resp)
			} else {
				require.Equal(t, lokiResps[1], resp)
			}
		})
	}
}

func Test_astMapper(t *testing.T) {
	var lock sync.Mutex
	called := 0

	handler := queryrangebase.HandlerFunc(func(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
		lock.Lock()
		defer lock.Unlock()
		resp := lokiResps[called]
		called++
		return resp, nil
	})

	mware := newASTMapperware(
		queryrange.ShardingConfigs{
			config.PeriodConfig{
				RowShards: 2,
			},
		},
		testEngineOpts,
		handler,
		nil,
		log.NewNopLogger(),
		nilShardingMetrics,
		fakeLimits{maxSeries: math.MaxInt32, maxQueryParallelism: 1, queryTimeout: time.Second},
		0,
		[]string{},
		New(queryrange.LogxyConfig{
			DownstreamServers: []queryrange.DownstreamServer{
				{Name: "default", Url: "http://default", IsDefault: true},
				{Name: "shard1", Url: "http://shard1"},
			},
		}),
	)

	req := defaultReq()
	req.Query = `{foo="bar"}`
	req.Plan = &plan.QueryPlan{
		AST: syntax.MustParseExpr(req.Query),
	}
	resp, err := mware.Do(user.InjectOrgID(context.Background(), "1"), req)
	require.Nil(t, err)

	require.Equal(t, []*definitions.PrometheusResponseHeader{
		{Name: "Header", Values: []string{"value"}},
	}, resp.GetHeaders())

	expected, err := queryrange.DefaultCodec.MergeResponse(lokiResps...)
	sort.Sort(logproto.Streams(expected.(*queryrange.LokiResponse).Data.Result))
	require.Nil(t, err)
	require.Equal(t, called, 2)
	require.Equal(t, expected.(*queryrange.LokiResponse).Data, resp.(*queryrange.LokiResponse).Data)
}

func Test_ShardingByPass(t *testing.T) {
	called := 0
	handler := queryrangebase.HandlerFunc(func(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
		called++
		return nil, nil
	})

	mware := newASTMapperware(
		queryrange.ShardingConfigs{
			config.PeriodConfig{
				RowShards: 2,
			},
		},
		testEngineOpts,
		handler,
		nil,
		log.NewNopLogger(),
		nilShardingMetrics,
		fakeLimits{maxSeries: math.MaxInt32, maxQueryParallelism: 1},
		0,
		[]string{},
		New(queryrange.LogxyConfig{
			DownstreamServers: []queryrange.DownstreamServer{
				{Name: "default", Url: "http://default", IsDefault: true},
				{Name: "shard1", Url: "http://shard1"},
			},
		}),
	)

	req := defaultReq()
	req.Query = `1+1`
	req.Plan = &plan.QueryPlan{
		AST: syntax.MustParseExpr(req.Query),
	}

	_, err := mware.Do(user.InjectOrgID(context.Background(), "1"), req)
	require.Nil(t, err)
	require.Equal(t, called, 1)
}

func Test_InstantSharding(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "1")

	var lock sync.Mutex
	called := 0
	shards := []string{}

	cpyPeriodConf := testSchemas[0]
	cpyPeriodConf.RowShards = 3
	sharding := NewQueryShardMiddleware(log.NewNopLogger(), queryrange.ShardingConfigs{
		cpyPeriodConf,
	}, testEngineOpts, queryrangebase.NewInstrumentMiddlewareMetrics(nil, constants.Loki),
		nilShardingMetrics,
		fakeLimits{
			maxSeries:           math.MaxInt32,
			maxQueryParallelism: 10,
			queryTimeout:        time.Second,
		},
		0,
		nil,
		[]string{},
		New(queryrange.LogxyConfig{
			DownstreamServers: []queryrange.DownstreamServer{
				{Name: "default", Url: "http://default", IsDefault: true},
				{Name: "shard1", Url: "http://shard1"},
				{Name: "shard2", Url: "http://shard2"},
			},
		}),
	)
	response, err := sharding.Wrap(queryrangebase.HandlerFunc(func(c context.Context, r queryrangebase.Request) (queryrangebase.Response, error) {
		lock.Lock()
		defer lock.Unlock()
		called++
		shards = append(shards, r.(*queryrange.LokiInstantRequest).Shards...)
		return &queryrange.LokiPromResponse{Response: &queryrangebase.PrometheusResponse{
			Data: queryrangebase.PrometheusData{
				ResultType: loghttp.ResultTypeVector,
				Result: []queryrangebase.SampleStream{
					{
						Labels:  []logproto.LabelAdapter{{Name: "foo", Value: "bar"}},
						Samples: []logproto.LegacySample{{Value: 10, TimestampMs: 10}},
					},
				},
			},
		}}, nil
	})).Do(ctx, &queryrange.LokiInstantRequest{
		Query:  `rate({app="foo"}[1m])`,
		TimeTs: util.TimeFromMillis(10),
		Path:   "/v1/query",
		Plan: &plan.QueryPlan{
			AST: syntax.MustParseExpr(`rate({app="foo"}[1m])`),
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, called, "expected 3 calls but got {}", called)
	require.Len(t, response.(*queryrange.LokiPromResponse).Response.Data.Result, 3)
	require.ElementsMatch(t, []string{
		"{\"version\":\"logxy_v1\",\"name\":\"shard1\",\"url\":\"http://shard1\"}",
		"{\"version\":\"logxy_v1\",\"name\":\"default\",\"url\":\"http://default\"}",
		"{\"version\":\"logxy_v1\",\"name\":\"shard2\",\"url\":\"http://shard2\"}",
	}, shards)
	require.Equal(t, queryrangebase.PrometheusData{
		ResultType: loghttp.ResultTypeVector,
		Result: []queryrangebase.SampleStream{
			{
				Labels:  []logproto.LabelAdapter{{Name: "foo", Value: "bar"}},
				Samples: []logproto.LegacySample{{Value: 10, TimestampMs: 10}},
			},
			{
				Labels:  []logproto.LabelAdapter{{Name: "foo", Value: "bar"}},
				Samples: []logproto.LegacySample{{Value: 10, TimestampMs: 10}},
			},
			{
				Labels:  []logproto.LabelAdapter{{Name: "foo", Value: "bar"}},
				Samples: []logproto.LegacySample{{Value: 10, TimestampMs: 10}},
			},
		},
	}, response.(*queryrange.LokiPromResponse).Response.Data)
	require.Equal(t, loghttp.QueryStatusSuccess, response.(*queryrange.LokiPromResponse).Response.Status)
}
