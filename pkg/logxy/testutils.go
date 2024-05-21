package logxy

import (
	"context"
	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase/definitions"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/util/validation"
	valid "github.com/grafana/loki/v3/pkg/validation"
	"gopkg.in/yaml.v3"
	"time"
)

var (
	testTime       = time.Date(2019, 12, 2, 11, 10, 10, 10, time.UTC)
	testEngineOpts = logql.EngineOpts{
		MaxLookBackPeriod: 30 * time.Second,
		LogExecutingQuery: false,
	}
	start              = testTime //  Marshalling the time drops the monotonic clock so we can't use time.Now
	end                = start.Add(1 * time.Hour)
	nilShardingMetrics = logql.NewShardMapperMetrics(nil)
	defaultReq         = func() *queryrange.LokiRequest {
		return &queryrange.LokiRequest{
			Step:      1000,
			Limit:     100,
			StartTs:   start,
			EndTs:     end,
			Direction: logproto.BACKWARD,
			Path:      "/loki/api/v1/query_range",
		}
	}
	lokiResps = []queryrangebase.Response{
		&queryrange.LokiResponse{
			Status:    loghttp.QueryStatusSuccess,
			Direction: logproto.BACKWARD,
			Limit:     defaultReq().Limit,
			Version:   1,
			Headers: []definitions.PrometheusResponseHeader{
				{Name: "Header", Values: []string{"value"}},
			},
			Data: queryrange.LokiData{
				ResultType: loghttp.ResultTypeStream,
				Result: []logproto.Stream{
					{
						Labels: `{foo="bar", level="debug"}`,
						Entries: []logproto.Entry{
							{Timestamp: time.Unix(0, 6), Line: "6"},
							{Timestamp: time.Unix(0, 5), Line: "5"},
						},
					},
				},
			},
		},
		&queryrange.LokiResponse{
			Status:    loghttp.QueryStatusSuccess,
			Direction: logproto.BACKWARD,
			Limit:     100,
			Version:   1,
			Headers: []definitions.PrometheusResponseHeader{
				{Name: "Header", Values: []string{"value"}},
			},
			Data: queryrange.LokiData{
				ResultType: loghttp.ResultTypeStream,
				Result: []logproto.Stream{
					{
						Labels: `{foo="bar", level="error"}`,
						Entries: []logproto.Entry{
							{Timestamp: time.Unix(0, 2), Line: "2"},
							{Timestamp: time.Unix(0, 1), Line: "1"},
						},
					},
				},
			},
		},
	}
)

func mockHandler(resp queryrangebase.Response, err error) queryrangebase.Handler {
	return queryrangebase.HandlerFunc(func(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
		if expired := ctx.Err(); expired != nil {
			return nil, expired
		}

		return resp, err
	})
}

type fakeLimits struct {
	maxQueryLength              time.Duration
	maxQueryParallelism         int
	tsdbMaxQueryParallelism     int
	maxQueryLookback            time.Duration
	maxEntriesLimitPerQuery     int
	maxSeries                   int
	splitDuration               map[string]time.Duration
	metadataSplitDuration       map[string]time.Duration
	recentMetadataSplitDuration map[string]time.Duration
	recentMetadataQueryWindow   map[string]time.Duration
	instantMetricSplitDuration  map[string]time.Duration
	ingesterSplitDuration       map[string]time.Duration
	minShardingLookback         time.Duration
	queryTimeout                time.Duration
	requiredLabels              []string
	requiredNumberLabels        int
	maxQueryBytesRead           int
	maxQuerierBytesRead         int
	maxStatsCacheFreshness      time.Duration
	maxMetadataCacheFreshness   time.Duration
	volumeEnabled               bool
}

func (f fakeLimits) QuerySplitDuration(key string) time.Duration {
	if f.splitDuration == nil {
		return 0
	}
	return f.splitDuration[key]
}

func (f fakeLimits) InstantMetricQuerySplitDuration(key string) time.Duration {
	if f.instantMetricSplitDuration == nil {
		return 0
	}
	return f.instantMetricSplitDuration[key]
}

func (f fakeLimits) MetadataQuerySplitDuration(key string) time.Duration {
	if f.metadataSplitDuration == nil {
		return 0
	}
	return f.metadataSplitDuration[key]
}

func (f fakeLimits) RecentMetadataQuerySplitDuration(key string) time.Duration {
	if f.recentMetadataSplitDuration == nil {
		return 0
	}
	return f.recentMetadataSplitDuration[key]
}

func (f fakeLimits) RecentMetadataQueryWindow(key string) time.Duration {
	if f.recentMetadataQueryWindow == nil {
		return 0
	}
	return f.recentMetadataQueryWindow[key]
}

func (f fakeLimits) IngesterQuerySplitDuration(key string) time.Duration {
	if f.ingesterSplitDuration == nil {
		return 0
	}
	return f.ingesterSplitDuration[key]
}

func (f fakeLimits) MaxQueryLength(context.Context, string) time.Duration {
	if f.maxQueryLength == 0 {
		return time.Hour * 7
	}
	return f.maxQueryLength
}

func (f fakeLimits) MaxQueryRange(context.Context, string) time.Duration {
	return time.Second
}

func (f fakeLimits) MaxQueryParallelism(context.Context, string) int {
	return f.maxQueryParallelism
}

func (f fakeLimits) TSDBMaxQueryParallelism(context.Context, string) int {
	return f.tsdbMaxQueryParallelism
}

func (f fakeLimits) MaxEntriesLimitPerQuery(context.Context, string) int {
	return f.maxEntriesLimitPerQuery
}

func (f fakeLimits) MaxQuerySeries(context.Context, string) int {
	return f.maxSeries
}

func (f fakeLimits) MaxCacheFreshness(context.Context, string) time.Duration {
	return 1 * time.Minute
}

func (f fakeLimits) MaxQueryLookback(context.Context, string) time.Duration {
	return f.maxQueryLookback
}

func (f fakeLimits) MinShardingLookback(string) time.Duration {
	return f.minShardingLookback
}

func (f fakeLimits) MaxQueryBytesRead(context.Context, string) int {
	return f.maxQueryBytesRead
}

func (f fakeLimits) MaxQuerierBytesRead(context.Context, string) int {
	return f.maxQuerierBytesRead
}

func (f fakeLimits) QueryTimeout(context.Context, string) time.Duration {
	return f.queryTimeout
}

func (f fakeLimits) BlockedQueries(context.Context, string) []*validation.BlockedQuery {
	return []*validation.BlockedQuery{}
}

func (f fakeLimits) RequiredLabels(context.Context, string) []string {
	return f.requiredLabels
}

func (f fakeLimits) RequiredNumberLabels(_ context.Context, _ string) int {
	return f.requiredNumberLabels
}

func (f fakeLimits) MaxStatsCacheFreshness(_ context.Context, _ string) time.Duration {
	return f.maxStatsCacheFreshness
}

func (f fakeLimits) MaxMetadataCacheFreshness(_ context.Context, _ string) time.Duration {
	return f.maxMetadataCacheFreshness
}

func (f fakeLimits) VolumeEnabled(_ string) bool {
	return f.volumeEnabled
}

func (f fakeLimits) TSDBMaxBytesPerShard(_ string) int {
	return valid.DefaultTSDBMaxBytesPerShard
}
func (f fakeLimits) TSDBShardingStrategy(string) string {
	return logql.PowerOfTwoVersion.String()
}

var testSchemas = func() []config.PeriodConfig {
	confS := `
- from: "1950-01-01"
  store: boltdb-shipper
  object_store: gcs
  schema: v12
`

	var confs []config.PeriodConfig
	if err := yaml.Unmarshal([]byte(confS), &confs); err != nil {
		panic(err)
	}
	return confs
}()
