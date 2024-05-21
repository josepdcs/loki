package logxy

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/pkg/util/marshal"
	"github.com/grafana/loki/v3/pkg/util/spanlogger"
	"github.com/grafana/loki/v3/pkg/util/validation"
)

// NewQueryShardMiddleware creates a middleware which downstreams queries after AST mapping and query encoding.
func NewQueryShardMiddleware(
	logger log.Logger,
	confs queryrange.ShardingConfigs,
	engineOpts logql.EngineOpts,
	middlewareMetrics *queryrangebase.InstrumentMiddlewareMetrics,
	shardingMetrics *logql.MapperMetrics,
	limits queryrange.Limits,
	maxShards int,
	statsHandler queryrangebase.Handler,
	shardAggregation []string,
	logxy queryrange.Logxy,
) queryrangebase.Middleware {

	mapperware := queryrangebase.MiddlewareFunc(func(next queryrangebase.Handler) queryrangebase.Handler {
		return newASTMapperware(confs, engineOpts, next, statsHandler, logger, shardingMetrics, limits, maxShards, shardAggregation, logxy)
	})

	return queryrangebase.MiddlewareFunc(func(next queryrangebase.Handler) queryrangebase.Handler {
		return &shardSplitter{
			limits: limits,
			shardingware: queryrangebase.MergeMiddlewares(
				queryrangebase.InstrumentMiddleware("shardingware", middlewareMetrics),
				mapperware,
			).Wrap(next),
			now:  time.Now,
			next: queryrangebase.InstrumentMiddleware("sharding-bypass", middlewareMetrics).Wrap(next),
		}
	})
}

// NewASTMapperware creates a new AST mapperware middleware.
func newASTMapperware(
	confs queryrange.ShardingConfigs,
	engineOpts logql.EngineOpts,
	next queryrangebase.Handler,
	statsHandler queryrangebase.Handler,
	logger log.Logger,
	metrics *logql.MapperMetrics,
	limits queryrange.Limits,
	maxShards int,
	shardAggregation []string,
	logxy queryrange.Logxy,
) *astMapperware {
	ast := &astMapperware{
		confs:            confs,
		logger:           log.With(logger, "middleware", "QueryShard.astMapperware"),
		limits:           limits,
		next:             next,
		statsHandler:     next,
		ng:               logql.NewDownstreamEngine(engineOpts, queryrange.DownstreamHandler{Limits: limits, Next: next}, limits, logger),
		metrics:          metrics,
		maxShards:        maxShards,
		shardAggregation: shardAggregation,
		logxy:            logxy,
	}

	if statsHandler != nil {
		ast.statsHandler = statsHandler
	}

	return ast
}

// astMapperware is a middleware that maps ASTs to sharded queries.
type astMapperware struct {
	confs        queryrange.ShardingConfigs
	logger       log.Logger
	limits       queryrange.Limits
	next         queryrangebase.Handler
	statsHandler queryrangebase.Handler
	ng           *logql.DownstreamEngine
	metrics      *logql.MapperMetrics
	maxShards    int

	// Feature flag for sharding range and vector aggregations such as
	// quantile_ver_time with probabilistic data structures.
	shardAggregation []string
	logxy            queryrange.Logxy
}

// Do implements the queryrangebase.Handler interface.
func (ast *astMapperware) Do(ctx context.Context, r queryrangebase.Request) (queryrangebase.Response, error) {
	logger := spanlogger.FromContextWithFallback(
		ctx,
		util_log.WithContext(ctx, ast.logger),
	)

	params, err := queryrange.ParamsFromRequest(r)
	if err != nil {
		return nil, err
	}

	// The shard resolver uses index stats to determine the number of shards.
	// We want to store the cache stats for the requests to get the index stats.
	// Later on, the query engine overwrites the stats context with other stats,
	// so we create a separate stats context here for the resolver that we
	// will merge with the stats returned from the engine.
	resolverStats, ctx := stats.NewContext(ctx)

	mapper := logql.NewShardMapper(NewShardingStrategy(ast.logxy.Config()), ast.metrics, ast.shardAggregation)

	noop, bytesPerShard, parsed, err := mapper.Parse(params.GetExpression())
	if err != nil {
		level.Warn(logger).Log("msg", "failed mapping AST", "err", err.Error(), "query", r.GetQuery())
		return nil, err
	}
	level.Debug(logger).Log("no-op", noop, "mapped", parsed.String())

	// Note, even if noop, bytesPerShard contains the bytes that'd be read for the whole expr without sharding
	if err = ast.checkQuerySizeLimit(ctx, bytesPerShard, noop); err != nil {
		return nil, err
	}

	// If the ast can't be mapped to a sharded equivalent,
	// we can bypass the sharding engine and forward the request downstream.
	if noop {
		return ast.next.Do(ctx, r)
	}

	var path string
	switch r := r.(type) {
	case *queryrange.LokiRequest:
		path = r.GetPath()
	case *queryrange.LokiInstantRequest:
		path = r.GetPath()
	default:
		return nil, fmt.Errorf("expected *LokiRequest or *LokiInstantRequest, got (%T)", r)
	}
	query := ast.ng.Query(ctx, logql.ParamsWithExpressionOverride{Params: params, ExpressionOverride: parsed})

	res, err := query.Exec(ctx)
	if err != nil {
		return nil, err
	}

	// Merge index and volume stats result cache stats from shard resolver into the query stats.
	res.Statistics.Merge(resolverStats.Result(0, 0, 0))
	value, err := marshal.NewResultValue(res.Data)
	if err != nil {
		return nil, err
	}

	switch res.Data.Type() {
	case parser.ValueTypeMatrix:
		return &queryrange.LokiPromResponse{
			Response: &queryrangebase.PrometheusResponse{
				Status: loghttp.QueryStatusSuccess,
				Data: queryrangebase.PrometheusData{
					ResultType: loghttp.ResultTypeMatrix,
					Result:     queryrange.ToProtoMatrix(value.(loghttp.Matrix)),
				},
				Headers:  res.Headers,
				Warnings: res.Warnings,
			},
			Statistics: res.Statistics,
		}, nil
	case logqlmodel.ValueTypeStreams:
		respHeaders := make([]queryrangebase.PrometheusResponseHeader, 0, len(res.Headers))
		for i := range res.Headers {
			respHeaders = append(respHeaders, *res.Headers[i])
		}

		return &queryrange.LokiResponse{
			Status:     loghttp.QueryStatusSuccess,
			Direction:  params.Direction(),
			Limit:      params.Limit(),
			Version:    uint32(loghttp.GetVersion(path)),
			Statistics: res.Statistics,
			Data: queryrange.LokiData{
				ResultType: loghttp.ResultTypeStream,
				Result:     value.(loghttp.Streams).ToProto(),
			},
			Headers:  respHeaders,
			Warnings: res.Warnings,
		}, nil
	case parser.ValueTypeVector:
		return &queryrange.LokiPromResponse{
			Statistics: res.Statistics,
			Response: &queryrangebase.PrometheusResponse{
				Status: loghttp.QueryStatusSuccess,
				Data: queryrangebase.PrometheusData{
					ResultType: loghttp.ResultTypeVector,
					Result:     queryrange.ToProtoVector(value.(loghttp.Vector)),
				},
				Headers:  res.Headers,
				Warnings: res.Warnings,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unexpected downstream response type (%T)", res.Data.Type())
	}
}

// checkQuerySizeLimit checks if the query size exceeds the limit set for the querier.
// It returns an error if the query size exceeds the limit.
func (ast *astMapperware) checkQuerySizeLimit(ctx context.Context, bytesPerShard uint64, notShardable bool) error {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	maxQuerierBytesReadCapture := func(id string) int { return ast.limits.MaxQuerierBytesRead(ctx, id) }
	if maxBytesRead := validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, maxQuerierBytesReadCapture); maxBytesRead > 0 {
		statsBytesStr := humanize.IBytes(bytesPerShard)
		maxBytesReadStr := humanize.IBytes(uint64(maxBytesRead))

		if bytesPerShard > uint64(maxBytesRead) {
			level.Warn(ast.logger).Log("msg", "Query exceeds limits", "status", "rejected", "limit_name", "MaxQuerierBytesRead", "limit_bytes", maxBytesReadStr, "resolved_bytes", statsBytesStr)

			errorTmpl := queryrange.LimErrQuerierTooManyBytesShardableTmpl
			if notShardable {
				errorTmpl = queryrange.LimErrQuerierTooManyBytesUnshardableTmpl
			}

			return httpgrpc.Errorf(http.StatusBadRequest, errorTmpl, statsBytesStr, maxBytesReadStr)
		}

		level.Debug(ast.logger).Log("msg", "Query is within limits", "status", "accepted", "limit_name", "MaxQuerierBytesRead", "limit_bytes", maxBytesReadStr, "resolved_bytes", statsBytesStr)
	}

	return nil
}

// shardSplitter middleware will only shard appropriate requests that do not extend past the MinShardingLookback interval.
// This is used to send nonsharded requests to the ingesters in order to not overload them.
type shardSplitter struct {
	limits       queryrange.Limits      // delimiter for splitting sharded vs non-sharded queries
	shardingware queryrangebase.Handler // handler for sharded queries
	next         queryrangebase.Handler // handler for non-sharded queries
	now          func() time.Time       // injectable time.Now
}

func (splitter *shardSplitter) Do(ctx context.Context, r queryrangebase.Request) (queryrangebase.Response, error) {
	return splitter.shardingware.Do(ctx, r)

}
