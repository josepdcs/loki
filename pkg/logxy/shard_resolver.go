package logxy

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb/sharding"
	"github.com/grafana/loki/v3/pkg/util/spanlogger"
	"github.com/grafana/loki/v3/pkg/util/validation"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/stats"
)

type ShardStrategy struct {
	resolver logql.ShardResolver
}

func NewShardStrategy(resolver logql.ShardResolver) *ShardStrategy {
	return &ShardStrategy{
		resolver: resolver,
	}

}

type logxyShardResolver struct {
	ctx context.Context
	// TODO(owen-d): shouldn't have to fork handlers here -- one should just transparently handle the right logic
	// depending on the underlying type?
	statsHandler queryrangebase.Handler // index stats handler (hooked up to results cache, etc)
	next         queryrangebase.Handler // next handler in the chain (used for non-stats reqs)
	logger       log.Logger
	limits       queryrange.Limits

	from, through   model.Time
	maxParallelism  int
	maxShards       int
	defaultLookback time.Duration

	dataSources []string
}

func (r *logxyShardResolver) GetStats(e syntax.Expr) (stats.Stats, error) {
	sp, ctx := opentracing.StartSpanFromContext(r.ctx, "dynamicShardResolver.GetStats")
	defer sp.Finish()
	log := spanlogger.FromContext(r.ctx)
	defer log.Finish()

	start := time.Now()

	// We try to shard subtrees in the AST independently if possible, although
	// nested binary expressions can make this difficult. In this case,
	// we query the index stats for all matcher groups then sum the results.
	grps, err := syntax.MatcherGroups(e)
	if err != nil {
		return stats.Stats{}, err
	}

	// If there are zero matchers groups, we'll inject one to query everything
	if len(grps) == 0 {
		grps = append(grps, syntax.MatcherRange{})
	}

	results, err := getStatsForMatchers(ctx, log, r.statsHandler, r.from, r.through, grps, r.maxParallelism, r.defaultLookback)
	if err != nil {
		return stats.Stats{}, err
	}

	combined := stats.MergeStats(results...)

	level.Debug(log).Log(
		append(
			combined.LoggingKeyValues(),
			"msg", "queried index",
			"type", "combined",
			"len", len(results),
			"max_parallelism", r.maxParallelism,
			"duration", time.Since(start),
		)...,
	)

	return combined, nil
}

// getStatsForMatchers returns the index stats for all the groups in matcherGroups.
func getStatsForMatchers(
	ctx context.Context,
	logger log.Logger,
	statsHandler queryrangebase.Handler,
	start, end model.Time,
	matcherGroups []syntax.MatcherRange,
	parallelism int,
	defaultLookback time.Duration,
) ([]*stats.Stats, error) {
	startTime := time.Now()

	results := make([]*stats.Stats, len(matcherGroups))
	if err := concurrency.ForEachJob(ctx, len(matcherGroups), parallelism, func(ctx context.Context, i int) error {
		matchers := syntax.MatchersString(matcherGroups[i].Matchers)
		diff := matcherGroups[i].Interval + matcherGroups[i].Offset
		adjustedFrom := start.Add(-diff)
		if matcherGroups[i].Interval == 0 {
			// For limited instant queries, when start == end, the queries would return
			// zero results. Prometheus has a concept of "look back amount of time for instant queries"
			// since metric data is sampled at some configurable scrape_interval (commonly 15s, 30s, or 1m).
			// We copy that idea and say "find me logs from the past when start=end".
			adjustedFrom = adjustedFrom.Add(-defaultLookback)
		}

		adjustedThrough := end.Add(-matcherGroups[i].Offset)

		resp, err := statsHandler.Do(ctx, &logproto.IndexStatsRequest{
			From:     adjustedFrom,
			Through:  adjustedThrough,
			Matchers: matchers,
		})
		if err != nil {
			return err
		}

		casted, ok := resp.(*queryrange.IndexStatsResponse)
		if !ok {
			return fmt.Errorf("expected *IndexStatsResponse while querying index, got %T", resp)
		}

		results[i] = casted.Response

		level.Debug(logger).Log(
			append(
				casted.Response.LoggingKeyValues(),
				"msg", "queried index",
				"type", "single",
				"matchers", matchers,
				"duration", time.Since(startTime),
				"from", adjustedFrom.Time(),
				"through", adjustedThrough.Time(),
				"length", adjustedThrough.Sub(adjustedFrom),
			)...,
		)

		return nil
	}); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *logxyShardResolver) Shards(e syntax.Expr) (int, uint64, error) {
	sp, ctx := opentracing.StartSpanFromContext(r.ctx, "dynamicShardResolver.Shards")
	defer sp.Finish()
	log := spanlogger.FromContext(ctx)
	defer log.Finish()

	combined, err := r.GetStats(e)
	if err != nil {
		return 0, 0, err
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return 0, 0, err
	}

	maxBytesPerShard := validation.SmallestPositiveIntPerTenant(tenantIDs, r.limits.TSDBMaxBytesPerShard)
	factor := sharding.GuessShardFactor(combined.Bytes, uint64(maxBytesPerShard), r.maxShards)

	var bytesPerShard = combined.Bytes
	if factor > 0 {
		bytesPerShard = combined.Bytes / uint64(factor)
	}

	level.Debug(log).Log(
		append(
			combined.LoggingKeyValues(),
			"msg", "got shard factor",
			"factor", factor,
			"total_bytes", strings.Replace(humanize.Bytes(combined.Bytes), " ", "", 1),
			"bytes_per_shard", strings.Replace(humanize.Bytes(bytesPerShard), " ", "", 1),
		)...,
	)
	return factor, bytesPerShard, nil
}
