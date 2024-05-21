package logxy

import (
	"context"
	"github.com/go-kit/log/level"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/pkg/errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

// The VERSION to be used internally for running Loki with logxy enabled
// It's only used for testing purposes as a PoC
//
// downstreamRoundTripper is a queryrangebase.Handler that forwards requests to a downstream URL.
type downstreamRoundTripper struct {
	logxyConfig queryrange.LogxyConfig

	defaultDownstreamURL *url.URL
	defaultTransport     http.RoundTripper
	codec                queryrangebase.Codec
}

// NewDownstreamRoundTripper creates a new downstreamRoundTripper passing the given downstream URL and RoundTripper.
func NewDownstreamRoundTripper(logxyConfig queryrange.LogxyConfig) (queryrangebase.Handler, error) {
	var defaultDownstreamURL *url.URL
	var err error
	if logxyConfig.HasDefaultDownstreamServer() {
		defaultDownstreamURL, err = url.Parse(logxyConfig.GetDefaultDownstreamServer().Url)
		if err != nil {
			return nil, err
		}
	}

	return &downstreamRoundTripper{
		defaultDownstreamURL: defaultDownstreamURL,
		defaultTransport:     http.DefaultTransport,
		codec:                queryrange.DefaultCodec,
		logxyConfig:          logxyConfig,
	}, nil
}

func (d *downstreamRoundTripper) Do(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
	tracer, span := opentracing.GlobalTracer(), opentracing.SpanFromContext(ctx)

	r, err := d.codec.EncodeRequest(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "cannot convert request to HTTP request")
	}
	if err := user.InjectOrgIDIntoHTTPRequest(ctx, r); err != nil {
		return nil, err
	}

	if tracer != nil && span != nil {
		carrier := opentracing.HTTPHeadersCarrier(r.Header)
		err := tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier)
		if err != nil {
			return nil, err
		}
	}

	switch concrete := req.(type) {
	case *queryrange.LokiInstantRequest:
		shards, _, err := logql.ParseShards(concrete.Shards)

		if len(shards) == 0 {
			if d.defaultDownstreamURL != nil {
				setURL(r, d.defaultDownstreamURL.Scheme, d.defaultDownstreamURL.Host)
				_ = level.Info(util_log.Logger).Log("msg", "Unshardeable Instant Query", "host", r.URL.Host, "time", concrete.TimeTs.Format(time.RFC3339Nano), "query", concrete.Query)
				break
			} else {
				_ = level.Error(util_log.Logger).Log("msg", "Unshardeable Instant Query", "host", r.URL.Host, "time", concrete.TimeTs.Format(time.RFC3339Nano), "query", concrete.Query)
				return nil, ErrNotShardeableQuery(concrete.Query)
			}
		}

		if err != nil {
			return nil, err
		}

		n := len(shards)
		if n > 1 {
			return nil, errors.New("Unexpected shard length: " + strconv.Itoa(n))
		}

		shard := shards[0]
		if shard.LogxyShard == nil {
			return nil, errors.New("Unexpected shard type")
		}
		shardUrl, err := url.Parse(shard.LogxyShard.Url)
		if err != nil {
			return nil, errors.New("Unexpected shard url: " + shard.LogxyShard.Url)
		}

		setURL(r, shardUrl.Scheme, shardUrl.Host)

		_ = level.Info(util_log.Logger).Log("msg", "Shardeable Instant Query", "host", r.URL.Host, "time", concrete.TimeTs.Format(time.RFC3339Nano), "query", concrete.Query)

	case *queryrange.LokiRequest:
		shards, _, err := logql.ParseShards(concrete.Shards)

		if len(shards) == 0 {
			if d.defaultDownstreamURL != nil {
				setURL(r, d.defaultDownstreamURL.Scheme, d.defaultDownstreamURL.Host)
				r.Host = ""
				_ = level.Info(util_log.Logger).Log("msg", "Unshardeable Range Query", "host", r.URL.Host, "start", concrete.StartTs.Format(time.RFC3339Nano), "end", concrete.EndTs.Format(time.RFC3339Nano), "query", concrete.Query)
				break
			} else {
				_ = level.Error(util_log.Logger).Log("msg", "Unshardeable Range Query", "host", r.URL.Host, "start", concrete.StartTs.Format(time.RFC3339Nano), "end", concrete.EndTs.Format(time.RFC3339Nano), "query", concrete.Query)
				return nil, ErrNotShardeableQuery(concrete.Query)
			}
		}

		if err != nil {
			return nil, err
		}

		n := len(shards)
		if n > 1 {
			return nil, errors.New("Unexpected shard length: " + strconv.Itoa(n))
		}

		shard := shards[0]
		if shard.LogxyShard == nil {
			return nil, errors.New("Unexpected shard type")
		}
		shardUrl, err := url.Parse(shard.LogxyShard.Url)
		if err != nil {
			return nil, errors.New("Unexpected shard url: " + shard.LogxyShard.Url)
		}

		setURL(r, shardUrl.Scheme, shardUrl.Host)

		_ = level.Info(util_log.Logger).Log("msg", "Shardeable Range Query", "host", r.URL.Host, "start", concrete.StartTs.Format(time.RFC3339Nano), "end", concrete.EndTs.Format(time.RFC3339Nano), "query", concrete.Query)

	default:
		// If the request is not a LokiRequest or LokiInstantRequest, we consider it unshardeable
		// and forward it to the default downstream server if it exists.
		if d.defaultDownstreamURL != nil {
			setURL(r, d.defaultDownstreamURL.Scheme, d.defaultDownstreamURL.Host)
			_ = level.Info(util_log.Logger).Log("msg", "Unshardeable Request", "host", r.URL.Host, "path", r.URL.Path)
		} else {
			_ = level.Error(util_log.Logger).Log("msg", "Unshardeable Request", "host", r.URL.Host, "path", r.URL.Path)
			return nil, ErrNotShardeableReq
		}

	}

	return d.roundtrip(ctx, req, r)
}

func setURL(r *http.Request, schema, host string) {
	r.URL.Scheme = schema
	r.URL.Host = host
	r.Host = ""
}

func (d *downstreamRoundTripper) roundtrip(ctx context.Context, req queryrangebase.Request, r *http.Request) (queryrangebase.Response, error) {
	httpResp, err := d.defaultTransport.RoundTrip(r)
	if err != nil {
		return nil, err
	}

	resp, err := d.codec.DecodeResponse(ctx, httpResp, req)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot convert HTTP response to response for request %s", req.GetQuery())
	}

	return resp, nil
}
