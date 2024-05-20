package logxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-kit/log/level"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

// RoundTripper that forwards requests to downstream URL.
type downstreamRoundTripper struct {
	downstreamURL *url.URL
	transport     http.RoundTripper
	codec         queryrangebase.Codec
	logxyConfig   queryrange.LogxyConfig
}

func NewDownstreamRoundTripper(downstreamURL string, transport http.RoundTripper, logxyconfig queryrange.LogxyConfig) (queryrangebase.Handler, error) {
	u, err := url.Parse(downstreamURL)
	if err != nil {
		return nil, err
	}

	return &downstreamRoundTripper{
		downstreamURL: u,
		transport:     transport,
		codec:         queryrange.DefaultCodec,
		logxyConfig:   logxyconfig,
	}, nil
}

func (d downstreamRoundTripper) Do(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
	tracer, span := opentracing.GlobalTracer(), opentracing.SpanFromContext(ctx)

	var r *http.Request

	r, err := d.codec.EncodeRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("connot convert request ot HTTP request: %w", err)
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
			r.URL.Scheme = d.downstreamURL.Scheme
			r.URL.Host = d.downstreamURL.Host
			r.URL.Path = path.Join(d.downstreamURL.Path, r.URL.Path)
			r.Host = ""
			level.Info(util_log.Logger).Log("msg", "Unshardeable Instant Query", "host", r.URL.Host, "time", concrete.TimeTs.Format(time.RFC3339Nano), "query", concrete.Query)
			break
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

		r.URL.Scheme = shardUrl.Scheme
		r.URL.Host = shardUrl.Host
		r.URL.Path = path.Join(d.downstreamURL.Path, r.URL.Path)
		r.Host = ""

		level.Info(util_log.Logger).Log("msg", "Shardeable Instant Query", "host", r.URL.Host, "time", concrete.TimeTs.Format(time.RFC3339Nano), "query", concrete.Query)
	case *queryrange.LokiRequest:
		shards, _, err := logql.ParseShards(concrete.Shards)

		if len(shards) == 0 {
			r.URL.Scheme = d.downstreamURL.Scheme
			r.URL.Host = d.downstreamURL.Host
			r.URL.Path = path.Join(d.downstreamURL.Path, r.URL.Path)
			r.Host = ""
			level.Info(util_log.Logger).Log("msg", "Unshardeable Range Query", "host", r.URL.Host, "start", concrete.StartTs.Format(time.RFC3339Nano), "end", concrete.EndTs.Format(time.RFC3339Nano), "query", concrete.Query)
			break
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
			fmt.Println(shard)
			return nil, errors.New("Unexpected shard type")
		}
		shardUrl, err := url.Parse(shard.LogxyShard.Url)
		if err != nil {
			return nil, errors.New("Unexpected shard url: " + shard.LogxyShard.Url)
		}

		r.URL.Scheme = shardUrl.Scheme
		r.URL.Host = shardUrl.Host
		r.URL.Path = path.Join(d.downstreamURL.Path, r.URL.Path)
		r.Host = ""

		level.Info(util_log.Logger).Log("msg", "Shardeable Range Query", "host", r.URL.Host, "start", concrete.StartTs.Format(time.RFC3339Nano), "end", concrete.EndTs.Format(time.RFC3339Nano), "query", concrete.Query)

	default:

		r.URL.Scheme = d.downstreamURL.Scheme
		r.URL.Host = d.downstreamURL.Host
		r.URL.Path = path.Join(d.downstreamURL.Path, r.URL.Path)
		r.Host = ""

		level.Info(util_log.Logger).Log("Request not shareable", "Downstream URL", r.URL.String())
		if drUrl, ok := ctx.Value("donwstream_roundtring_url").(string); ok {
			logxyUrl, err := url.Parse(drUrl)
			if err == nil {
				r.URL.Scheme = logxyUrl.Scheme
				r.URL.Host = logxyUrl.Host
				r.URL.Path = path.Join(logxyUrl.Path, r.URL.Path)
				r.Host = ""
				level.Info(util_log.Logger).Log("Request not shareable", "Downstream URL overwrite to", r.URL.String())
			}

		}

	}

	return roundtrip(ctx, d.transport, d.codec, req, r)
}

func roundtrip(ctx context.Context, transport http.RoundTripper, codec queryrangebase.Codec, req queryrangebase.Request, r *http.Request) (queryrangebase.Response, error) {
	httpResp, err := transport.RoundTrip(r)
	if err != nil {
		return nil, err
	}

	resp, err := codec.DecodeResponse(ctx, httpResp, req)
	if err != nil {
		return nil, fmt.Errorf("cannot convert HTTP response to response: %w", err)
	}

	return resp, nil
}
