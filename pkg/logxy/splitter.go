package logxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

func NewLogxySplitterMiddleware(logxy queryrange.LogxyInterface) queryrangebase.Middleware {
	return queryrangebase.MiddlewareFunc(func(next queryrangebase.Handler) queryrangebase.Handler {
		return queryrangebase.HandlerFunc(func(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {

			fmt.Println(".............NewLogxySplitterMiddleware")

			resps := []queryrangebase.Response{}
			for _, c := range logxy.Config().DownstreamServers {

				newCtx := context.WithValue(ctx, "donwstream_roundtring_url", c.Url)
				resp, err := next.Do(newCtx, req)
				if err != nil {
					return nil, err
				}
				resps = append(resps, resp)

			}
			if len(resps) == 0 {
				return nil, errors.New("Logxy splitter didnt found combined data for: " + req.String())
			}

			return merge(resps)
		})
	})
}

func merge(resps []queryrangebase.Response) (queryrangebase.Response, error) {

	switch concrete := resps[0].(type) {

	case *queryrange.IndexStatsResponse:
		return mergeIndexStats(resps)
	case *queryrange.LokiLabelNamesResponse:
		return mergeLabelNames(resps)

	default:
		return nil, errors.New("Unsupported type" + concrete.String())
	}

}

func mergeIndexStats(resps []queryrangebase.Response) (queryrangebase.Response, error) {
	combined := logproto.IndexStatsResponse{
		Streams: 0,
		Chunks:  0,
		Bytes:   0,
		Entries: 0,
	}

	for _, r := range resps {
		indexStatsRes, ok := r.(*queryrange.IndexStatsResponse)
		if !ok {
			return nil, errors.New("error merging index stats")
		}
		combined.Streams += indexStatsRes.Response.Streams
		combined.Chunks += indexStatsRes.Response.Chunks
		combined.Bytes += indexStatsRes.Response.Bytes
		combined.Entries += indexStatsRes.Response.Entries
	}
	return &queryrange.IndexStatsResponse{
		Response: &combined,
		Headers:  httpResponseHeadersToPromResponseHeaders(http.Header{}), // TODO: headers
	}, nil
}

func mergeLabelNames(resps []queryrangebase.Response) (queryrangebase.Response, error) {
	combinedMap := map[string]int{}

	for _, r := range resps {
		labelResponse, ok := r.(*queryrange.LokiLabelNamesResponse)
		if !ok {
			return nil, errors.New("error merging label names")
		}
		for _, label := range labelResponse.Data {
			combinedMap[label]++
		}
	}

	combined := make([]string, 0, len(combinedMap))
	for k := range combinedMap {
		combined = append(combined, k)
	}

	return &queryrange.LokiLabelNamesResponse{
		Data:    combined,
		Status:  "success",
		Version: uint32(loghttp.VersionV1),
		Headers: httpResponseHeadersToPromResponseHeaders(http.Header{}), // TODO: headers
	}, nil
}

func httpResponseHeadersToPromResponseHeaders(httpHeaders http.Header) []queryrangebase.PrometheusResponseHeader {
	var promHeaders []queryrangebase.PrometheusResponseHeader
	for h, hv := range httpHeaders {
		promHeaders = append(promHeaders, queryrangebase.PrometheusResponseHeader{Name: h, Values: hv})
	}

	return promHeaders
}
