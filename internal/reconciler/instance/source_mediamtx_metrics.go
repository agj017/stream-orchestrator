package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type MediaMTXHTTPMetricsSourceConfig struct {
	Port                int
	Path                string
	Timeout             time.Duration
	DefaultMaxStreams   int
	DefaultMaxBandwidth float64
}

type MediaMTXHTTPMetricsSource struct {
	client *http.Client
	cfg    MediaMTXHTTPMetricsSourceConfig
}

func NewMediaMTXHTTPMetricsSource(cfg MediaMTXHTTPMetricsSourceConfig) *MediaMTXHTTPMetricsSource {
	if cfg.Port == 0 {
		cfg.Port = 9997
	}
	if cfg.Path == "" {
		cfg.Path = "/v1/metrics"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}

	return &MediaMTXHTTPMetricsSource{
		client: &http.Client{Timeout: cfg.Timeout},
		cfg:    cfg,
	}
}

func (s *MediaMTXHTTPMetricsSource) FetchByPod(ctx context.Context, pod PodInfo) (MediaMetrics, error) {
	if strings.TrimSpace(pod.PodIP) == "" {
		return MediaMetrics{}, fmt.Errorf("pod ip is empty for pod=%s/%s", pod.Namespace, pod.Name)
	}

	url := fmt.Sprintf("http://%s:%d%s", pod.PodIP, s.cfg.Port, s.cfg.Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return MediaMetrics{}, fmt.Errorf("create request: %w", err)
	}

	res, err := s.client.Do(req)
	if err != nil {
		return MediaMetrics{}, fmt.Errorf("http request failed: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return MediaMetrics{}, fmt.Errorf("metrics endpoint status=%d", res.StatusCode)
	}

	// Expected JSON payload example:
	// {
	//   "current_streams": 12,
	//   "used_bandwidth_mbps": 152.3,
	//   "max_streams": 500,
	//   "max_bandwidth_mbps": 5000
	// }
	var payload struct {
		CurrentStreams    int     `json:"current_streams"`
		UsedBandwidthMbps float64 `json:"used_bandwidth_mbps"`
		MaxStreams        int     `json:"max_streams"`
		MaxBandwidthMbps  float64 `json:"max_bandwidth_mbps"`
	}

	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return MediaMetrics{}, fmt.Errorf("decode metrics json: %w", err)
	}

	maxStreams := payload.MaxStreams
	if maxStreams == 0 {
		maxStreams = s.cfg.DefaultMaxStreams
	}
	maxBandwidth := payload.MaxBandwidthMbps
	if maxBandwidth == 0 {
		maxBandwidth = s.cfg.DefaultMaxBandwidth
	}

	return MediaMetrics{
		CurrentStreams:    payload.CurrentStreams,
		UsedBandwidthMbps: payload.UsedBandwidthMbps,
		MaxStreams:        maxStreams,
		MaxBandwidthMbps:  maxBandwidth,
	}, nil
}

