package instance

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"stream-orchestrator/internal/domain"
)

type PodInfo struct {
	Namespace string
	Name      string
	UID       string
	NodeName  string
	PodIP     string
	Ready     bool
	Labels    map[string]string
}

type MediaMetrics struct {
	CurrentStreams    int
	UsedBandwidthMbps float64
	MaxStreams        int
	MaxBandwidthMbps  float64
}

type NodeMetrics struct {
	CPUUsagePct float64
	MemUsagePct float64
	Region      string
	Zone        string
}

type K8sPodSource interface {
	ListMediaPods(ctx context.Context, namespace string, selector string) ([]PodInfo, error)
}

type MediaMetricsSource interface {
	FetchByPod(ctx context.Context, pod PodInfo) (MediaMetrics, error)
}

type NodeMetricsSource interface {
	FetchByNode(ctx context.Context, nodeName string) (NodeMetrics, error)
}

type K8sMediaMTXCollectorConfig struct {
	Namespace            string
	Selector             string
	Provider             string
	CollectTimeout       time.Duration
	CollectConcurrency   int
	DefaultMaxStreams    int
	DefaultMaxBandwidth  float64
	DrainingLabelKey     string
	DrainingLabelValue   string
}

type K8sMediaMTXCollector struct {
	podSource         K8sPodSource
	mediaMetrics      MediaMetricsSource
	nodeMetrics       NodeMetricsSource
	cfg               K8sMediaMTXCollectorConfig
	now               func() time.Time
}

func NewK8sMediaMTXCollector(
	podSource K8sPodSource,
	mediaMetrics MediaMetricsSource,
	nodeMetrics NodeMetricsSource,
	cfg K8sMediaMTXCollectorConfig,
) *K8sMediaMTXCollector {
	if cfg.Provider == "" {
		cfg.Provider = "mediamtx"
	}
	if cfg.CollectTimeout <= 0 {
		cfg.CollectTimeout = 5 * time.Second
	}
	if cfg.CollectConcurrency <= 0 {
		cfg.CollectConcurrency = 10
	}
	if cfg.DrainingLabelKey == "" {
		cfg.DrainingLabelKey = "orchestrator/draining"
	}
	if cfg.DrainingLabelValue == "" {
		cfg.DrainingLabelValue = "true"
	}

	return &K8sMediaMTXCollector{
		podSource:    podSource,
		mediaMetrics: mediaMetrics,
		nodeMetrics:  nodeMetrics,
		cfg:          cfg,
		now:          time.Now().UTC,
	}
}

func (c *K8sMediaMTXCollector) Collect(ctx context.Context) ([]domain.StreamInstance, error) {
	pods, err := c.podSource.ListMediaPods(ctx, c.cfg.Namespace, c.cfg.Selector)
	if err != nil {
		return nil, fmt.Errorf("list media pods: %w", err)
	}
	if len(pods) == 0 {
		return []domain.StreamInstance{}, nil
	}

	type result struct {
		instance domain.StreamInstance
		err      error
	}

	in := make(chan PodInfo)
	out := make(chan result)
	var wg sync.WaitGroup

	workers := c.cfg.CollectConcurrency
	if workers > len(pods) {
		workers = len(pods)
	}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for pod := range in {
				inst, buildErr := c.buildInstance(ctx, pod)
				out <- result{instance: inst, err: buildErr}
			}
		}()
	}

	go func() {
		for _, p := range pods {
			in <- p
		}
		close(in)
		wg.Wait()
		close(out)
	}()

	instances := make([]domain.StreamInstance, 0, len(pods))
	for r := range out {
		// Keep best-effort behavior:
		// if one pod fails metric collection, we still keep other instances.
		if r.err == nil {
			instances = append(instances, r.instance)
		}
	}
	return instances, nil
}

func (c *K8sMediaMTXCollector) buildInstance(ctx context.Context, pod PodInfo) (domain.StreamInstance, error) {
	tctx, cancel := context.WithTimeout(ctx, c.cfg.CollectTimeout)
	defer cancel()

	metrics, mmErr := c.mediaMetrics.FetchByPod(tctx, pod)
	node, nmErr := c.nodeMetrics.FetchByNode(tctx, pod.NodeName)

	health := domain.InstanceHealthHealthy
	if !pod.Ready || mmErr != nil || nmErr != nil {
		health = domain.InstanceHealthUnhealthy
	}
	if isDraining(pod.Labels, c.cfg.DrainingLabelKey, c.cfg.DrainingLabelValue) {
		health = domain.InstanceHealthDraining
	}

	maxStreams := metrics.MaxStreams
	if maxStreams == 0 {
		maxStreams = c.cfg.DefaultMaxStreams
	}
	maxBandwidth := metrics.MaxBandwidthMbps
	if maxBandwidth == 0 {
		maxBandwidth = c.cfg.DefaultMaxBandwidth
	}

	caps, _ := json.Marshal(map[string]string{
		"k8s_node_name": pod.NodeName,
		"pod_uid":       pod.UID,
	})

	instance := domain.StreamInstance{
		ID:                    deterministicID(c.cfg.Provider, pod.Namespace, pod.Name),
		NodeName:              pod.Name,
		Provider:              c.cfg.Provider,
		K8sNamespace:          pod.Namespace,
		K8sPodName:            pod.Name,
		Region:                node.Region,
		Zone:                  node.Zone,
		HealthStatus:          health,
		MaxStreams:            maxStreams,
		CurrentStreams:        metrics.CurrentStreams,
		ReservedStreams:       0,
		MaxBandwidthMbps:      maxBandwidth,
		UsedBandwidthMbps:     metrics.UsedBandwidthMbps,
		ReservedBandwidthMbps: 0,
		CPUUsagePct:           node.CPUUsagePct,
		MemUsagePct:           node.MemUsagePct,
		Capabilities:          caps,
		LastHeartbeatAt:       c.now(),
	}
	return instance, nil
}

func isDraining(labels map[string]string, key, value string) bool {
	if len(labels) == 0 {
		return false
	}
	return labels[key] == value
}

func deterministicID(provider, namespace, podName string) string {
	h := sha1.Sum([]byte(provider + ":" + namespace + ":" + podName))
	hexStr := hex.EncodeToString(h[:16])
	// UUID-like formatting for compatibility with UUID columns.
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexStr[0:8], hexStr[8:12], hexStr[12:16], hexStr[16:20], hexStr[20:32])
}

