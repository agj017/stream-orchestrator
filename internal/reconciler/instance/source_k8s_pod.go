package instance

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type K8sClientPodSourceConfig struct {
	ExcludeTerminalPods bool
}

type K8sClientPodSource struct {
	client kubernetes.Interface
	cfg    K8sClientPodSourceConfig
}

func NewK8sClientPodSource(client kubernetes.Interface) *K8sClientPodSource {
	return &K8sClientPodSource{
		client: client,
		cfg: K8sClientPodSourceConfig{
			ExcludeTerminalPods: true,
		},
	}
}

func NewK8sClientPodSourceWithConfig(client kubernetes.Interface, cfg K8sClientPodSourceConfig) *K8sClientPodSource {
	return &K8sClientPodSource{
		client: client,
		cfg:    cfg,
	}
}

func (s *K8sClientPodSource) ListMediaPods(ctx context.Context, namespace string, selector string) ([]PodInfo, error) {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	podList, err := s.client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods namespace=%s selector=%s: %w", ns, selector, err)
	}

	out := make([]PodInfo, 0, len(podList.Items))
	for _, pod := range podList.Items {
		if s.cfg.ExcludeTerminalPods && isTerminalPhase(pod.Status.Phase) {
			continue
		}

		out = append(out, PodInfo{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			UID:       string(pod.UID),
			NodeName:  pod.Spec.NodeName,
			PodIP:     pod.Status.PodIP,
			Ready:     isPodReady(pod),
			Labels:    cloneLabels(pod.Labels),
		})
	}
	return out, nil
}

func isPodReady(pod corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func isTerminalPhase(phase corev1.PodPhase) bool {
	return phase == corev1.PodSucceeded || phase == corev1.PodFailed
}

func cloneLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

