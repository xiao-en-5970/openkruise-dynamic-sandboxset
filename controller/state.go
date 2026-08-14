package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	stateConfigMapName = "sandbox-pool-controller-state"
	stateDataKey       = "state.json"
	stateVersion       = 1
)

type supplyObservation struct {
	uid       string
	createdAt time.Time
	readyAt   time.Time
	ready     bool
}

type persistedDemandSample struct {
	At       time.Time `json:"at"`
	Replicas int       `json:"replicas"`
}

type persistedPoolState struct {
	LeadSamplesSeconds  []float64               `json:"leadSamplesSeconds,omitempty"`
	ReadyCheckpoint     time.Time               `json:"readyCheckpoint,omitempty"`
	ReadyUIDs           []string                `json:"readyUIDs,omitempty"`
	ObservedDemandTotal uint64                  `json:"observedDemandTotal,omitempty"`
	DemandSamples       []persistedDemandSample `json:"demandSamples,omitempty"`
	LastDemandTime      time.Time               `json:"lastDemandTime,omitempty"`
}

type persistedControllerState struct {
	Version int                            `json:"version"`
	Pools   map[string]*persistedPoolState `json:"pools"`
}

type stateStore struct {
	mu        sync.RWMutex
	kube      kubernetes.Interface
	namespace string
	state     persistedControllerState
	dirty     bool
}

func newStateStore(kube kubernetes.Interface, namespace string) *stateStore {
	return &stateStore{
		kube:      kube,
		namespace: namespace,
		state: persistedControllerState{
			Version: stateVersion,
			Pools:   make(map[string]*persistedPoolState),
		},
	}
}

func (s *stateStore) load(ctx context.Context) error {
	configMap, err := s.kube.CoreV1().ConfigMaps(s.namespace).Get(
		ctx, stateConfigMapName, metav1.GetOptions{},
	)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get state ConfigMap: %w", err)
	}
	raw := configMap.Data[stateDataKey]
	if raw == "" {
		return nil
	}
	var loaded persistedControllerState
	if err := json.Unmarshal([]byte(raw), &loaded); err != nil {
		return fmt.Errorf("decode state ConfigMap: %w", err)
	}
	if loaded.Version != stateVersion {
		return fmt.Errorf("unsupported state version %d", loaded.Version)
	}
	if loaded.Pools == nil {
		loaded.Pools = make(map[string]*persistedPoolState)
	}
	s.mu.Lock()
	s.state = loaded
	s.dirty = false
	s.mu.Unlock()
	return nil
}

func (s *stateStore) save(ctx context.Context) error {
	s.mu.RLock()
	if !s.dirty {
		s.mu.RUnlock()
		return nil
	}
	payload, err := json.Marshal(s.state)
	s.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("encode controller state: %w", err)
	}

	configMaps := s.kube.CoreV1().ConfigMaps(s.namespace)
	configMap, err := configMaps.Get(ctx, stateConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stateConfigMapName,
				Namespace: s.namespace,
				Labels: map[string]string{
					"app": "sandbox-pool-controller",
				},
			},
			Data: map[string]string{stateDataKey: string(payload)},
		}, metav1.CreateOptions{})
	} else if err == nil {
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data[stateDataKey] = string(payload)
		_, err = configMaps.Update(ctx, configMap, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("persist controller state: %w", err)
	}
	s.mu.Lock()
	s.dirty = false
	s.mu.Unlock()
	return nil
}

func (s *stateStore) observeReady(
	poolID string,
	observations []supplyObservation,
	sampleLimit int,
) (added int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.state.Pools[poolID]
	if pool == nil {
		pool = &persistedPoolState{}
		s.state.Pools[poolID] = pool
	}

	checkpointUIDs := make(map[string]struct{}, len(pool.ReadyUIDs))
	for _, uid := range pool.ReadyUIDs {
		checkpointUIDs[uid] = struct{}{}
	}
	candidates := make([]supplyObservation, 0)
	for _, observation := range observations {
		if !observation.ready || observation.readyAt.Before(observation.createdAt) {
			continue
		}
		if observation.readyAt.Before(pool.ReadyCheckpoint) {
			continue
		}
		if observation.readyAt.Equal(pool.ReadyCheckpoint) {
			if _, found := checkpointUIDs[observation.uid]; found {
				continue
			}
		}
		candidates = append(candidates, observation)
	}
	if len(candidates) == 0 {
		return 0
	}

	newCheckpoint := pool.ReadyCheckpoint
	for _, observation := range candidates {
		leadSeconds := max(1.0, observation.readyAt.Sub(observation.createdAt).Seconds())
		pool.LeadSamplesSeconds = append(pool.LeadSamplesSeconds, leadSeconds)
		if observation.readyAt.After(newCheckpoint) {
			newCheckpoint = observation.readyAt
		}
	}
	if sampleLimit > 0 && len(pool.LeadSamplesSeconds) > sampleLimit {
		pool.LeadSamplesSeconds = append(
			[]float64(nil),
			pool.LeadSamplesSeconds[len(pool.LeadSamplesSeconds)-sampleLimit:]...,
		)
	}
	newUIDs := make([]string, 0)
	for _, observation := range observations {
		if observation.ready && observation.readyAt.Equal(newCheckpoint) {
			newUIDs = append(newUIDs, observation.uid)
		}
	}
	sort.Strings(newUIDs)
	pool.ReadyCheckpoint = newCheckpoint
	pool.ReadyUIDs = newUIDs
	s.dirty = true
	return len(candidates)
}

func (s *stateStore) leadTime(
	poolID string,
	quantile float64,
	minSamples int,
	fallback float64,
) (seconds float64, sampleCount int, learned bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pool := s.state.Pools[poolID]
	if pool == nil {
		return fallback, 0, false
	}
	sampleCount = len(pool.LeadSamplesSeconds)
	if sampleCount < minSamples {
		return fallback, sampleCount, false
	}
	return sampleQuantile(pool.LeadSamplesSeconds, quantile), sampleCount, true
}

// observeDemand converts the monotonic request counter written by
// sandbox-manager into windowed demand samples. Persisting the last observed
// counter prevents controller restarts or leader changes from replaying old
// requests as new demand.
func (s *stateStore) observeDemand(
	poolID string,
	total uint64,
	at time.Time,
	windowStart time.Time,
) (recentReplicas int, latestDemand time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.state.Pools[poolID]
	if pool == nil {
		pool = &persistedPoolState{}
		s.state.Pools[poolID] = pool
	}

	if total != pool.ObservedDemandTotal {
		delta := total
		if total > pool.ObservedDemandTotal {
			delta = total - pool.ObservedDemandTotal
		}
		pool.ObservedDemandTotal = total
		if delta > 0 && !at.IsZero() {
			maxInt := uint64(^uint(0) >> 1)
			if delta > maxInt {
				delta = maxInt
			}
			pool.DemandSamples = append(pool.DemandSamples, persistedDemandSample{
				At:       at,
				Replicas: int(delta),
			})
			if at.After(pool.LastDemandTime) {
				pool.LastDemandTime = at
			}
		}
		s.dirty = true
	}

	retained := pool.DemandSamples[:0]
	for _, sample := range pool.DemandSamples {
		if sample.At.Before(windowStart) {
			s.dirty = true
			continue
		}
		retained = append(retained, sample)
		recentReplicas += sample.Replicas
	}
	pool.DemandSamples = retained
	return recentReplicas, pool.LastDemandTime
}

func (s *stateStore) prune(activePools map[string]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for poolID := range s.state.Pools {
		if _, active := activePools[poolID]; !active {
			delete(s.state.Pools, poolID)
			s.dirty = true
		}
	}
}

func sampleQuantile(samples []float64, quantile float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	position := quantile * float64(len(sorted)-1)
	lower := int(position)
	upper := min(lower+1, len(sorted)-1)
	fraction := position - float64(lower)
	return sorted[lower] + fraction*(sorted[upper]-sorted[lower])
}
