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

type overflowObservation struct {
	uid  string
	at   time.Time
	cold bool
}

type demandSlot struct {
	At    int64 `json:"at"`
	Count int   `json:"n"`
}

type overflowSlot struct {
	At   int64 `json:"at"`
	Cold int   `json:"c"`
	Warm int   `json:"w"`
}

type intentPoint struct {
	At     int64 `json:"at"`
	Intent int   `json:"intent"`
}

type persistedPoolState struct {
	LeadSamplesSeconds []float64 `json:"leadSamplesSeconds,omitempty"`
	// LeadPressure is parallel to LeadSamplesSeconds and records how many
	// cold-start creates were in flight when each sample was taken; samples
	// taken under cold-create pressure are excluded from lead-time learning
	// once enough clean samples exist.
	LeadPressure    []int            `json:"leadPressure,omitempty"`
	ReadyCheckpoint time.Time        `json:"readyCheckpoint,omitempty"`
	ReadyUIDs       []string         `json:"readyUIDs,omitempty"`
	DemandSlots     []demandSlot     `json:"demandSlots,omitempty"`
	DemandSeen      map[string]int64 `json:"demandSeen,omitempty"`
	DemandSince     time.Time        `json:"demandSince,omitempty"`
	IntentHistory   []intentPoint    `json:"intentHistory,omitempty"`
	OverflowSlots   []overflowSlot   `json:"overflowSlots,omitempty"`
	OverflowSeen    map[string]int64 `json:"overflowSeen,omitempty"`
	ZAdjust         float64          `json:"zAdjust,omitempty"`
	ZAdjustAt       int64            `json:"zAdjustAt,omitempty"`
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
	coldPressure int,
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
		pool.LeadPressure = append(pool.LeadPressure, max(0, coldPressure))
		if observation.readyAt.After(newCheckpoint) {
			newCheckpoint = observation.readyAt
		}
	}
	if sampleLimit > 0 && len(pool.LeadSamplesSeconds) > sampleLimit {
		trimFrom := len(pool.LeadSamplesSeconds) - sampleLimit
		pool.LeadSamplesSeconds = append(
			[]float64(nil),
			pool.LeadSamplesSeconds[trimFrom:]...,
		)
		if len(pool.LeadPressure) > trimFrom {
			pool.LeadPressure = append([]int(nil), pool.LeadPressure[trimFrom:]...)
		} else {
			pool.LeadPressure = nil
		}
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
	effective := pool.LeadSamplesSeconds
	if len(pool.LeadPressure) > 0 {
		pressureOffset := len(pool.LeadSamplesSeconds) - len(pool.LeadPressure)
		clean := make([]float64, 0, sampleCount)
		for index, seconds := range pool.LeadSamplesSeconds {
			pressure := 0
			if index >= pressureOffset {
				pressure = pool.LeadPressure[index-pressureOffset]
			}
			if pressure == 0 {
				clean = append(clean, seconds)
			}
		}
		if len(clean) >= minSamples {
			effective = clean
		}
	}
	return sampleQuantile(effective, quantile), sampleCount, true
}

func (s *stateStore) observeDemand(
	poolID string,
	events []demandEvent,
	windowSeconds float64,
	now time.Time,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.state.Pools[poolID]
	if pool == nil {
		pool = &persistedPoolState{}
		s.state.Pools[poolID] = pool
	}
	if pool.DemandSeen == nil {
		pool.DemandSeen = make(map[string]int64)
	}
	slot := time.Minute
	keepAfter := now.Add(-time.Duration(max(3600, 2*int(windowSeconds))) * time.Second)
	dirty := false
	for _, event := range events {
		if event.replicas <= 0 || event.at.IsZero() {
			continue
		}
		uid := event.uid
		if uid == "" {
			uid = event.at.UTC().Format(time.RFC3339Nano) + ":" + itoa(event.replicas)
		}
		if _, seen := pool.DemandSeen[uid]; seen {
			continue
		}
		start := event.at.UTC().Unix()
		width := int64(slot.Seconds())
		if width <= 0 {
			width = 60
		}
		start = start - start%width
		found := false
		for index := range pool.DemandSlots {
			if pool.DemandSlots[index].At == start {
				pool.DemandSlots[index].Count += event.replicas
				found = true
				break
			}
		}
		if !found {
			pool.DemandSlots = append(pool.DemandSlots, demandSlot{At: start, Count: event.replicas})
		}
		pool.DemandSeen[uid] = event.at.Unix()
		if pool.DemandSince.IsZero() || event.at.Before(pool.DemandSince) {
			pool.DemandSince = event.at
		}
		dirty = true
	}
	keptSlots := pool.DemandSlots[:0]
	for _, item := range pool.DemandSlots {
		if time.Unix(item.At, 0).After(keepAfter) {
			keptSlots = append(keptSlots, item)
		} else {
			dirty = true
		}
	}
	pool.DemandSlots = keptSlots
	for uid, at := range pool.DemandSeen {
		if time.Unix(at, 0).Before(keepAfter) {
			delete(pool.DemandSeen, uid)
			dirty = true
		}
	}
	if dirty {
		s.dirty = true
	}
}

// observeOverflow records per-minute cold/warm served counts for claimed
// sandboxes, deduplicated by UID, mirroring the demand-slot bookkeeping.
func (s *stateStore) observeOverflow(
	poolID string,
	observations []overflowObservation,
	windowSeconds float64,
	now time.Time,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.state.Pools[poolID]
	if pool == nil {
		pool = &persistedPoolState{}
		s.state.Pools[poolID] = pool
	}
	if pool.OverflowSeen == nil {
		pool.OverflowSeen = make(map[string]int64)
	}
	width := int64(60)
	keepAfter := now.Add(-time.Duration(max(3600, 2*int(windowSeconds))) * time.Second)
	dirty := false
	for _, observation := range observations {
		if observation.at.IsZero() || observation.uid == "" {
			continue
		}
		if _, seen := pool.OverflowSeen[observation.uid]; seen {
			continue
		}
		start := observation.at.UTC().Unix()
		start -= start % width
		found := false
		for index := range pool.OverflowSlots {
			if pool.OverflowSlots[index].At == start {
				if observation.cold {
					pool.OverflowSlots[index].Cold++
				} else {
					pool.OverflowSlots[index].Warm++
				}
				found = true
				break
			}
		}
		if !found {
			slot := overflowSlot{At: start}
			if observation.cold {
				slot.Cold = 1
			} else {
				slot.Warm = 1
			}
			pool.OverflowSlots = append(pool.OverflowSlots, slot)
		}
		pool.OverflowSeen[observation.uid] = observation.at.Unix()
		dirty = true
	}
	keptSlots := pool.OverflowSlots[:0]
	for _, item := range pool.OverflowSlots {
		if time.Unix(item.At, 0).After(keepAfter) {
			keptSlots = append(keptSlots, item)
		} else {
			dirty = true
		}
	}
	pool.OverflowSlots = keptSlots
	for uid, at := range pool.OverflowSeen {
		if time.Unix(at, 0).Before(keepAfter) {
			delete(pool.OverflowSeen, uid)
			dirty = true
		}
	}
	if dirty {
		s.dirty = true
	}
}

func (s *stateStore) overflowRatio(
	poolID string,
	now time.Time,
	windowSeconds float64,
) (ratio float64, cold int, warm int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pool := s.state.Pools[poolID]
	if pool == nil {
		return 0, 0, 0
	}
	start := now.Add(-time.Duration(windowSeconds * float64(time.Second)))
	for _, slot := range pool.OverflowSlots {
		at := time.Unix(slot.At, 0).UTC()
		if at.After(start) && !at.After(now) {
			cold += slot.Cold
			warm += slot.Warm
		}
	}
	total := cold + warm
	if total <= 0 {
		return 0, cold, warm
	}
	return float64(cold) / float64(total), cold, warm
}

func (s *stateStore) zAdjustment(poolID string) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if pool := s.state.Pools[poolID]; pool != nil {
		return pool.ZAdjust
	}
	return 0
}

// stepZAdjustment applies at most one closed-loop adjustment per cooldown
// window and persists the result so restarts and leader changes do not
// relearn from scratch.
func (s *stateStore) stepZAdjustment(
	poolID string,
	now time.Time,
	cooldownSeconds float64,
	delta float64,
) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.ensurePool(poolID)
	if pool.ZAdjustAt != 0 &&
		now.Sub(time.Unix(pool.ZAdjustAt, 0)) < time.Duration(cooldownSeconds*float64(time.Second)) {
		return pool.ZAdjust
	}
	pool.ZAdjust = min(max(pool.ZAdjust+delta, -2.0), 2.0)
	pool.ZAdjustAt = now.Unix()
	s.dirty = true
	return pool.ZAdjust
}

func (s *stateStore) recentAverageRate(
	poolID string,
	now time.Time,
	windowSeconds float64,
) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if windowSeconds <= 0 {
		return 0
	}
	pool := s.state.Pools[poolID]
	if pool == nil {
		return 0
	}
	start := now.Add(-time.Duration(windowSeconds * float64(time.Second)))
	count := 0
	for _, slot := range pool.DemandSlots {
		at := time.Unix(slot.At, 0).UTC()
		if at.After(start) && !at.After(now) {
			count += slot.Count
		}
	}
	return float64(count) / windowSeconds
}

func itoa(value int) string {
	return fmt.Sprintf("%d", value)
}

func (s *stateStore) rememberIntent(poolID string, now time.Time, intent int, keepSeconds float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.ensurePool(poolID)
	cutoff := now.Add(-time.Duration(max(1.0, keepSeconds) * float64(time.Second)))
	pool.IntentHistory = append(pool.IntentHistory, intentPoint{
		At:     now.Unix(),
		Intent: intent,
	})
	kept := pool.IntentHistory[:0]
	for _, point := range pool.IntentHistory {
		if time.Unix(point.At, 0).After(cutoff) || time.Unix(point.At, 0).Equal(cutoff) {
			kept = append(kept, point)
		}
	}
	pool.IntentHistory = kept
	s.dirty = true
}

func (s *stateStore) resetIntentHistory(poolID string, now time.Time, intent int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.ensurePool(poolID)
	pool.IntentHistory = []intentPoint{{At: now.Unix(), Intent: intent}}
	s.dirty = true
}

func (s *stateStore) stabilizedIntentFloor(poolID string, fallback int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pool := s.state.Pools[poolID]
	if pool == nil || len(pool.IntentHistory) == 0 {
		return fallback
	}
	floor := fallback
	for _, point := range pool.IntentHistory {
		if point.Intent > floor {
			floor = point.Intent
		}
	}
	return floor
}

func (s *stateStore) ensurePool(poolID string) *persistedPoolState {
	pool := s.state.Pools[poolID]
	if pool == nil {
		pool = &persistedPoolState{}
		s.state.Pools[poolID] = pool
	}
	return pool
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
