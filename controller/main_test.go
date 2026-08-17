package main

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCalculateTargetDocumentExample(t *testing.T) {
	target := calculateTarget(2, 40, 10, 0.99, 2)
	if target < 82 || target > 84 {
		t.Fatalf("expected approximately 83 replicas, got %d", target)
	}
}

func TestConfigSupportsRequestedLegacyAnnotations(t *testing.T) {
	annotations := map[string]string{
		"dynamic-pool": "true",
		"min-scale":    "0",
		"max-scale":    "10",
	}
	if !dynamicPoolEnabled(annotations) {
		t.Fatal("dynamic-pool=true should enable the controller")
	}
	config := configFromAnnotations(annotations)
	if config.minReplicas != 0 || config.maxReplicas != 10 {
		t.Fatalf("unexpected bounds: min=%d max=%d", config.minReplicas, config.maxReplicas)
	}
}

func TestCalculateTargetIsClampedByPoolBudget(t *testing.T) {
	config := configFromAnnotations(map[string]string{
		"min-scale": "0",
		"max-scale": "10",
	})
	raw := calculateTarget(100, 40, 10, 0.99, 2)
	if target := clamp(raw, config.minReplicas, config.maxReplicas); target != 10 {
		t.Fatalf("expected max-scale budget 10, got %d", target)
	}
}

func TestLeadTimeSamplesAreLearnedOnceAndPersistedInMemory(t *testing.T) {
	store := newStateStore(nil, "")
	created := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	ready := created.Add(4 * time.Second)
	observations := []supplyObservation{
		{uid: "one", createdAt: created, readyAt: ready, ready: true},
		{uid: "two", createdAt: created.Add(2 * time.Second), readyAt: ready, ready: true},
	}
	if added := store.observeReady("default/pool@uid", observations, 128); added != 2 {
		t.Fatalf("expected two new samples, got %d", added)
	}
	lead, count, learned := store.leadTime("default/pool@uid", 0.95, 2, 40)
	if !learned || count != 2 || lead < 3.8 || lead > 4 {
		t.Fatalf("unexpected learned lead: learned=%t count=%d lead=%f", learned, count, lead)
	}
	if added := store.observeReady("default/pool@uid", observations, 128); added != 0 {
		t.Fatalf("ready UIDs at the checkpoint must not be sampled twice, got %d", added)
	}
}

func TestSupplyGatePreservesIntentWhileCreationSlotsAreFull(t *testing.T) {
	if applied := applySupplyGate(20, 0, 0, 0, 10, 0.8, false); applied != 8 {
		t.Fatalf("expected first gated step to 8, got %d", applied)
	}
	if applied := applySupplyGate(20, 8, 8, 8, 10, 0.8, false); applied != 8 {
		t.Fatalf("expected full creation budget to hold at 8, got %d", applied)
	}
	if applied := applySupplyGate(20, 8, 8, 0, 10, 0.8, false); applied != 16 {
		t.Fatalf("expected next gated step to 16, got %d", applied)
	}
	if applied := applySupplyGate(20, 8, 8, 0, 10, 0.8, true); applied != 8 {
		t.Fatalf("expected timed-out supply to freeze at 8, got %d", applied)
	}
}

func TestMaxUnavailableSupportsPercentages(t *testing.T) {
	set := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"scaleStrategy": map[string]any{"maxUnavailable": "25%"},
		},
	}}
	if got := sandboxSetMaxUnavailable(set, 20); got != 5 {
		t.Fatalf("expected 25%% of 20 to be 5, got %d", got)
	}
}

func TestLeadTimeStateSurvivesControllerRestart(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewSimpleClientset()
	first := newStateStore(kube, "sandbox-system")
	created := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	first.observeReady("default/pool@uid", []supplyObservation{{
		uid: "sandbox", createdAt: created, readyAt: created.Add(7 * time.Second), ready: true,
	}}, 128)
	if err := first.save(ctx); err != nil {
		t.Fatalf("save persisted state: %v", err)
	}

	restarted := newStateStore(kube, "sandbox-system")
	if err := restarted.load(ctx); err != nil {
		t.Fatalf("load persisted state: %v", err)
	}
	lead, count, learned := restarted.leadTime("default/pool@uid", 0.95, 1, 40)
	if !learned || count != 1 || lead != 7 {
		t.Fatalf("state did not survive restart: learned=%t count=%d lead=%f", learned, count, lead)
	}
}

func TestE2BDemandFromSandboxUsesOfficialClaimMetadata(t *testing.T) {
	claimedAt := time.Date(2026, 8, 17, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		labels      map[string]string
		annotations map[string]string
		want        bool
	}{
		{
			name: "sandbox-manager claim",
			labels: map[string]string{
				sandboxClaimedLabel: "true",
			},
			annotations: map[string]string{sandboxClaimTimeKey: claimedAt.Format(time.RFC3339)},
			want:        true,
		},
		{
			name: "SandboxClaim-owned sandbox is not double counted",
			labels: map[string]string{
				sandboxClaimedLabel:   "true",
				sandboxClaimNameLabel: "batch-claim",
			},
			annotations: map[string]string{sandboxClaimTimeKey: claimedAt.Format(time.RFC3339)},
		},
		{
			name:        "pool inventory is not demand",
			labels:      map[string]string{sandboxClaimedLabel: "false"},
			annotations: map[string]string{sandboxClaimTimeKey: claimedAt.Format(time.RFC3339)},
		},
		{
			name:        "invalid timestamp is ignored",
			labels:      map[string]string{sandboxClaimedLabel: "true"},
			annotations: map[string]string{sandboxClaimTimeKey: "invalid"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sandbox := &unstructured.Unstructured{}
			sandbox.SetLabels(test.labels)
			sandbox.SetAnnotations(test.annotations)
			event, found := e2bDemandFromSandbox(sandbox)
			if found != test.want {
				t.Fatalf("found=%t, want %t", found, test.want)
			}
			if found && (!event.at.Equal(claimedAt) || event.replicas != 1) {
				t.Fatalf("unexpected event: %+v", event)
			}
		})
	}
}

func TestReconcilePoolRestoresNonZeroMinimumWithoutDemand(t *testing.T) {
	now := time.Date(2026, 8, 6, 9, 0, 0, 0, time.UTC)
	set := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.kruise.io/v1alpha1",
		"kind":       "SandboxSet",
		"metadata": map[string]any{
			"name":              "dynamic-pool",
			"namespace":         "default",
			"uid":               "pool-uid",
			"creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339),
			"annotations": map[string]any{
				enabledKey:     "true",
				minReplicasKey: "1",
				maxReplicasKey: "10",
				safetyGuardKey: "0",
			},
		},
		"spec": map[string]any{"replicas": int64(0)},
		"status": map[string]any{
			"replicas":          int64(0),
			"availableReplicas": int64(0),
		},
	}}
	set.SetUID(types.UID("pool-uid"))
	set.SetCreationTimestamp(metav1.NewTime(now.Add(-time.Hour)))
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), set)
	ctrl := &controller{
		client:  dynamicClient,
		state:   newStateStore(fake.NewSimpleClientset(), "sandbox-system"),
		metrics: newMetricsRegistry(),
	}

	if err := ctrl.reconcilePool(context.Background(), set, nil, nil, 0, now); err != nil {
		t.Fatalf("reconcile minimum inventory: %v", err)
	}
	updated, err := dynamicClient.Resource(sandboxSetGVR).Namespace("default").Get(
		context.Background(), "dynamic-pool", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated SandboxSet: %v", err)
	}
	replicas, found, err := unstructured.NestedInt64(updated.Object, "spec", "replicas")
	if err != nil || !found || replicas != 1 {
		t.Fatalf("managed pool must restore its minimum inventory, replicas=%d found=%t err=%v", replicas, found, err)
	}
}

func TestDemandRateUsesFastMediumAndSlowWindows(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	events := []demandEvent{
		{at: now.Add(-2 * time.Second), replicas: 20},
		{at: now.Add(-30 * time.Second), replicas: 60},
	}
	fast := demandRate(events, now, 5)
	medium := demandRate(events, now, 15)
	slow := demandRate(events, now, 60)
	if fast < 9 || fast > 11 {
		t.Fatalf("unexpected fast window rate: %f", fast)
	}
	if medium < 9 || medium > 11 {
		t.Fatalf("unexpected medium window rate: %f", medium)
	}
	if slow >= fast {
		t.Fatalf("slow window should dilute burst: fast=%f slow=%f", fast, slow)
	}
	if planned := max(fast, medium, slow); planned < 9 || planned > 11 {
		t.Fatalf("planned rate must preserve the burst, got %f", planned)
	}
}

func TestClaimPendingReplicasCountsOnlyActiveDebt(t *testing.T) {
	pending := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"phase":           "Pending",
			"claimedReplicas": int64(2),
		},
	}}
	if got := claimPendingReplicas(pending, 5); got != 3 {
		t.Fatalf("expected three unfulfilled replicas, got %d", got)
	}
	completed := pending.DeepCopy()
	_ = unstructured.SetNestedField(completed.Object, "Completed", "status", "phase")
	if got := claimPendingReplicas(completed, 5); got != 0 {
		t.Fatalf("completed claim must not remain queued, got %d", got)
	}
}

func TestPendingDemandAnnotationRequiresFreshTimestamp(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	fresh := map[string]string{
		pendingDemandKey:     "7",
		pendingDemandTimeKey: now.Add(-2 * time.Second).Format(time.RFC3339Nano),
	}
	pending, updatedAt := pendingDemandFromAnnotations(fresh, now, 30)
	if pending != 7 || updatedAt.IsZero() {
		t.Fatalf("fresh pending signal was not accepted: pending=%d updatedAt=%v", pending, updatedAt)
	}
	stale := map[string]string{
		pendingDemandKey:     "7",
		pendingDemandTimeKey: now.Add(-31 * time.Second).Format(time.RFC3339Nano),
	}
	pending, updatedAt = pendingDemandFromAnnotations(stale, now, 30)
	if pending != 0 || !updatedAt.IsZero() {
		t.Fatalf("stale pending signal must be ignored: pending=%d updatedAt=%v", pending, updatedAt)
	}
}

func TestPendingDemandWakesZeroPoolAndBoostsIntent(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	set := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.kruise.io/v1alpha1",
		"kind":       "SandboxSet",
		"metadata": map[string]any{
			"name":              "queue-aware",
			"namespace":         "default",
			"uid":               "queue-aware-uid",
			"creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339),
			"annotations": map[string]any{
				enabledKey:     "true",
				minReplicasKey: "0",
				maxReplicasKey: "50",
				safetyGuardKey: "0",
			},
		},
		"spec": map[string]any{"replicas": int64(0)},
		"status": map[string]any{
			"replicas":          int64(0),
			"availableReplicas": int64(0),
		},
	}}
	set.SetUID(types.UID("queue-aware-uid"))
	set.SetCreationTimestamp(metav1.NewTime(now.Add(-time.Hour)))
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), set)
	ctrl := &controller{
		client:  dynamicClient,
		state:   newStateStore(fake.NewSimpleClientset(), "sandbox-system"),
		metrics: newMetricsRegistry(),
	}

	if err := ctrl.reconcilePool(context.Background(), set, nil, nil, 12, now); err != nil {
		t.Fatalf("reconcile queued demand: %v", err)
	}
	updated, err := dynamicClient.Resource(sandboxSetGVR).Namespace("default").Get(
		context.Background(), "queue-aware", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated SandboxSet: %v", err)
	}
	replicas, _, _ := unstructured.NestedInt64(updated.Object, "spec", "replicas")
	if replicas != 12 {
		t.Fatalf("queue debt should wake pool directly to 12, got %d", replicas)
	}
	annotations := updated.GetAnnotations()
	if annotations[controllerStateKey] != "SURGE" ||
		annotations[pendingObservedKey] != "12" ||
		annotations[intentKey] != "12" {
		t.Fatalf("unexpected queue-aware decision annotations: %#v", annotations)
	}
}
