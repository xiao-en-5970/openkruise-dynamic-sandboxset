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

func TestDirectDemandCounterIsConsumedOnceAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewSimpleClientset()
	poolID := "default/pool@uid"
	now := time.Date(2026, 8, 6, 9, 0, 0, 0, time.UTC)
	windowStart := now.Add(-time.Minute)

	first := newStateStore(kube, "sandbox-system")
	recent, latest := first.observeDemand(poolID, 5, now, windowStart)
	if recent != 5 || !latest.Equal(now) {
		t.Fatalf("unexpected first demand observation: recent=%d latest=%s", recent, latest)
	}
	recent, _ = first.observeDemand(poolID, 5, now, windowStart)
	if recent != 5 {
		t.Fatalf("unchanged counter must not be consumed twice, got %d", recent)
	}
	if err := first.save(ctx); err != nil {
		t.Fatalf("save demand state: %v", err)
	}

	restarted := newStateStore(kube, "sandbox-system")
	if err := restarted.load(ctx); err != nil {
		t.Fatalf("load demand state: %v", err)
	}
	recent, _ = restarted.observeDemand(poolID, 5, now, windowStart)
	if recent != 5 {
		t.Fatalf("restart must not replay the persisted counter, got %d", recent)
	}
	recent, _ = restarted.observeDemand(poolID, 7, now.Add(time.Second), windowStart)
	if recent != 7 {
		t.Fatalf("expected only the counter delta to be appended, got %d", recent)
	}
	recent, latest = restarted.observeDemand(poolID, 7, now.Add(time.Second), now.Add(2*time.Second))
	if recent != 0 || !latest.Equal(now.Add(time.Second)) {
		t.Fatalf("expired samples should leave only idle timestamp: recent=%d latest=%s", recent, latest)
	}
}

func TestReconcilePoolActivatesFromDirectDemandWithoutSandboxClaim(t *testing.T) {
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
				enabledKey:        "true",
				maxReplicasKey:    "1",
				safetyGuardKey:    "0",
				demandTotalKey:    "1",
				lastDemandTimeKey: now.Format(time.RFC3339Nano),
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

	if err := ctrl.reconcilePool(context.Background(), set, nil, nil, now); err != nil {
		t.Fatalf("reconcile direct demand: %v", err)
	}
	updated, err := dynamicClient.Resource(sandboxSetGVR).Namespace("default").Get(
		context.Background(), "dynamic-pool", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated SandboxSet: %v", err)
	}
	replicas, found, err := unstructured.NestedInt64(updated.Object, "spec", "replicas")
	if err != nil || !found || replicas != 1 {
		t.Fatalf("direct demand must activate pool 0 -> 1, replicas=%d found=%t err=%v", replicas, found, err)
	}
}

func TestDirectDemandAnnotationsRequireTimestamp(t *testing.T) {
	_, _, err := directDemandFromAnnotations(map[string]string{demandTotalKey: "1"})
	if err == nil {
		t.Fatal("positive direct demand without timestamp must be rejected")
	}
}
