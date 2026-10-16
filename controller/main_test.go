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
	target := calculateTarget(2, 40, 10, quantileZ(0.99), 2)
	if target < 82 || target > 84 {
		t.Fatalf("expected approximately 83 replicas, got %d", target)
	}
}

func TestConfigSupportsRequestedLegacyAnnotations(t *testing.T) {
	annotations := map[string]string{
		"dynamic-pool": "true",
		"min-scale":    "0",
		"max-scale":    "10",
		"pool-autoscaling.bohr.io/queue-boost-factor": "0.25",
		"pool-autoscaling.bohr.io/queue-boost-max":    "3",
		"pool-autoscaling.bohr.io/max-scale-up-step":  "4",
	}
	if !dynamicPoolEnabled(annotations) {
		t.Fatal("dynamic-pool=true should enable the controller")
	}
	config := configFromAnnotations(annotations)
	if config.minReplicas != 0 || config.maxReplicas != 10 {
		t.Fatalf("unexpected bounds: min=%d max=%d", config.minReplicas, config.maxReplicas)
	}
	if config.queueBoostFactor != 0.25 || config.queueBoostMax != 3 || config.maxScaleUpStep != 4 {
		t.Fatalf("unexpected surge tuning: %+v", config)
	}
	if config.idleToMinSeconds != defaultIdleToMin ||
		config.scaleDownStabilization != defaultScaleDownStabilization ||
		config.scaleDownFraction != defaultScaleDownFraction {
		t.Fatalf("unexpected scale-down defaults: %+v", config)
	}
}

func TestConfigReadsScaleDownTuningAnnotations(t *testing.T) {
	config := configFromAnnotations(map[string]string{
		idleToMinSecondsKey:       "90",
		scaleDownStabilizationKey: "240",
		scaleDownFractionKey:      "0.08",
	})
	if config.idleToMinSeconds != 90 ||
		config.scaleDownStabilization != 240 ||
		config.scaleDownFraction != 0.08 {
		t.Fatalf("unexpected scale-down tuning: %+v", config)
	}
}

func TestIntentHistoryStabilizesScaleDownFloor(t *testing.T) {
	store := newStateStore(nil, "")
	now := time.Date(2026, 8, 18, 3, 0, 0, 0, time.UTC)
	store.rememberIntent("pool", now, 10, 180)
	store.rememberIntent("pool", now.Add(30*time.Second), 1, 180)
	if got := store.stabilizedIntentFloor("pool", 1); got != 10 {
		t.Fatalf("expected 180s window to keep peak intent 10, got %d", got)
	}
	store.rememberIntent("pool", now.Add(181*time.Second), 1, 180)
	if got := store.stabilizedIntentFloor("pool", 1); got != 1 {
		t.Fatalf("expected expired peak intent to drop, got %d", got)
	}
}

func TestApplyGradualScaleDownHoldsUntilStabilized(t *testing.T) {
	if got := applyGradualScaleDown(10, 1, 10, 0, 0.05); got != 10 {
		t.Fatalf("expected hold at 10 while stabilization window is high, got %d", got)
	}
	if got := applyGradualScaleDown(10, 1, 10, 3, 0.05); got != 10 {
		t.Fatalf("expected hold at 10 while pending demand exists, got %d", got)
	}
	if got := applyGradualScaleDown(10, 1, 1, 0, 0.05); got != 9 {
		t.Fatalf("expected 5%% shrink from 10 to 9, got %d", got)
	}
}

func TestCalculateTargetIsClampedByPoolBudget(t *testing.T) {
	config := configFromAnnotations(map[string]string{
		"min-scale": "0",
		"max-scale": "10",
	})
	raw := calculateTarget(100, 40, 10, quantileZ(0.99), 2)
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
	if added := store.observeReady("default/pool@uid", observations, 128, 0); added != 2 {
		t.Fatalf("expected two new samples, got %d", added)
	}
	lead, count, learned := store.leadTime("default/pool@uid", 0.95, 2, 40)
	if !learned || count != 2 || lead < 3.8 || lead > 4 {
		t.Fatalf("unexpected learned lead: learned=%t count=%d lead=%f", learned, count, lead)
	}
	if added := store.observeReady("default/pool@uid", observations, 128, 0); added != 0 {
		t.Fatalf("ready UIDs at the checkpoint must not be sampled twice, got %d", added)
	}
}

func TestSupplyGatePreservesIntentWhileCreationSlotsAreFull(t *testing.T) {
	if applied := applySupplyGate(20, 0, 0, 0, 10, 0.8, 0, false); applied != 8 {
		t.Fatalf("expected first gated step to 8, got %d", applied)
	}
	if applied := applySupplyGate(20, 8, 8, 8, 10, 0.8, 0, false); applied != 8 {
		t.Fatalf("expected full creation budget to hold at 8, got %d", applied)
	}
	if applied := applySupplyGate(20, 8, 8, 0, 10, 0.8, 0, false); applied != 16 {
		t.Fatalf("expected next gated step to 16, got %d", applied)
	}
	if applied := applySupplyGate(20, 8, 8, 0, 10, 0.8, 0, true); applied != 8 {
		t.Fatalf("expected timed-out supply to freeze at 8, got %d", applied)
	}
}

func TestSupplyGateLimitsEachScaleUpStep(t *testing.T) {
	if applied := applySupplyGate(50, 2, 2, 0, 0, 0.75, 4, false); applied != 6 {
		t.Fatalf("expected unlimited supply to remain step-limited at 6, got %d", applied)
	}
}

func TestTrafficFloorUsesHourlyAverageThenMinReplicas(t *testing.T) {
	if got := calculateTrafficFloor(1.0/3600.0, 23.5, 1); got != 1 {
		t.Fatalf("one request in an hour should keep min=1, got %d", got)
	}
	if got := calculateTrafficFloor(1.0/3600.0, 23.5, 0); got != 0 {
		t.Fatalf("sparse hour raw floor should be 0 when min=0, got %d", got)
	}
	if got := calculateTrafficFloor(400.0/3600.0, 23.5, 1); got != 3 {
		t.Fatalf("expected hourly floor 3, got %d", got)
	}
}

func TestQueueBoostIsWeightedAndCapped(t *testing.T) {
	if got := calculateQueueBoost(8, 0.5, 10); got != 4 {
		t.Fatalf("expected weighted queue boost 4, got %d", got)
	}
	if got := calculateQueueBoost(100, 0.5, 10); got != 10 {
		t.Fatalf("expected queue boost cap 10, got %d", got)
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
	}}, 128, 0)
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

	if err := ctrl.reconcilePool(context.Background(), set, nil, nil, 0, 0, nil, 0, now); err != nil {
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

func TestReconcilePoolDropsDirectlyToConfiguredMinimumAfterIdle(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	set := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.kruise.io/v1alpha1",
		"kind":       "SandboxSet",
		"metadata": map[string]any{
			"name":              "idle-pool",
			"namespace":         "default",
			"uid":               "idle-pool-uid",
			"creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339),
			"annotations": map[string]any{
				enabledKey:          "true",
				minReplicasKey:      "37",
				maxReplicasKey:      "1000",
				idleToMinSecondsKey: "30",
			},
		},
		"spec": map[string]any{"replicas": int64(500)},
		"status": map[string]any{
			"replicas":          int64(500),
			"availableReplicas": int64(500),
		},
	}}
	set.SetUID(types.UID("idle-pool-uid"))
	set.SetCreationTimestamp(metav1.NewTime(now.Add(-time.Hour)))
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), set)
	ctrl := &controller{
		client:  dynamicClient,
		state:   newStateStore(fake.NewSimpleClientset(), "sandbox-system"),
		metrics: newMetricsRegistry(),
	}

	if err := ctrl.reconcilePool(context.Background(), set, nil, nil, 0, 0, nil, 0, now); err != nil {
		t.Fatalf("reconcile idle pool: %v", err)
	}
	updated, err := dynamicClient.Resource(sandboxSetGVR).Namespace("default").Get(
		context.Background(), "idle-pool", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated SandboxSet: %v", err)
	}
	replicas, _, _ := unstructured.NestedInt64(updated.Object, "spec", "replicas")
	if replicas != 37 {
		t.Fatalf("idle pool should drop directly to min-scale=37, got %d", replicas)
	}
	if state := updated.GetAnnotations()[controllerStateKey]; state != "IDLE" {
		t.Fatalf("idle pool should report IDLE state, got %q", state)
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
	if !claimCreatesOnNoStock(pending) {
		t.Fatal("createOnNoStock must default to true when omitted")
	}
	_ = unstructured.SetNestedField(pending.Object, false, "spec", "createOnNoStock")
	if claimCreatesOnNoStock(pending) {
		t.Fatal("explicit createOnNoStock=false must require pool inventory")
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

	if err := ctrl.reconcilePool(context.Background(), set, nil, nil, 12, 0, nil, 0, now); err != nil {
		t.Fatalf("reconcile queued demand: %v", err)
	}
	updated, err := dynamicClient.Resource(sandboxSetGVR).Namespace("default").Get(
		context.Background(), "queue-aware", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated SandboxSet: %v", err)
	}
	replicas, _, _ := unstructured.NestedInt64(updated.Object, "spec", "replicas")
	if replicas != 8 {
		t.Fatalf("weighted queue debt 6 plus frozen guard 2 should wake pool to 8, got %d", replicas)
	}
	annotations := updated.GetAnnotations()
	if annotations[controllerStateKey] != "SURGE" ||
		annotations[pendingObservedKey] != "12" ||
		annotations[queueBoostKey] != "6" ||
		annotations[intentKey] != "8" {
		t.Fatalf("unexpected queue-aware decision annotations: %#v", annotations)
	}
}

func TestOverflowClassifierUsesUpstreamPriorityMarker(t *testing.T) {
	base := func() *unstructured.Unstructured {
		sandbox := &unstructured.Unstructured{}
		sandbox.SetAPIVersion("agents.kruise.io/v1alpha1")
		sandbox.SetKind("Sandbox")
		sandbox.SetNamespace("default")
		sandbox.SetName("sandbox")
		sandbox.SetUID(types.UID("sandbox-uid"))
		sandbox.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-time.Minute)))
		return sandbox
	}
	cold := base()
	cold.SetLabels(map[string]string{sandboxClaimedLabel: "true", sandboxPoolLabel: "pool"})
	cold.SetAnnotations(map[string]string{sandboxPriorityKey: coldPriorityValue})
	observation, found := overflowObservationFromSandbox(cold)
	if !found || !observation.cold {
		t.Fatalf("priority=100 claimed sandbox must classify as cold: %+v", observation)
	}

	warm := base()
	warm.SetLabels(map[string]string{sandboxClaimedLabel: "true", sandboxPoolLabel: "pool"})
	observation, found = overflowObservationFromSandbox(warm)
	if !found || observation.cold {
		t.Fatalf("claimed sandbox without priority marker must classify as warm: %+v", observation)
	}

	idle := base()
	idle.SetLabels(map[string]string{sandboxPoolLabel: "pool"})
	if _, found := overflowObservationFromSandbox(idle); found {
		t.Fatal("unclaimed pool sandbox must not produce an overflow observation")
	}
}

func TestOverflowRatioWindowsAndDeduplicatesByUID(t *testing.T) {
	store := newStateStore(nil, "")
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	observations := []overflowObservation{
		{uid: "a", at: now.Add(-30 * time.Second), cold: true},
		{uid: "b", at: now.Add(-20 * time.Second), cold: false},
		{uid: "a", at: now.Add(-30 * time.Second), cold: true},
		{uid: "old", at: now.Add(-2 * time.Hour), cold: true},
	}
	store.observeOverflow("pool", observations, 900, now)
	ratio, cold, warm := store.overflowRatio("pool", now, 900)
	if cold != 1 || warm != 1 {
		t.Fatalf("expected deduplicated cold=1 warm=1 in window, got cold=%d warm=%d", cold, warm)
	}
	if ratio != 0.5 {
		t.Fatalf("expected overflow ratio 0.5, got %f", ratio)
	}
	ratio, cold, warm = store.overflowRatio("pool", now, 10)
	if cold != 0 || warm != 0 || ratio != 0 {
		t.Fatalf("narrow window should be empty, got cold=%d warm=%d ratio=%f", cold, warm, ratio)
	}
}

func TestStepZAdjustmentRespectsCooldownAndClamp(t *testing.T) {
	store := newStateStore(nil, "")
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	first := store.stepZAdjustment("pool", now, 180, 0.15)
	if first != 0.15 {
		t.Fatalf("first step should apply +0.15, got %f", first)
	}
	quick := store.stepZAdjustment("pool", now.Add(60*time.Second), 180, 0.15)
	if quick != 0.15 {
		t.Fatalf("step inside cooldown must not change adjustment, got %f", quick)
	}
	for index := 0; index < 20; index++ {
		store.stepZAdjustment(
			"pool", now.Add(time.Duration(181*(index+1))*time.Second), 180, 0.15,
		)
	}
	if got := store.zAdjustment("pool"); got != 2.0 {
		t.Fatalf("adjustment must clamp at +2.0, got %f", got)
	}
}

func TestEffectiveDrainFractionBoostsOnlyWhenActive(t *testing.T) {
	if got := effectiveDrainFraction(0.05, 3.0, false); got != 0.05 {
		t.Fatalf("inactive boost must keep base fraction, got %f", got)
	}
	if got := effectiveDrainFraction(0.05, 3.0, true); got < 0.149 || got > 0.151 {
		t.Fatalf("active boost should reach 0.15, got %f", got)
	}
	if got := effectiveDrainFraction(0.15, 3.0, true); got != drainFractionCap {
		t.Fatalf("boost must cap at %f, got %f", drainFractionCap, got)
	}
	if got := effectiveDrainFraction(0.05, 1.0, true); got != 0.05 {
		t.Fatalf("boost max of 1 must be a no-op, got %f", got)
	}
}

func TestOverflowBudgetLowersSafetyStock(t *testing.T) {
	strict := calculateTarget(2, 40, 10, quantileZ(0.99), 2)
	tolerant := calculateTarget(2, 40, 10, quantileZ(1-defaultOverflowBudget), 2)
	if tolerant >= strict {
		t.Fatalf("5%% overflow budget should size below P99: strict=%d tolerant=%d", strict, tolerant)
	}
	config := configFromAnnotations(map[string]string{})
	if config.overflowBudget != defaultOverflowBudget ||
		config.overflowWindow != defaultOverflowWindow ||
		config.minOverflowSamples != defaultMinOverflowSamples ||
		config.drainBoostMax != defaultDrainBoostMax {
		t.Fatalf("unexpected cold-compatible defaults: %+v", config)
	}
	legacy := configFromAnnotations(map[string]string{overflowBudgetKey: "0"})
	if legacy.overflowBudget != 0 {
		t.Fatalf("explicit overflow-budget=0 must disable the cold-compatible mode, got %f", legacy.overflowBudget)
	}
}

func TestLeadTimeExcludesSamplesTakenUnderColdPressure(t *testing.T) {
	store := newStateStore(nil, "")
	created := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	pressured := []supplyObservation{{
		uid: "slow-cold", createdAt: created, readyAt: created.Add(90 * time.Second), ready: true,
	}}
	store.observeReady("pool", pressured, 128, 4)
	clean := []supplyObservation{{
		uid: "fast-clean", createdAt: created.Add(3 * time.Minute),
		readyAt: created.Add(3*time.Minute + 5*time.Second), ready: true,
	}, {
		uid: "fast-clean-2", createdAt: created.Add(4 * time.Minute),
		readyAt: created.Add(4*time.Minute + 6*time.Second), ready: true,
	}}
	store.observeReady("pool", clean, 128, 0)
	lead, count, learned := store.leadTime("pool", 0.95, 2, 40)
	if !learned || count != 3 {
		t.Fatalf("expected all samples counted, learned=%t count=%d", learned, count)
	}
	if lead < 4.9 || lead > 6.1 {
		t.Fatalf("clean samples should win once enough exist, got lead=%f", lead)
	}
}
