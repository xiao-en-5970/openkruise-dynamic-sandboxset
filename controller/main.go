package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	sandboxSetGVR = schema.GroupVersionResource{
		Group: "agents.kruise.io", Version: "v1alpha1", Resource: "sandboxsets",
	}
	sandboxClaimGVR = schema.GroupVersionResource{
		Group: "agents.kruise.io", Version: "v1alpha1", Resource: "sandboxclaims",
	}
	sandboxGVR = schema.GroupVersionResource{
		Group: "agents.kruise.io", Version: "v1alpha1", Resource: "sandboxes",
	}
)

const (
	enabledKey                = "pool-autoscaling.bohr.io/enabled"
	minReplicasKey            = "pool-autoscaling.bohr.io/min-replicas"
	maxReplicasKey            = "pool-autoscaling.bohr.io/max-replicas"
	slaSecondsKey             = "pool-autoscaling.bohr.io/sla-seconds"
	refillSecondsKey          = "pool-autoscaling.bohr.io/refill-seconds"
	pendingDemandKey          = "pool-autoscaling.bohr.io/pending-demand"
	pendingDemandTimeKey      = "pool-autoscaling.bohr.io/pending-demand-updated-at"
	queueBoostFactorKey       = "pool-autoscaling.bohr.io/queue-boost-factor"
	queueBoostMaxKey          = "pool-autoscaling.bohr.io/queue-boost-max"
	maxScaleUpStepKey         = "pool-autoscaling.bohr.io/max-scale-up-step"
	idleToMinSecondsKey       = "pool-autoscaling.bohr.io/idle-to-min-seconds"
	scaleDownStabilizationKey = "pool-autoscaling.bohr.io/scale-down-stabilization-seconds"
	scaleDownFractionKey      = "pool-autoscaling.bohr.io/scale-down-fraction"
	legacyIdleToZeroKey       = "pool-autoscaling.bohr.io/idle-to-zero-seconds"
	// Advanced escape hatches for supply-fault triage; leave untouched
	// during normal operation.
	capacityUtilizationKey        = "pool-autoscaling.bohr.io/capacity-utilization"
	timeoutMultiplierKey          = "pool-autoscaling.bohr.io/supply-timeout-multiplier"
	overflowBudgetKey             = "pool-autoscaling.bohr.io/overflow-budget"
	lastDecisionKey               = "pool-autoscaling.bohr.io/last-decision"
	controllerStateKey            = "pool-autoscaling.bohr.io/state"
	learnedRefillKey              = "pool-autoscaling.bohr.io/learned-refill-seconds"
	refillSourceKey               = "pool-autoscaling.bohr.io/refill-source"
	refillSamplesKey              = "pool-autoscaling.bohr.io/refill-samples"
	demandRateKey                 = "pool-autoscaling.bohr.io/demand-rate"
	demandRateFastKey             = "pool-autoscaling.bohr.io/demand-rate-fast"
	demandRateMediumKey           = "pool-autoscaling.bohr.io/demand-rate-medium"
	demandRateSlowKey             = "pool-autoscaling.bohr.io/demand-rate-slow"
	pendingObservedKey            = "pool-autoscaling.bohr.io/pending-demand-observed"
	queueBoostKey                 = "pool-autoscaling.bohr.io/queue-boost"
	supplyCapacityKey             = "pool-autoscaling.bohr.io/supply-capacity-rate"
	intentKey                     = "pool-autoscaling.bohr.io/target-intent"
	appliedKey                    = "pool-autoscaling.bohr.io/target-applied"
	overflowRatioKey              = "pool-autoscaling.bohr.io/overflow-ratio"
	coldServedKey                 = "pool-autoscaling.bohr.io/cold-served-replicas"
	zEffectiveKey                 = "pool-autoscaling.bohr.io/z-effective"
	drainBoostKey                 = "pool-autoscaling.bohr.io/drain-boost"
	legacyEnabledKey              = "dynamic-pool"
	legacyMinReplicasKey          = "min-scale"
	legacyMaxReplicasKey          = "max-scale"
	defaultMinReplicas            = 1
	defaultMaxReplicas            = 100
	defaultSLASeconds             = 10.0
	defaultRefillSeconds          = 40.0
	defaultServiceQuantile        = 0.99
	defaultRateWindow             = 60.0
	defaultFastRateWindow         = 5.0
	defaultMediumWindow           = 15.0
	defaultPendingMaxAge          = 30.0
	defaultQueueBoost             = 0.5
	defaultQueueBoostMax          = 10
	defaultMaxScaleUpStep         = 10
	defaultIdleToMin              = 120.0
	defaultScaleDownStabilization = 180.0
	defaultScaleDownFraction      = 0.05
	defaultHistoryWindow          = 3600.0
	defaultSafetyGuard            = 2
	defaultRefillQuantile         = 0.95
	defaultMinLeadSamples         = 5
	defaultLeadSampleLimit        = 128
	defaultCapacityUsage          = 0.8
	defaultTimeoutMultiple        = 3.0
	defaultOverflowBudget         = 0.05
	defaultOverflowWindow         = 900.0
	defaultMinOverflowSamples     = 20
	defaultDrainBoostMax          = 3.0
	minEffectiveZ                 = 0.84
	maxEffectiveZ                 = 3.09
	drainFractionCap              = 0.30
	sandboxPoolLabel              = "agents.kruise.io/sandbox-pool"
	sandboxClaimedLabel           = "agents.kruise.io/sandbox-claimed"
	sandboxClaimNameLabel         = "agents.kruise.io/claim-name"
	sandboxClaimTimeKey           = "agents.kruise.io/claim-timestamp"
	// sandbox-agent writes agents.kruise.io/sandbox-priority=100 exclusively on
	// the cold-create path (newSandboxFromSandboxSet); pool-refill members never
	// carry it, so it is the upstream marker distinguishing cold-started
	// sandboxes from warm-pool claims.
	sandboxPriorityKey = "agents.kruise.io/sandbox-priority"
	coldPriorityValue  = "100"
)

type demandEvent struct {
	at       time.Time
	replicas int
	uid      string
}

type poolConfig struct {
	minReplicas            int
	maxReplicas            int
	slaSeconds             float64
	refillSeconds          float64
	serviceQuantile        float64
	rateWindowSeconds      float64
	fastRateWindow         float64
	mediumRateWindow       float64
	pendingMaxAge          float64
	queueBoostFactor       float64
	queueBoostMax          int
	maxScaleUpStep         int
	idleToMinSeconds       float64
	scaleDownStabilization float64
	scaleDownFraction      float64
	historyRateWindow      float64
	safetyGuard            int
	refillQuantile         float64
	minLeadSamples         int
	leadSampleLimit        int
	capacityUsage          float64
	timeoutMultiplier      float64
	overflowBudget         float64
	overflowWindow         float64
	minOverflowSamples     int
	drainBoostMax          float64
}

type controller struct {
	client  dynamic.Interface
	state   *stateStore
	metrics *metricsRegistry
}

func main() {
	config, err := kubernetesConfig()
	if err != nil {
		log.Fatalf("build Kubernetes config: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("create dynamic Kubernetes client: %v", err)
	}
	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("create typed Kubernetes client: %v", err)
	}

	interval := durationFromEnv("RECONCILE_INTERVAL", 5*time.Second)
	namespace := firstNonEmpty(os.Getenv("POD_NAMESPACE"), "sandbox-system")
	identity := firstNonEmpty(os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"))
	if identity == "" {
		identity, err = os.Hostname()
		if err != nil {
			log.Fatalf("determine leader identity: %v", err)
		}
	}
	metrics := newMetricsRegistry()
	ctrl := &controller{
		client:  dynamicClient,
		state:   newStateStore(kube, namespace),
		metrics: metrics,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startHTTPServer(ctx, firstNonEmpty(os.Getenv("HTTP_ADDRESS"), ":8080"), metrics)
	log.Printf("dynamic pool controller started; reconcile interval=%s identity=%s", interval, identity)
	runLeaderElection(ctx, kube, namespace, identity, ctrl, interval, metrics)
}

func kubernetesConfig() (*rest.Config, error) {
	if config, err := rest.InClusterConfig(); err == nil {
		return config, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		log.Printf("ignore invalid %s=%q", key, value)
		return fallback
	}
	return parsed
}

func (c *controller) run(ctx context.Context, interval time.Duration) {
	if err := c.state.load(ctx); err != nil {
		log.Printf("load persisted state failed: %v", err)
	}
	reconcile := func() {
		err := c.reconcile(ctx)
		c.metrics.recordReconcile(err)
		if err != nil {
			log.Printf("reconcile failed: %v", err)
		}
	}
	reconcile()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("leader reconcile loop stopped")
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

func (c *controller) reconcile(ctx context.Context) error {
	sets, err := c.client.Resource(sandboxSetGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list SandboxSets: %w", err)
	}
	claims, err := c.client.Resource(sandboxClaimGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list SandboxClaims: %w", err)
	}
	sandboxes, err := c.client.Resource(sandboxGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list Sandboxes: %w", err)
	}

	events := make(map[string][]demandEvent)
	poolPendingByPool := make(map[string]int)
	coldPendingByPool := make(map[string]int)
	for index := range claims.Items {
		claim := &claims.Items[index]
		templateName, found, _ := unstructured.NestedString(claim.Object, "spec", "templateName")
		if !found || templateName == "" {
			continue
		}
		replicas64, found, _ := unstructured.NestedInt64(claim.Object, "spec", "replicas")
		replicas := 1
		if found && replicas64 > 0 {
			replicas = int(replicas64)
		}
		key := claim.GetNamespace() + "/" + templateName
		events[key] = append(events[key], demandEvent{
			at:       claim.GetCreationTimestamp().Time,
			replicas: replicas,
			uid:      string(claim.GetUID()),
		})
		pending := claimPendingReplicas(claim, replicas)
		if claimCreatesOnNoStock(claim) {
			coldPendingByPool[key] += pending
		} else {
			poolPendingByPool[key] += pending
		}
	}
	supply := make(map[string][]supplyObservation)
	overflow := make(map[string][]overflowObservation)
	coldInflightByPool := make(map[string]int)
	for index := range sandboxes.Items {
		sandbox := &sandboxes.Items[index]
		labels := sandbox.GetLabels()
		poolName := labels[sandboxPoolLabel]
		if poolName == "" {
			continue
		}
		key := sandbox.GetNamespace() + "/" + poolName
		if event, found := e2bDemandFromSandbox(sandbox); found {
			events[key] = append(events[key], event)
		}
		if labels[sandboxClaimedLabel] == "true" {
			if observation, found := overflowObservationFromSandbox(sandbox); found {
				overflow[key] = append(overflow[key], observation)
			}
			continue
		}
		if sandbox.GetAnnotations()[sandboxPriorityKey] == coldPriorityValue {
			coldInflightByPool[key]++
		}
		ready, readyAt := sandboxReady(sandbox)
		supply[key] = append(supply[key], supplyObservation{
			uid:       string(sandbox.GetUID()),
			createdAt: sandbox.GetCreationTimestamp().Time,
			readyAt:   readyAt,
			ready:     ready,
		})
	}

	now := time.Now()
	var failures []string
	activePools := make(map[string]struct{})
	for index := range sets.Items {
		set := &sets.Items[index]
		if !dynamicPoolEnabled(set.GetAnnotations()) {
			continue
		}
		poolID := persistentPoolID(set)
		activePools[poolID] = struct{}{}
		key := set.GetNamespace() + "/" + set.GetName()
		if err := c.reconcilePool(
			ctx, set, events[key], supply[key],
			poolPendingByPool[key], coldPendingByPool[key],
			overflow[key], coldInflightByPool[key], now,
		); err != nil {
			failures = append(failures, set.GetNamespace()+"/"+set.GetName()+": "+err.Error())
		}
	}
	c.state.prune(activePools)
	c.metrics.prunePools(activePools)
	if err := c.state.save(ctx); err != nil {
		failures = append(failures, err.Error())
	}
	if len(failures) > 0 {
		return fmt.Errorf("%s", strings.Join(failures, "; "))
	}
	return nil
}

func (c *controller) reconcilePool(
	ctx context.Context,
	set *unstructured.Unstructured,
	events []demandEvent,
	supply []supplyObservation,
	poolClaimPending int,
	coldClaimPending int,
	overflow []overflowObservation,
	coldInflight int,
	now time.Time,
) error {
	annotations := set.GetAnnotations()
	config := configFromAnnotations(annotations)
	poolID := persistentPoolID(set)
	coldPressure := max(coldClaimPending, coldInflight)
	c.state.observeReady(poolID, supply, config.leadSampleLimit, coldPressure)
	c.state.observeDemand(poolID, events, config.historyRateWindow, now)
	c.state.observeOverflow(poolID, overflow, config.overflowWindow, now)
	overflowRatio, coldServed, warmServed := c.state.overflowRatio(
		poolID, now, config.overflowWindow,
	)
	overflowSamples := coldServed + warmServed
	overflowEnough := overflowSamples >= config.minOverflowSamples
	refillSeconds, sampleCount, learned := c.state.leadTime(
		poolID,
		config.refillQuantile,
		config.minLeadSamples,
		config.refillSeconds,
	)
	current64, found, err := unstructured.NestedInt64(set.Object, "spec", "replicas")
	if err != nil || !found {
		return fmt.Errorf("read spec.replicas: %w", err)
	}
	current := int(current64)
	available64, _, _ := unstructured.NestedInt64(set.Object, "status", "availableReplicas")
	actual64, _, _ := unstructured.NestedInt64(set.Object, "status", "replicas")
	available := max(0, int(available64))
	actual := max(0, int(actual64))
	creating := max(0, actual-available)
	maxUnavailable := sandboxSetMaxUnavailable(set, config.maxReplicas)
	effectiveConcurrency := maxUnavailable
	if effectiveConcurrency == 0 {
		effectiveConcurrency = max(1, config.maxReplicas)
	}
	supplyCapacityRate := float64(effectiveConcurrency) * config.capacityUsage / refillSeconds
	oldestCreatingAge := 0.0
	for _, observation := range supply {
		if !observation.ready {
			oldestCreatingAge = max(oldestCreatingAge, now.Sub(observation.createdAt).Seconds())
		}
	}

	latestDemand := set.GetCreationTimestamp().Time
	for _, event := range events {
		if event.at.After(latestDemand) {
			latestDemand = event.at
		}
	}
	externalPending, pendingUpdatedAt := pendingDemandFromAnnotations(
		annotations, now, config.pendingMaxAge,
	)
	if pendingUpdatedAt.After(latestDemand) {
		latestDemand = pendingUpdatedAt
	}
	pending := min(
		config.maxReplicas,
		min(max(0, poolClaimPending), config.maxReplicas)+
			min(max(0, coldClaimPending), config.maxReplicas)+
			min(externalPending, config.maxReplicas),
	)
	boostablePending := min(
		config.maxReplicas,
		min(max(0, poolClaimPending), config.maxReplicas)+
			min(externalPending, config.maxReplicas),
	)
	queueBoost := calculateQueueBoost(
		boostablePending, config.queueBoostFactor, config.queueBoostMax,
	)
	fastRate := demandRate(events, now, config.fastRateWindow)
	mediumRate := demandRate(events, now, config.mediumRateWindow)
	slowRate := demandRate(events, now, config.rateWindowSeconds)
	historyRate := c.state.recentAverageRate(poolID, now, config.historyRateWindow)
	if historyRate <= 0 {
		historyRate = demandAverageRate(events, now, config.historyRateWindow)
	}
	rate := max(fastRate, mediumRate, slowRate)
	trafficFloor := calculateTrafficFloor(historyRate, refillSeconds, config.minReplicas)
	recentReplicas := demandCount(events, now, config.rateWindowSeconds)
	zBase := quantileZ(config.serviceQuantile)
	zAdjust := 0.0
	if config.overflowBudget > 0 {
		zBase = quantileZ(1 - config.overflowBudget)
		if overflowEnough {
			delta := 0.0
			switch {
			case overflowRatio > 2*config.overflowBudget:
				delta = 0.15
			case overflowRatio == 0:
				delta = -0.05
			case overflowRatio < config.overflowBudget/2:
				delta = -0.10
			}
			zAdjust = c.state.stepZAdjustment(
				poolID, now, config.scaleDownStabilization, delta,
			)
		}
	}
	zEffective := min(max(zBase+zAdjust, minEffectiveZ), maxEffectiveZ)
	drainBoostActive := config.overflowBudget > 0 && config.drainBoostMax > 1.0 &&
		overflowEnough && overflowRatio == 0 && pending == 0
	rawTarget := calculateTarget(
		rate,
		refillSeconds,
		config.slaSeconds,
		zEffective,
		config.safetyGuard,
	) + queueBoost
	intent := clamp(rawTarget, trafficFloor, config.maxReplicas)
	c.state.rememberIntent(poolID, now, intent, config.scaleDownStabilization)
	state := "NORMAL"

	idleFor := now.Sub(latestDemand).Seconds()
	hasDemand := recentReplicas > 0 || pending > 0
	idleExpired := pending == 0 && idleFor >= config.idleToMinSeconds
	if idleExpired {
		intent = trafficFloor
		c.state.resetIntentHistory(poolID, now, trafficFloor)
		state = "IDLE"
	} else if !hasDemand {
		if current < trafficFloor {
			intent = trafficFloor
		} else {
			intent = current
			state = "RECOVERING"
		}
	} else {
		if pending > 0 || intent > current || fastRate > slowRate {
			state = "SURGE"
		}
		if intent < current {
			intent = max(
				trafficFloor,
				applyGradualScaleDown(
					current,
					intent,
					c.state.stabilizedIntentFloor(poolID, intent),
					pending,
					effectiveDrainFraction(config.scaleDownFraction, config.drainBoostMax, drainBoostActive),
				),
			)
		}
	}

	capacityExceeded := rate > supplyCapacityRate && rate > 0
	supplyTimedOut := creating > 0 &&
		oldestCreatingAge > refillSeconds*config.timeoutMultiplier
	applied := applySupplyGate(
		intent, current, actual, creating, maxUnavailable, config.capacityUsage,
		config.maxScaleUpStep, supplyTimedOut,
	)
	if capacityExceeded || supplyTimedOut {
		state = "DEGRADED"
	}

	decision := fmt.Sprintf(
		"state=%s rate=%.3f/s fast=%.3f medium=%.3f slow=%.3f hour=%.3f floor=%d pending=%d queue-boost=%d lead=%.1fs intent=%d applied=%d creating=%d/%d overflow=%.3f cold=%d z=%.2f drain-boost=%t",
		state, rate, fastRate, mediumRate, slowRate, historyRate, trafficFloor, pending, queueBoost,
		refillSeconds, intent, applied, creating, maxUnavailable,
		overflowRatio, coldServed, zEffective, drainBoostActive,
	)
	annotationUpdates := map[string]string{
		lastDecisionKey:     decision,
		controllerStateKey:  state,
		learnedRefillKey:    fmt.Sprintf("%.3f", refillSeconds),
		refillSourceKey:     map[bool]string{true: "learned", false: "fallback"}[learned],
		refillSamplesKey:    strconv.Itoa(sampleCount),
		demandRateKey:       fmt.Sprintf("%.6f", rate),
		demandRateFastKey:   fmt.Sprintf("%.6f", fastRate),
		demandRateMediumKey: fmt.Sprintf("%.6f", mediumRate),
		demandRateSlowKey:   fmt.Sprintf("%.6f", slowRate),
		pendingObservedKey:  strconv.Itoa(pending),
		queueBoostKey:       strconv.Itoa(queueBoost),
		supplyCapacityKey:   fmt.Sprintf("%.6f", supplyCapacityRate),
		intentKey:           strconv.Itoa(intent),
		appliedKey:          strconv.Itoa(applied),
		overflowRatioKey:    fmt.Sprintf("%.4f", overflowRatio),
		coldServedKey:       strconv.Itoa(coldServed),
		zEffectiveKey:       fmt.Sprintf("%.3f", zEffective),
		drainBoostKey:       map[bool]string{true: "true", false: "false"}[drainBoostActive],
	}
	c.metrics.setPool(poolID, poolMetrics{
		namespace:          set.GetNamespace(),
		name:               set.GetName(),
		state:              state,
		demandRate:         rate,
		fastRate:           fastRate,
		mediumRate:         mediumRate,
		slowRate:           slowRate,
		pendingDemand:      pending,
		queueBoost:         queueBoost,
		refillP95Seconds:   refillSeconds,
		refillLearned:      learned,
		refillSamples:      sampleCount,
		supplyCapacityRate: supplyCapacityRate,
		intent:             intent,
		applied:            applied,
		available:          available,
		creating:           creating,
		overflowRatio:      overflowRatio,
		coldServed:         coldServed,
		zEffective:         zEffective,
		drainBoost:         drainBoostActive,
	})

	patch := map[string]any{}
	metadataChanged := false
	for key, value := range annotationUpdates {
		if annotations[key] != value {
			metadataChanged = true
			break
		}
	}
	if metadataChanged {
		patch["metadata"] = map[string]any{"annotations": annotationUpdates}
	}
	if applied != current {
		patch["spec"] = map[string]any{"replicas": applied}
	}
	if len(patch) == 0 {
		return nil
	}
	payload, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("encode patch: %w", err)
	}
	_, err = c.client.Resource(sandboxSetGVR).Namespace(set.GetNamespace()).Patch(
		ctx,
		set.GetName(),
		types.MergePatchType,
		payload,
		metav1.PatchOptions{FieldManager: "sandbox-pool-controller"},
	)
	if err != nil {
		return fmt.Errorf("patch replicas %d -> %d: %w", current, applied, err)
	}
	log.Printf(
		"pool=%s/%s replicas=%d->%d %s learned=%t samples=%d oldestCreating=%.1fs",
		set.GetNamespace(), set.GetName(), current, applied, decision,
		learned, sampleCount, oldestCreatingAge,
	)
	return nil
}

func dynamicPoolEnabled(annotations map[string]string) bool {
	return parseBool(firstNonEmpty(annotations[enabledKey], annotations[legacyEnabledKey]), false)
}

func claimPendingReplicas(claim *unstructured.Unstructured, requested int) int {
	phase, _, _ := unstructured.NestedString(claim.Object, "status", "phase")
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "completed", "failed":
		return 0
	}
	completionTime, _, _ := unstructured.NestedString(claim.Object, "status", "completionTime")
	if strings.TrimSpace(completionTime) != "" {
		return 0
	}
	claimed64, _, _ := unstructured.NestedInt64(claim.Object, "status", "claimedReplicas")
	return max(0, requested-int(claimed64))
}

func claimCreatesOnNoStock(claim *unstructured.Unstructured) bool {
	createOnNoStock, found, _ := unstructured.NestedBool(
		claim.Object, "spec", "createOnNoStock",
	)
	if !found {
		return true
	}
	return createOnNoStock
}

func pendingDemandFromAnnotations(
	annotations map[string]string,
	now time.Time,
	maxAgeSeconds float64,
) (int, time.Time) {
	rawPending := strings.TrimSpace(annotations[pendingDemandKey])
	if rawPending == "" {
		return 0, time.Time{}
	}
	pending, err := strconv.Atoi(rawPending)
	if err != nil || pending < 0 {
		return 0, time.Time{}
	}
	rawTime := strings.TrimSpace(annotations[pendingDemandTimeKey])
	if rawTime == "" {
		return 0, time.Time{}
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, rawTime)
	if err != nil {
		return 0, time.Time{}
	}
	age := now.Sub(updatedAt).Seconds()
	if age < -5 || age > maxAgeSeconds {
		return 0, time.Time{}
	}
	return pending, updatedAt
}

func demandCount(events []demandEvent, now time.Time, windowSeconds float64) int {
	if windowSeconds <= 0 {
		return 0
	}
	windowStart := now.Add(-time.Duration(windowSeconds * float64(time.Second)))
	total := 0
	for _, event := range events {
		if event.at.After(now) || event.at.Before(windowStart) {
			continue
		}
		total += max(0, event.replicas)
	}
	return total
}

func demandAverageRate(events []demandEvent, now time.Time, windowSeconds float64) float64 {
	if windowSeconds <= 0 {
		return 0
	}
	return float64(demandCount(events, now, windowSeconds)) / windowSeconds
}

func demandRate(events []demandEvent, now time.Time, windowSeconds float64) float64 {
	if windowSeconds <= 0 {
		return 0
	}
	windowStart := now.Add(-time.Duration(windowSeconds * float64(time.Second)))
	window := 0.0
	count := 0
	found := false
	for _, event := range events {
		if event.at.After(now) || event.at.Before(windowStart) {
			continue
		}
		effectiveReplicas := max(0, event.replicas)
		count += effectiveReplicas
		window = max(window, now.Sub(event.at).Seconds())
		found = found || effectiveReplicas > 0
	}
	if !found || count == 0 {
		return 0
	}
	elapsed := max(1.0, window)
	return float64(count) / elapsed
}

// configFromAnnotations reads only the operator-facing knobs. Window sizes,
// learning quantiles, guard inventory, and other internal mechanics are
// frozen constants: they were tuned once and had no real-world overrides.
func configFromAnnotations(annotations map[string]string) poolConfig {
	minReplicas := max(0, parseInt(
		firstNonEmpty(annotations[minReplicasKey], annotations[legacyMinReplicasKey]),
		defaultMinReplicas,
	))
	maxReplicas := parseInt(
		firstNonEmpty(annotations[maxReplicasKey], annotations[legacyMaxReplicasKey]),
		defaultMaxReplicas,
	)
	if maxReplicas < minReplicas {
		maxReplicas = minReplicas
	}
	return poolConfig{
		minReplicas:       minReplicas,
		maxReplicas:       max(0, maxReplicas),
		slaSeconds:        positiveFloat(annotations[slaSecondsKey], defaultSLASeconds),
		refillSeconds:     positiveFloat(annotations[refillSecondsKey], defaultRefillSeconds),
		serviceQuantile:   defaultServiceQuantile,
		rateWindowSeconds: defaultRateWindow,
		fastRateWindow:    defaultFastRateWindow,
		mediumRateWindow:  defaultMediumWindow,
		pendingMaxAge:     defaultPendingMaxAge,
		queueBoostFactor:  boundedFloat(annotations[queueBoostFactorKey], defaultQueueBoost, 0.0, 1.0),
		queueBoostMax:     min(max(0, parseInt(annotations[queueBoostMaxKey], defaultQueueBoostMax)), max(0, maxReplicas)),
		maxScaleUpStep:    max(0, parseInt(annotations[maxScaleUpStepKey], defaultMaxScaleUpStep)),
		idleToMinSeconds: positiveFloat(
			firstNonEmpty(annotations[idleToMinSecondsKey], annotations[legacyIdleToZeroKey]),
			defaultIdleToMin,
		),
		scaleDownStabilization: positiveFloat(annotations[scaleDownStabilizationKey], defaultScaleDownStabilization),
		scaleDownFraction:      boundedFloat(annotations[scaleDownFractionKey], defaultScaleDownFraction, 0.01, 0.5),
		historyRateWindow:      defaultHistoryWindow,
		safetyGuard:            defaultSafetyGuard,
		refillQuantile:         defaultRefillQuantile,
		minLeadSamples:         defaultMinLeadSamples,
		leadSampleLimit:        defaultLeadSampleLimit,
		capacityUsage:          boundedFloat(annotations[capacityUtilizationKey], defaultCapacityUsage, 0.05, 1.0),
		timeoutMultiplier:      positiveFloat(annotations[timeoutMultiplierKey], defaultTimeoutMultiple),
		overflowBudget:         boundedFloat(annotations[overflowBudgetKey], defaultOverflowBudget, 0.0, 0.20),
		overflowWindow:         defaultOverflowWindow,
		minOverflowSamples:     defaultMinOverflowSamples,
		drainBoostMax:          defaultDrainBoostMax,
	}
}

// e2bDemandFromSandbox observes successful sandbox-manager claims through
// official Sandbox metadata. SandboxClaim-owned sandboxes are excluded because
// their demand is already counted from the SandboxClaim object itself.
func e2bDemandFromSandbox(sandbox *unstructured.Unstructured) (demandEvent, bool) {
	labels := sandbox.GetLabels()
	if labels[sandboxClaimedLabel] != "true" || labels[sandboxClaimNameLabel] != "" {
		return demandEvent{}, false
	}
	rawTime := strings.TrimSpace(sandbox.GetAnnotations()[sandboxClaimTimeKey])
	if rawTime == "" {
		return demandEvent{}, false
	}
	claimedAt, err := time.Parse(time.RFC3339Nano, rawTime)
	if err != nil {
		return demandEvent{}, false
	}
	return demandEvent{at: claimedAt, replicas: 1, uid: string(sandbox.GetUID())}, true
}

// overflowObservationFromSandbox classifies a claimed sandbox as cold-started
// or warm-claimed using the upstream priority marker written exclusively by
// the sandbox-manager cold-create path.
func overflowObservationFromSandbox(sandbox *unstructured.Unstructured) (overflowObservation, bool) {
	labels := sandbox.GetLabels()
	if labels[sandboxClaimedLabel] != "true" {
		return overflowObservation{}, false
	}
	return overflowObservation{
		uid:  string(sandbox.GetUID()),
		at:   sandboxClaimedAt(sandbox),
		cold: sandbox.GetAnnotations()[sandboxPriorityKey] == coldPriorityValue,
	}, true
}

func sandboxClaimedAt(sandbox *unstructured.Unstructured) time.Time {
	rawTime := strings.TrimSpace(sandbox.GetAnnotations()[sandboxClaimTimeKey])
	if rawTime != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, rawTime); err == nil {
			return parsed
		}
	}
	return sandbox.GetCreationTimestamp().Time
}

func persistentPoolID(set *unstructured.Unstructured) string {
	return fmt.Sprintf("%s/%s@%s", set.GetNamespace(), set.GetName(), set.GetUID())
}

func sandboxReady(sandbox *unstructured.Unstructured) (bool, time.Time) {
	conditions, found, _ := unstructured.NestedSlice(sandbox.Object, "status", "conditions")
	if !found {
		return false, time.Time{}
	}
	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]any)
		if !ok || condition["type"] != "Ready" || condition["status"] != "True" {
			continue
		}
		rawTransition, ok := condition["lastTransitionTime"].(string)
		if !ok {
			return false, time.Time{}
		}
		transition, err := time.Parse(time.RFC3339, rawTransition)
		if err != nil {
			return false, time.Time{}
		}
		return true, transition
	}
	return false, time.Time{}
}

func sandboxSetMaxUnavailable(set *unstructured.Unstructured, maxReplicas int) int {
	raw, found, _ := unstructured.NestedFieldNoCopy(set.Object, "spec", "scaleStrategy", "maxUnavailable")
	if !found || raw == nil {
		return 0
	}
	switch value := raw.(type) {
	case int64:
		return max(0, int(value))
	case int32:
		return max(0, int(value))
	case float64:
		return max(0, int(value))
	case string:
		value = strings.TrimSpace(value)
		if strings.HasSuffix(value, "%") {
			percent, err := strconv.ParseFloat(strings.TrimSuffix(value, "%"), 64)
			if err != nil || percent < 0 {
				return 0
			}
			return max(1, int(math.Ceil(percent/100*float64(maxReplicas))))
		}
		return max(0, parseInt(value, 0))
	default:
		return 0
	}
}

func applyGradualScaleDown(current, intent, stabilized, pending int, fraction float64) int {
	if intent >= current {
		return intent
	}
	if pending > 0 || stabilized >= current {
		return current
	}
	if fraction <= 0 {
		fraction = defaultScaleDownFraction
	}
	reduction := max(1, int(math.Ceil(float64(current)*fraction)))
	return max(intent, current-reduction)
}

// effectiveDrainFraction accelerates post-burst drain when recent demand was
// served entirely from the warm pool with zero cold overflow: inventory built
// for an already-digested peak is idle cost, and the hourly traffic floor
// still protects recurring tidal baselines.
func effectiveDrainFraction(base, boostMax float64, active bool) float64 {
	if !active || boostMax <= 1.0 {
		return base
	}
	return min(drainFractionCap, base*boostMax)
}

func applySupplyGate(
	intent int,
	current int,
	actual int,
	creating int,
	maxUnavailable int,
	capacityUsage float64,
	maxScaleUpStep int,
	supplyTimedOut bool,
) int {
	if intent <= current {
		return intent
	}
	if supplyTimedOut {
		return current
	}
	stepLimitedIntent := intent
	if maxScaleUpStep > 0 {
		stepLimitedIntent = min(intent, current+maxScaleUpStep)
	}
	if maxUnavailable <= 0 {
		return stepLimitedIntent
	}
	creationBudget := max(1, int(math.Floor(float64(maxUnavailable)*capacityUsage)))
	creationSlots := max(0, creationBudget-creating)
	return min(stepLimitedIntent, max(current, actual+creationSlots))
}

func calculateTrafficFloor(rate, leadSeconds float64, minReplicas int) int {
	raw := int(math.Round(rate * max(1.0, leadSeconds)))
	if raw < 0 {
		raw = 0
	}
	return max(minReplicas, raw)
}

func calculateQueueBoost(pending int, factor float64, maximum int) int {
	pending = max(0, pending)
	factor = min(max(factor, 0.0), 1.0)
	boost := int(math.Ceil(float64(pending) * factor))
	if maximum > 0 {
		boost = min(boost, maximum)
	}
	return boost
}

func calculateTarget(
	rate float64,
	refillSeconds float64,
	slaSeconds float64,
	zScore float64,
	safetyGuard int,
) int {
	rate = max(0, rate)
	refillSeconds = max(0, refillSeconds)
	slaSeconds = max(0, slaSeconds)
	horizon := max(0, refillSeconds-slaSeconds)
	baseInventory := rate * horizon
	zScore = min(max(zScore, 0.0), 5.0)
	safetyStock := zScore * math.Sqrt(max(0, rate*refillSeconds))
	return int(math.Ceil(baseInventory+safetyStock)) + max(0, safetyGuard)
}

// quantileZ maps a coverage quantile (or a cold-overflow budget) to the
// standard-normal z score used by the safety-stock term.
func quantileZ(quantile float64) float64 {
	quantile = min(max(quantile, 0.5000001), 0.9999999)
	return math.Sqrt2 * math.Erfcinv(2*(1-quantile))
}

func clamp(value, minimum, maximum int) int {
	return min(max(value, minimum), maximum)
}

func parseBool(value string, fallback bool) bool {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}

func positiveFloat(value string, fallback float64) float64 {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func boundedFloat(value string, fallback, minimum, maximum float64) float64 {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return fallback
	}
	return min(max(parsed, minimum), maximum)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
