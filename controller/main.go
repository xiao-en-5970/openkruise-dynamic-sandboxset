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
	enabledKey             = "pool-autoscaling.bohr.io/enabled"
	minReplicasKey         = "pool-autoscaling.bohr.io/min-replicas"
	maxReplicasKey         = "pool-autoscaling.bohr.io/max-replicas"
	slaSecondsKey          = "pool-autoscaling.bohr.io/sla-seconds"
	refillSecondsKey       = "pool-autoscaling.bohr.io/refill-seconds"
	serviceQuantileKey     = "pool-autoscaling.bohr.io/service-quantile"
	rateWindowSecondsKey   = "pool-autoscaling.bohr.io/rate-window-seconds"
	idleToZeroSecondsKey   = "pool-autoscaling.bohr.io/idle-to-zero-seconds"
	safetyGuardKey         = "pool-autoscaling.bohr.io/safety-guard"
	refillQuantileKey      = "pool-autoscaling.bohr.io/refill-quantile"
	minLeadSamplesKey      = "pool-autoscaling.bohr.io/min-lead-samples"
	leadSampleLimitKey     = "pool-autoscaling.bohr.io/lead-sample-limit"
	capacityUtilizationKey = "pool-autoscaling.bohr.io/capacity-utilization"
	timeoutMultiplierKey   = "pool-autoscaling.bohr.io/supply-timeout-multiplier"
	lastDecisionKey        = "pool-autoscaling.bohr.io/last-decision"
	controllerStateKey     = "pool-autoscaling.bohr.io/state"
	learnedRefillKey       = "pool-autoscaling.bohr.io/learned-refill-seconds"
	refillSourceKey        = "pool-autoscaling.bohr.io/refill-source"
	refillSamplesKey       = "pool-autoscaling.bohr.io/refill-samples"
	demandRateKey          = "pool-autoscaling.bohr.io/demand-rate"
	demandTotalKey         = "pool-autoscaling.bohr.io/demand-total"
	lastDemandTimeKey      = "pool-autoscaling.bohr.io/last-demand-time"
	supplyCapacityKey      = "pool-autoscaling.bohr.io/supply-capacity-rate"
	intentKey              = "pool-autoscaling.bohr.io/target-intent"
	appliedKey             = "pool-autoscaling.bohr.io/target-applied"
	legacyEnabledKey       = "dynamic-pool"
	legacyMinReplicasKey   = "min-scale"
	legacyMaxReplicasKey   = "max-scale"
	defaultMinReplicas     = 0
	defaultMaxReplicas     = 100
	defaultSLASeconds      = 10.0
	defaultRefillSeconds   = 40.0
	defaultServiceQuantile = 0.99
	defaultRateWindow      = 60.0
	defaultIdleToZero      = 600.0
	defaultSafetyGuard     = 2
	defaultRefillQuantile  = 0.95
	defaultMinLeadSamples  = 5
	defaultLeadSampleLimit = 128
	defaultCapacityUsage   = 0.8
	defaultTimeoutMultiple = 3.0
)

type demandEvent struct {
	at       time.Time
	replicas int
}

type poolConfig struct {
	minReplicas       int
	maxReplicas       int
	slaSeconds        float64
	refillSeconds     float64
	serviceQuantile   float64
	rateWindowSeconds float64
	idleToZeroSeconds float64
	safetyGuard       int
	refillQuantile    float64
	minLeadSamples    int
	leadSampleLimit   int
	capacityUsage     float64
	timeoutMultiplier float64
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
			at: claim.GetCreationTimestamp().Time, replicas: replicas,
		})
	}
	supply := make(map[string][]supplyObservation)
	for index := range sandboxes.Items {
		sandbox := &sandboxes.Items[index]
		poolName := sandbox.GetLabels()["agents.kruise.io/sandbox-pool"]
		if poolName == "" {
			continue
		}
		ready, readyAt := sandboxReady(sandbox)
		key := sandbox.GetNamespace() + "/" + poolName
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
		if err := c.reconcilePool(ctx, set, events[key], supply[key], now); err != nil {
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
	now time.Time,
) error {
	annotations := set.GetAnnotations()
	config := configFromAnnotations(annotations)
	poolID := persistentPoolID(set)
	c.state.observeReady(poolID, supply, config.leadSampleLimit)
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
	windowStart := now.Add(-time.Duration(config.rateWindowSeconds * float64(time.Second)))
	demandTotal, directDemandAt, err := directDemandFromAnnotations(annotations)
	if err != nil {
		return err
	}
	recentReplicas, persistedDemandAt := c.state.observeDemand(
		poolID, demandTotal, directDemandAt, windowStart,
	)
	if persistedDemandAt.After(latestDemand) {
		latestDemand = persistedDemandAt
	}
	for _, event := range events {
		if event.at.After(latestDemand) {
			latestDemand = event.at
		}
		if !event.at.Before(windowStart) {
			recentReplicas += event.replicas
		}
	}

	rate := float64(recentReplicas) / config.rateWindowSeconds
	rawTarget := calculateTarget(
		rate,
		refillSeconds,
		config.slaSeconds,
		config.serviceQuantile,
		config.safetyGuard,
	)
	intent := clamp(rawTarget, config.minReplicas, config.maxReplicas)
	if recentReplicas > 0 && current == 0 && config.maxReplicas > 0 {
		intent = max(intent, 1)
	}
	state := "NORMAL"

	idleFor := now.Sub(latestDemand).Seconds()
	if recentReplicas == 0 {
		if idleFor >= config.idleToZeroSeconds {
			intent = config.minReplicas
		} else {
			intent = current
			state = "RECOVERING"
		}
	} else if intent > current {
		state = "SURGE"
	}

	capacityExceeded := rate > supplyCapacityRate && rate > 0
	supplyTimedOut := creating > 0 &&
		oldestCreatingAge > refillSeconds*config.timeoutMultiplier
	applied := applySupplyGate(
		intent, current, actual, creating, maxUnavailable, config.capacityUsage, supplyTimedOut,
	)
	if capacityExceeded || supplyTimedOut {
		state = "DEGRADED"
	}

	decision := fmt.Sprintf(
		"state=%s rate=%.3f/s lead=%.1fs intent=%d applied=%d creating=%d/%d",
		state, rate, refillSeconds, intent, applied, creating, maxUnavailable,
	)
	annotationUpdates := map[string]string{
		lastDecisionKey:    decision,
		controllerStateKey: state,
		learnedRefillKey:   fmt.Sprintf("%.3f", refillSeconds),
		refillSourceKey:    map[bool]string{true: "learned", false: "fallback"}[learned],
		refillSamplesKey:   strconv.Itoa(sampleCount),
		demandRateKey:      fmt.Sprintf("%.6f", rate),
		supplyCapacityKey:  fmt.Sprintf("%.6f", supplyCapacityRate),
		intentKey:          strconv.Itoa(intent),
		appliedKey:         strconv.Itoa(applied),
	}
	c.metrics.setPool(poolID, poolMetrics{
		namespace:          set.GetNamespace(),
		name:               set.GetName(),
		state:              state,
		demandRate:         rate,
		refillP95Seconds:   refillSeconds,
		refillLearned:      learned,
		refillSamples:      sampleCount,
		supplyCapacityRate: supplyCapacityRate,
		intent:             intent,
		applied:            applied,
		available:          available,
		creating:           creating,
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

func directDemandFromAnnotations(annotations map[string]string) (uint64, time.Time, error) {
	rawTotal := strings.TrimSpace(annotations[demandTotalKey])
	if rawTotal == "" {
		return 0, time.Time{}, nil
	}
	total, err := strconv.ParseUint(rawTotal, 10, 64)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("parse %s: %w", demandTotalKey, err)
	}
	if total == 0 {
		return 0, time.Time{}, nil
	}
	rawTime := strings.TrimSpace(annotations[lastDemandTimeKey])
	at, err := time.Parse(time.RFC3339Nano, rawTime)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("parse %s: %w", lastDemandTimeKey, err)
	}
	return total, at, nil
}

func configFromAnnotations(annotations map[string]string) poolConfig {
	minReplicas := parseInt(
		firstNonEmpty(annotations[minReplicasKey], annotations[legacyMinReplicasKey]),
		defaultMinReplicas,
	)
	maxReplicas := parseInt(
		firstNonEmpty(annotations[maxReplicasKey], annotations[legacyMaxReplicasKey]),
		defaultMaxReplicas,
	)
	if maxReplicas < minReplicas {
		maxReplicas = minReplicas
	}
	return poolConfig{
		minReplicas:       max(0, minReplicas),
		maxReplicas:       max(0, maxReplicas),
		slaSeconds:        positiveFloat(annotations[slaSecondsKey], defaultSLASeconds),
		refillSeconds:     positiveFloat(annotations[refillSecondsKey], defaultRefillSeconds),
		serviceQuantile:   boundedFloat(annotations[serviceQuantileKey], defaultServiceQuantile, 0.5001, 0.9999),
		rateWindowSeconds: positiveFloat(annotations[rateWindowSecondsKey], defaultRateWindow),
		idleToZeroSeconds: positiveFloat(annotations[idleToZeroSecondsKey], defaultIdleToZero),
		safetyGuard:       max(0, parseInt(annotations[safetyGuardKey], defaultSafetyGuard)),
		refillQuantile:    boundedFloat(annotations[refillQuantileKey], defaultRefillQuantile, 0.5001, 1.0),
		minLeadSamples:    max(1, parseInt(annotations[minLeadSamplesKey], defaultMinLeadSamples)),
		leadSampleLimit:   max(10, parseInt(annotations[leadSampleLimitKey], defaultLeadSampleLimit)),
		capacityUsage:     boundedFloat(annotations[capacityUtilizationKey], defaultCapacityUsage, 0.05, 1.0),
		timeoutMultiplier: positiveFloat(annotations[timeoutMultiplierKey], defaultTimeoutMultiple),
	}
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

func applySupplyGate(
	intent int,
	current int,
	actual int,
	creating int,
	maxUnavailable int,
	capacityUsage float64,
	supplyTimedOut bool,
) int {
	if intent <= current {
		return intent
	}
	if supplyTimedOut {
		return current
	}
	if maxUnavailable <= 0 {
		return intent
	}
	creationBudget := max(1, int(math.Floor(float64(maxUnavailable)*capacityUsage)))
	creationSlots := max(0, creationBudget-creating)
	return min(intent, max(current, actual+creationSlots))
}

func calculateTarget(
	rate float64,
	refillSeconds float64,
	slaSeconds float64,
	serviceQuantile float64,
	safetyGuard int,
) int {
	rate = max(0, rate)
	refillSeconds = max(0, refillSeconds)
	slaSeconds = max(0, slaSeconds)
	horizon := max(0, refillSeconds-slaSeconds)
	baseInventory := rate * horizon
	zScore := math.Sqrt2 * math.Erfcinv(2*(1-serviceQuantile))
	safetyStock := zScore * math.Sqrt(max(0, rate*refillSeconds))
	return int(math.Ceil(baseInventory+safetyStock)) + max(0, safetyGuard)
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
