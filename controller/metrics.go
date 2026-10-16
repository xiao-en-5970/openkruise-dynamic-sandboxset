package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type poolMetrics struct {
	namespace          string
	name               string
	state              string
	demandRate         float64
	fastRate           float64
	mediumRate         float64
	slowRate           float64
	pendingDemand      int
	queueBoost         int
	refillP95Seconds   float64
	refillLearned      bool
	refillSamples      int
	supplyCapacityRate float64
	intent             int
	applied            int
	available          int
	creating           int
	overflowRatio      float64
	coldServed         int
	zEffective         float64
	drainBoost         bool
}

type metricsRegistry struct {
	mu              sync.RWMutex
	leader          bool
	reconciles      uint64
	reconcileErrors uint64
	pools           map[string]poolMetrics
}

func newMetricsRegistry() *metricsRegistry {
	return &metricsRegistry{pools: make(map[string]poolMetrics)}
}

func (m *metricsRegistry) setLeader(leader bool) {
	m.mu.Lock()
	m.leader = leader
	m.mu.Unlock()
}

func (m *metricsRegistry) recordReconcile(err error) {
	m.mu.Lock()
	m.reconciles++
	if err != nil {
		m.reconcileErrors++
	}
	m.mu.Unlock()
}

func (m *metricsRegistry) setPool(poolID string, value poolMetrics) {
	m.mu.Lock()
	m.pools[poolID] = value
	m.mu.Unlock()
}

func (m *metricsRegistry) prunePools(activePools map[string]struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for poolID := range m.pools {
		if _, active := activePools[poolID]; !active {
			delete(m.pools, poolID)
		}
	}
}

func (m *metricsRegistry) ServeHTTP(writer http.ResponseWriter, _ *http.Request) {
	m.mu.RLock()
	leader := m.leader
	reconciles := m.reconciles
	errors := m.reconcileErrors
	pools := make([]poolMetrics, 0, len(m.pools))
	for _, value := range m.pools {
		pools = append(pools, value)
	}
	m.mu.RUnlock()
	sort.Slice(pools, func(i, j int) bool {
		if pools[i].namespace == pools[j].namespace {
			return pools[i].name < pools[j].name
		}
		return pools[i].namespace < pools[j].namespace
	})

	writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
	writeMetricHelp(writer, "sandbox_pool_controller_leader", "Whether this replica currently owns the leader lease.", "gauge")
	fmt.Fprintf(writer, "sandbox_pool_controller_leader %d\n", boolInt(leader))
	writeMetricHelp(writer, "sandbox_pool_controller_reconciles_total", "Total controller reconciliation attempts.", "counter")
	fmt.Fprintf(writer, "sandbox_pool_controller_reconciles_total %d\n", reconciles)
	writeMetricHelp(writer, "sandbox_pool_controller_reconcile_errors_total", "Total failed controller reconciliations.", "counter")
	fmt.Fprintf(writer, "sandbox_pool_controller_reconcile_errors_total %d\n", errors)

	descriptions := []struct {
		name        string
		description string
	}{
		{"sandbox_pool_demand_rate", "Planned demand rate, the maximum of fast, medium, and slow windows."},
		{"sandbox_pool_demand_rate_fast", "Observed demand per second in the fast window."},
		{"sandbox_pool_demand_rate_medium", "Observed demand per second in the medium window."},
		{"sandbox_pool_demand_rate_slow", "Observed demand per second in the slow window."},
		{"sandbox_pool_pending_demand", "Current unfulfilled SandboxClaim and fresh external demand."},
		{"sandbox_pool_queue_boost", "Pending demand added directly to target intent."},
		{"sandbox_pool_refill_p95_seconds", "Effective creation-to-Ready quantile or configured fallback in seconds."},
		{"sandbox_pool_refill_learned", "Whether the effective refill lead time comes from learned samples."},
		{"sandbox_pool_refill_samples", "Number of persisted refill lead-time samples."},
		{"sandbox_pool_supply_capacity_rate", "Estimated sustainable refill throughput per second."},
		{"sandbox_pool_target_intent", "Inventory target before the supply capacity gate."},
		{"sandbox_pool_target_applied", "Inventory target applied to SandboxSet.spec.replicas."},
		{"sandbox_pool_available", "Currently available unclaimed sandboxes."},
		{"sandbox_pool_creating", "Currently non-ready sandboxes in the pool."},
		{"sandbox_pool_overflow_ratio", "Cold-start share of recently served demand in the overflow window."},
		{"sandbox_pool_cold_served", "Cold-started replicas served inside the current overflow window."},
		{"sandbox_pool_z_effective", "Effective safety-stock z score after overflow-budget calibration."},
		{"sandbox_pool_drain_boost", "Whether accelerated post-burst drain is currently active."},
	}
	for _, description := range descriptions {
		writeMetricHelp(writer, description.name, description.description, "gauge")
		for _, pool := range pools {
			value := metricValue(description.name, pool)
			fmt.Fprintf(
				writer,
				"%s{namespace=%q,pool=%q} %s\n",
				description.name,
				escapeLabel(pool.namespace),
				escapeLabel(pool.name),
				value,
			)
		}
	}
	writeMetricHelp(writer, "sandbox_pool_state", "Current pool controller state.", "gauge")
	for _, pool := range pools {
		fmt.Fprintf(
			writer,
			"sandbox_pool_state{namespace=%q,pool=%q,state=%q} 1\n",
			escapeLabel(pool.namespace),
			escapeLabel(pool.name),
			escapeLabel(pool.state),
		)
	}
}

func metricValue(name string, pool poolMetrics) string {
	switch name {
	case "sandbox_pool_demand_rate":
		return formatFloat(pool.demandRate)
	case "sandbox_pool_demand_rate_fast":
		return formatFloat(pool.fastRate)
	case "sandbox_pool_demand_rate_medium":
		return formatFloat(pool.mediumRate)
	case "sandbox_pool_demand_rate_slow":
		return formatFloat(pool.slowRate)
	case "sandbox_pool_pending_demand":
		return strconv.Itoa(pool.pendingDemand)
	case "sandbox_pool_queue_boost":
		return strconv.Itoa(pool.queueBoost)
	case "sandbox_pool_refill_p95_seconds":
		return formatFloat(pool.refillP95Seconds)
	case "sandbox_pool_refill_learned":
		return strconv.Itoa(boolInt(pool.refillLearned))
	case "sandbox_pool_refill_samples":
		return strconv.Itoa(pool.refillSamples)
	case "sandbox_pool_supply_capacity_rate":
		return formatFloat(pool.supplyCapacityRate)
	case "sandbox_pool_target_intent":
		return strconv.Itoa(pool.intent)
	case "sandbox_pool_target_applied":
		return strconv.Itoa(pool.applied)
	case "sandbox_pool_available":
		return strconv.Itoa(pool.available)
	case "sandbox_pool_creating":
		return strconv.Itoa(pool.creating)
	case "sandbox_pool_overflow_ratio":
		return formatFloat(pool.overflowRatio)
	case "sandbox_pool_cold_served":
		return strconv.Itoa(pool.coldServed)
	case "sandbox_pool_z_effective":
		return formatFloat(pool.zEffective)
	case "sandbox_pool_drain_boost":
		return strconv.Itoa(boolInt(pool.drainBoost))
	default:
		return "0"
	}
}

func writeMetricHelp(writer http.ResponseWriter, name, description, metricType string) {
	fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s %s\n", name, description, name, metricType)
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return strings.ReplaceAll(value, "\"", "\\\"")
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func startHTTPServer(ctx context.Context, address string, metrics *metricsRegistry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ready\n"))
	})
	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("health and metrics server listening on %s", address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server failed: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown failed: %v", err)
		}
	}()
}
