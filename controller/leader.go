package main

import (
	"context"
	"log"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const leaderLeaseName = "sandbox-pool-controller"

func runLeaderElection(
	ctx context.Context,
	kube kubernetes.Interface,
	namespace string,
	identity string,
	ctrl *controller,
	interval time.Duration,
	metrics *metricsRegistry,
) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaderLeaseName,
			Namespace: namespace,
		},
		Client: kube.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		ReleaseOnCancel: true,
		Name:            leaderLeaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				metrics.setLeader(true)
				log.Printf("became leader identity=%s", identity)
				ctrl.run(leaderCtx, interval)
			},
			OnStoppedLeading: func() {
				metrics.setLeader(false)
				log.Printf("leadership lost identity=%s", identity)
			},
			OnNewLeader: func(newIdentity string) {
				if newIdentity != identity {
					log.Printf("leader changed identity=%s", newIdentity)
				}
			},
		},
	})
}
