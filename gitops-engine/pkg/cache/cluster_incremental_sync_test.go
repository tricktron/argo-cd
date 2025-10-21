package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/argoproj/gitops-engine/pkg/utils/kube/kubetest"
)

func TestAddNamespace(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(&kubetest.MockKubectlCmd{}),
			SetNamespaces([]string{"existing-namespace"}),
		)

		// given: cache was previously synced
		now := time.Now()
		cache.syncStatus.lock.Lock()
		cache.syncStatus.syncTime = &now
		cache.syncStatus.lock.Unlock()

		// when: adding a namespace with feature disabled
		err := cache.AddNamespace("new-namespace")

		assert.NoError(t, err)

		// should: invalidate the cache (observable via syncTime being cleared)
		cache.syncStatus.lock.Lock()
		actual := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()
		assert.Nil(t, actual, "given feature disabled, should invalidate cache when namespace added")

		// should: add namespace to the list
		assert.Contains(t, cache.namespaces, "new-namespace", "given feature disabled, should add namespace to list")
		assert.Contains(t, cache.namespaces, "existing-namespace", "given feature disabled, should preserve existing namespaces")
	})

	t.Run("feature enabled", func(t *testing.T) {
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(&kubetest.MockKubectlCmd{}),
			SetNamespaces([]string{"existing-namespace"}),
			WithIncrementalNamespaceSync(true),
		)

		// given: cache was previously synced
		now := time.Now()
		cache.syncStatus.lock.Lock()
		cache.syncStatus.syncTime = &now
		cache.syncStatus.lock.Unlock()

		// when: adding a namespace with feature enabled
		err := cache.AddNamespace("new-namespace")

		// should: not return error
		assert.NoError(t, err)

		// should: NOT invalidate the cache (syncTime preserved)
		cache.syncStatus.lock.Lock()
		actual := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()

		assert.NotNil(t, actual, "given feature enabled, should preserve cache when namespace added")
		assert.Equal(t, now.Unix(), actual.Unix(), "given feature enabled, should not change syncTime")

		// should: add namespace to the list
		assert.Contains(t, cache.namespaces, "new-namespace", "given feature enabled, should add namespace to list")
		assert.Contains(t, cache.namespaces, "existing-namespace", "given feature enabled, should preserve existing namespaces")
	})

	t.Run("feature enabled syncs resources in new namespace", func(t *testing.T) {
		// given: Pod exists in existing-namespace
		existingPod := &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Pod",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "existing-pod",
				Namespace: "existing-namespace",
				UID:       "1",
			},
		}

		// given: Pod exists in new-namespace
		newPod := &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Pod",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "new-pod",
				Namespace: "new-namespace",
				UID:       "2",
			},
		}

		// given: cluster cache with incremental sync enabled, watching existing-namespace
		cache := newClusterWithOptions(t, []UpdateSettingsFunc{
			SetNamespaces([]string{"existing-namespace"}),
			WithIncrementalNamespaceSync(true),
		}, existingPod, newPod)

		// given: cache has been synced
		err := cache.EnsureSynced()
		require.NoError(t, err)

		// given: verify initial state - cache contains only existing-namespace pod
		cache.lock.RLock()
		initialResourceCount := len(cache.resources)
		hasExistingPod := false
		hasNewPod := false
		for k := range cache.resources {
			if k.Namespace == "existing-namespace" && k.Name == "existing-pod" {
				hasExistingPod = true
			}
			if k.Namespace == "new-namespace" && k.Name == "new-pod" {
				hasNewPod = true
			}
		}
		cache.lock.RUnlock()
		assert.True(t, hasExistingPod, "given existing-namespace watched, should have synced existing-pod")
		assert.False(t, hasNewPod, "given new-namespace not watched yet, should not have synced new-pod")
		assert.Equal(t, 1, initialResourceCount, "given only one namespace watched, should have exactly one resource")

		// when: adding new-namespace with incremental sync enabled
		err = cache.AddNamespace("new-namespace")
		require.NoError(t, err)

		// should: sync resources from new-namespace to cache
		cache.lock.RLock()
		finalResourceCount := len(cache.resources)
		hasExistingPodAfter := false
		hasNewPodAfter := false
		for k := range cache.resources {
			if k.Namespace == "existing-namespace" && k.Name == "existing-pod" {
				hasExistingPodAfter = true
			}
			if k.Namespace == "new-namespace" && k.Name == "new-pod" {
				hasNewPodAfter = true
			}
		}
		cache.lock.RUnlock()

		assert.True(t, hasNewPodAfter, "given incremental sync enabled and namespace added, should sync resources from new-namespace")
		assert.True(t, hasExistingPodAfter, "given incremental sync, should preserve existing resources")
		assert.Equal(t, 2, finalResourceCount, "given two namespaces watched, should have exactly two resources")
	})
}
