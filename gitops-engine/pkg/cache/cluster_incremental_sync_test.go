package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
}
