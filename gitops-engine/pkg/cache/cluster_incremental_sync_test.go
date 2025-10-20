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
	})
}
