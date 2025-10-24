package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	testcore "k8s.io/client-go/testing"

	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	"github.com/argoproj/gitops-engine/pkg/utils/kube/kubetest"
)

func TestAddNamespace(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(&kubetest.MockKubectlCmd{}),
			SetNamespaces([]string{"existing-namespace"}),
		)

		// Given: cache was previously synced
		now := time.Now()
		cache.syncStatus.lock.Lock()
		cache.syncStatus.syncTime = &now
		cache.syncStatus.lock.Unlock()

		// When: adding a namespace with feature disabled
		err := cache.AddNamespace("new-namespace")
		assert.NoError(t, err)

		// Then: should invalidate the cache (observable via syncTime being cleared)
		cache.syncStatus.lock.Lock()
		actual := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()
		assert.Nil(t, actual, "given feature disabled, should invalidate cache when namespace added")

		// Then: should add namespace to the list
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

		// Given: cache was previously synced
		now := time.Now()
		cache.syncStatus.lock.Lock()
		cache.syncStatus.syncTime = &now
		cache.syncStatus.lock.Unlock()

		// When: adding a namespace with feature enabled
		err := cache.AddNamespace("new-namespace")
		assert.NoError(t, err)

		// Then: should NOT invalidate the cache (syncTime preserved)
		cache.syncStatus.lock.Lock()
		actual := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()

		assert.NotNil(t, actual, "given feature enabled, should preserve cache when namespace added")
		assert.Equal(t, now.Unix(), actual.Unix(), "given feature enabled, should not change syncTime")

		// Then: should add namespace to the list
		assert.Contains(t, cache.namespaces, "new-namespace", "given feature enabled, should add namespace to list")
		assert.Contains(t, cache.namespaces, "existing-namespace", "given feature enabled, should preserve existing namespaces")
	})

	t.Run("feature enabled syncs resources in new namespace", func(t *testing.T) {
		// Given: a pod exists in the new namespace
		pod := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "new-namespace"},
		}

		_, mockKubectl := setupFakeCluster(pod)
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(mockKubectl),
			SetNamespaces([]string{"existing-namespace"}),
			WithIncrementalNamespaceSync(true),
		)

		// Given: cache was previously synced (to populate apisMeta)
		err := cache.EnsureSynced()
		assert.NoError(t, err)

		// Store the sync time to verify it's preserved.
		cache.syncStatus.lock.Lock()
		syncTimeBefore := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()

		// When: adding a namespace with feature enabled
		err = cache.AddNamespace("new-namespace")
		assert.NoError(t, err)

		// Then: should preserve the sync time (not invalidate)
		cache.syncStatus.lock.Lock()
		syncTimeAfter := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()
		assert.Equal(t, syncTimeBefore, syncTimeAfter, "sync time should be preserved")

		// Then: should have the pod from new namespace in the cache
		assertPodInCache(t, cache, "new-namespace", "test-pod", "pod from new namespace should be in cache")
	})

	t.Run("feature enabled watches new namespace for changes", func(t *testing.T) {
		// Given: initial pod in existing namespace
		existingPod := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "existing-pod", Namespace: "existing-namespace"},
		}

		client, mockKubectl := setupFakeCluster(existingPod)
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(mockKubectl),
			SetNamespaces([]string{"existing-namespace"}),
			WithIncrementalNamespaceSync(true),
		)

		// Given: cache was previously synced (starts watches for existing namespace)
		err := cache.EnsureSynced()
		assert.NoError(t, err)

		// When: adding a new namespace
		err = cache.AddNamespace("new-namespace")
		assert.NoError(t, err)

		// Give watch goroutines time to start before creating the pod
		time.Sleep(50 * time.Millisecond)

		// When: a new pod is created in the new namespace AFTER AddNamespace
		newPod := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "new-pod", Namespace: "new-namespace", ResourceVersion: "124"},
		}

		podClient := client.Resource(schema.GroupVersionResource{
			Group: "", Version: "v1", Resource: "pods",
		}).Namespace("new-namespace")

		_, err = podClient.Create(context.Background(), mustToUnstructured(newPod), metav1.CreateOptions{})
		assert.NoError(t, err)

		// Give watch goroutines time to start after creating the pod
		time.Sleep(50 * time.Millisecond)

		// Then: the watch should pick up the new pod and add it to cache
		assertPodInCache(t, cache, "new-namespace", "new-pod", "pod created after AddNamespace should be in cache (proves watches are active)")
	})

	t.Run("feature enabled returns error for non-RBAC errors", func(t *testing.T) {
		// Given: a fake cluster that returns generic errors (not RBAC)
		client := fake.NewSimpleDynamicClient(scheme.Scheme)

		// Given: setup reactor to return generic error for "error-namespace"
		client.PrependReactor("list", "pods", func(action testcore.Action) (handled bool, ret runtime.Object, err error) {
			listAction := action.(testcore.ListAction)
			if listAction.GetNamespace() == "error-namespace" {
				return true, nil, apierrors.NewInternalError(fmt.Errorf("internal server error"))
			}
			return false, nil, nil
		})

		apiResources := []kube.APIResourceInfo{{
			GroupKind:            schema.GroupKind{Group: "", Kind: "Pod"},
			GroupVersionResource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			Meta:                 metav1.APIResource{Namespaced: true},
		}}

		mockKubectl := &kubetest.MockKubectlCmd{
			DynamicClient: client,
			APIResources:  apiResources,
			Version:       "v1.28.0",
		}

		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(mockKubectl),
			SetNamespaces([]string{"existing-namespace"}),
			WithIncrementalNamespaceSync(true),
			SetRespectRBAC(RespectRbacNormal),
		)

		// Given: cache was previously synced
		err := cache.EnsureSynced()
		assert.NoError(t, err)

		// When: adding a namespace with non-RBAC errors
		err = cache.AddNamespace("error-namespace")

		// Then: should return error (only RBAC errors should be ignored)
		assert.Error(t, err, "AddNamespace should return error for non-RBAC errors")
	})

	t.Run("feature enabled watches are canceled on Invalidate", func(t *testing.T) {
		// Given: a pod exists in the new namespace
		pod := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "new-namespace"},
		}

		_, mockKubectl := setupFakeCluster(pod)
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(mockKubectl),
			SetNamespaces([]string{"existing-namespace"}),
			WithIncrementalNamespaceSync(true),
		)

		// Given: cache was previously synced (to populate apisMeta)
		err := cache.EnsureSynced()
		assert.NoError(t, err)

		// When: adding a namespace with feature enabled
		err = cache.AddNamespace("new-namespace")
		assert.NoError(t, err)

		// then: should have apiMeta with namespace cancel context
		cache.lock.RLock()
		podMeta, exists := cache.apisMeta[schema.GroupKind{Group: "", Kind: "Pod"}]
		cache.lock.RUnlock()
		assert.True(t, exists, "Pod apiMeta should exist")
		assert.NotNil(t, podMeta.namespaceCancels, "namespaceCancels map should be initialized")

		nsCancel, hasNsCancel := podMeta.namespaceCancels["new-namespace"]
		assert.True(t, hasNsCancel, "new-namespace should have a cancel function")
		assert.NotNil(t, nsCancel, "namespace cancel function should not be nil")

		// Then: namespace context should not be canceled yet
		assert.NotNil(t, podMeta.watchCtx, "watchCtx should be set")
		select {
		case <-podMeta.watchCtx.Done():
			t.Fatal("watch context should not be canceled yet")
		default:
			// Context is still active - good!
		}

		// When: invalidating the cache
		cache.Invalidate()

		// Then: the parent watch context should be canceled (cascades to namespace contexts)
		select {
		case <-podMeta.watchCtx.Done():
			// Context was canceled - good!
		case <-time.After(100 * time.Millisecond):
			t.Fatal("watch context should be canceled after Invalidate")
		}
	})
}

func TestRemoveNamespace(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(&kubetest.MockKubectlCmd{}),
			SetNamespaces([]string{"ns-1", "ns-2"}),
		)

		// Given: cache was previously synced
		now := time.Now()
		cache.syncStatus.lock.Lock()
		cache.syncStatus.syncTime = &now
		cache.syncStatus.lock.Unlock()

		// When: removing a namespace with feature disabled
		err := cache.RemoveNamespace("ns-2")

		assert.NoError(t, err)

		// Then: should invalidate the cache (observable via syncTime being cleared)
		cache.syncStatus.lock.Lock()
		actual := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()
		assert.Nil(t, actual, "given feature disabled, should invalidate cache when namespace removed")

		// Then: should remove namespace from the list
		assert.NotContains(t, cache.namespaces, "ns-2", "given feature disabled, should remove namespace from list")
		assert.Contains(t, cache.namespaces, "ns-1", "given feature disabled, should preserve remaining namespaces")
	})

	t.Run("feature enabled", func(t *testing.T) {
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(&kubetest.MockKubectlCmd{}),
			SetNamespaces([]string{"ns-1", "ns-2"}),
			WithIncrementalNamespaceSync(true),
		)

		// Given: cache was previously synced
		now := time.Now()
		cache.syncStatus.lock.Lock()
		cache.syncStatus.syncTime = &now
		cache.syncStatus.lock.Unlock()

		// When: removing a namespace with feature enabled
		err := cache.RemoveNamespace("ns-2")

		// Then: should not return error
		assert.NoError(t, err)

		// Then: should NOT invalidate the cache (syncTime preserved)
		cache.syncStatus.lock.Lock()
		actual := cache.syncStatus.syncTime
		cache.syncStatus.lock.Unlock()

		assert.NotNil(t, actual, "given feature enabled, should preserve cache when namespace removed")
		assert.Equal(t, now.Unix(), actual.Unix(), "given feature enabled, should not change syncTime")

		// Then: should remove namespace from the list
		assert.NotContains(t, cache.namespaces, "ns-2", "given feature enabled, should remove namespace from list")
		assert.Contains(t, cache.namespaces, "ns-1", "given feature enabled, should preserve remaining namespaces")
	})

	t.Run("feature enabled removes resources from removed namespace", func(t *testing.T) {
		// Given: pods exist in both namespaces
		pod1 := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns-1"},
		}
		pod2 := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "ns-2"},
		}

		_, mockKubectl := setupFakeCluster(pod1, pod2)
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(mockKubectl),
			SetNamespaces([]string{"ns-1", "ns-2"}),
			WithIncrementalNamespaceSync(true),
		)

		// Given: cache was previously synced (both pods in cache)
		err := cache.EnsureSynced()
		assert.NoError(t, err)

		// Given: verify both pods are in cache
		assertPodInCache(t, cache, "ns-1", "pod-1", "pod-1 should be in cache before removal")
		assertPodInCache(t, cache, "ns-2", "pod-2", "pod-2 should be in cache before removal")

		// When: removing ns-2
		err = cache.RemoveNamespace("ns-2")
		assert.NoError(t, err)

		// Then: pod-2 from ns-2 should be removed from cache
		cache.lock.RLock()
		_, exists := cache.resources[kube.NewResourceKey("", "Pod", "ns-2", "pod-2")]
		cache.lock.RUnlock()
		assert.False(t, exists, "pod-2 from removed namespace should not be in cache")

		// Then: pod-1 from ns-1 should still be in cache
		assertPodInCache(t, cache, "ns-1", "pod-1", "pod-1 from remaining namespace should still be in cache")
	})

	t.Run("feature enabled cancels watches when namespace removed", func(t *testing.T) {
		// Given: pods exist in both namespaces
		pod1 := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns-1"},
		}
		pod2 := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "ns-2"},
		}

		_, mockKubectl := setupFakeCluster(pod1, pod2)
		cache := NewClusterCache(
			&rest.Config{},
			SetKubectl(mockKubectl),
			SetNamespaces([]string{"ns-1"}),
			WithIncrementalNamespaceSync(true),
		)

		// Given: cache was previously synced
		err := cache.EnsureSynced()
		assert.NoError(t, err)

		// When: adding ns-2 with feature enabled (creates per-namespace context)
		err = cache.AddNamespace("ns-2")
		assert.NoError(t, err)

		// Then: should have namespace cancel context for ns-2
		cache.lock.RLock()
		podMeta, exists := cache.apisMeta[schema.GroupKind{Group: "", Kind: "Pod"}]
		cache.lock.RUnlock()
		assert.True(t, exists, "Pod apiMeta should exist")

		ns2Cancel, hasNs2Cancel := podMeta.namespaceCancels["ns-2"]
		assert.True(t, hasNs2Cancel, "ns-2 should have a cancel function")
		assert.NotNil(t, ns2Cancel, "ns-2 cancel function should not be nil")

		// When: removing ns-2
		err = cache.RemoveNamespace("ns-2")
		assert.NoError(t, err)

		// Then: ns-2 should be removed from namespaceCancels map
		_, hasNs2Cancel = podMeta.namespaceCancels["ns-2"]
		assert.False(t, hasNs2Cancel, "ns-2 should be removed from namespaceCancels after RemoveNamespace")

		// Then: ns-1 should still have its cancel function (not affected)
		cache.lock.RLock()
		_, hasNs1Cancel := podMeta.namespaceCancels["ns-1"]
		cache.lock.RUnlock()
		// Note: ns-1 was added during initial sync, which doesn't create per-namespace contexts
		// So this test verifies that RemoveNamespace only affects the removed namespace
		assert.False(t, hasNs1Cancel, "ns-1 should not have per-namespace cancel (was added during sync)")
	})
}

func setupFakeCluster(objs ...runtime.Object) (*fake.FakeDynamicClient, *kubetest.MockKubectlCmd) {
	client := fake.NewSimpleDynamicClient(scheme.Scheme, objs...)
	reactor := client.ReactionChain[0]
	client.PrependReactor("list", "*", func(action testcore.Action) (handled bool, ret runtime.Object, err error) {
		handled, ret, err = reactor.React(action)
		if err != nil || !handled {
			return handled, ret, err
		}
		ret.(metav1.ListInterface).SetResourceVersion("123")
		return handled, ret, err
	})

	apiResources := []kube.APIResourceInfo{{
		GroupKind:            schema.GroupKind{Group: "", Kind: "Pod"},
		GroupVersionResource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Meta:                 metav1.APIResource{Namespaced: true},
	}}

	mockKubectl := &kubetest.MockKubectlCmd{
		DynamicClient: client,
		APIResources:  apiResources,
		Version:       "v1.28.0",
	}

	return client, mockKubectl
}

func assertPodInCache(t *testing.T, cache *clusterCache, namespace, name, message string) {
	t.Helper()
	podKey := kube.NewResourceKey("", "Pod", namespace, name)
	cache.lock.RLock()
	cachedPod, exists := cache.resources[podKey]
	cache.lock.RUnlock()

	assert.True(t, exists, message)
	assert.NotNil(t, cachedPod, "cached pod should not be nil")
	if cachedPod != nil {
		assert.Equal(t, name, cachedPod.Ref.Name)
		assert.Equal(t, namespace, cachedPod.Ref.Namespace)
	}
}
