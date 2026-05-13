package cluster

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
	"github.com/aerospike/aerospike-kubernetes-operator/v4/pkg/utils"
)

// computePodSpecHash mirrors the hash logic in getSTSConfData for testing purposes.
func computePodSpecHash(aeroCluster *asdbv1.AerospikeCluster, rack *asdbv1.Rack) (string, error) {
	podSpec := createPodSpecForRack(aeroCluster, rack)

	podSpecStr, err := json.Marshal(podSpec)
	if err != nil {
		return "", err
	}

	podSpecStr = []byte(strings.ReplaceAll(
		string(podSpecStr), "\"aerospikeInitContainer\":{},", "",
	))
	podSpecStr = []byte(strings.ReplaceAll(
		string(podSpecStr), "\"multiPodPerHost\":false,", "",
	))

	return utils.GetHash(string(podSpecStr))
}

func newCluster(affinity *corev1.Affinity, tolerations []corev1.Toleration, nodeSelector map[string]string,
) *asdbv1.AerospikeCluster {
	return &asdbv1.AerospikeCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: asdbv1.AerospikeClusterSpec{
			PodSpec: asdbv1.AerospikePodSpec{
				SchedulingPolicy: asdbv1.SchedulingPolicy{
					Affinity:     affinity,
					Tolerations:  tolerations,
					NodeSelector: nodeSelector,
				},
			},
		},
	}
}

func TestPodSpecHash_ClusterAffinityChange(t *testing.T) {
	rack := &asdbv1.Rack{ID: 1}

	clusterBefore := newCluster(nil, nil, nil)
	hashBefore, err := computePodSpecHash(clusterBefore, rack)
	if err != nil {
		t.Fatal(err)
	}

	clusterAfter := newCluster(&corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-east-1"}},
					}},
				},
			},
		},
	}, nil, nil)
	hashAfter, err := computePodSpecHash(clusterAfter, rack)
	if err != nil {
		t.Fatal(err)
	}

	if hashBefore == hashAfter {
		t.Fatal("expected hash to change when cluster-level affinity is added, but it didn't")
	}
}

func TestPodSpecHash_ClusterTolerationsChange(t *testing.T) {
	rack := &asdbv1.Rack{ID: 1}

	clusterBefore := newCluster(nil, nil, nil)
	hashBefore, err := computePodSpecHash(clusterBefore, rack)
	if err != nil {
		t.Fatal(err)
	}

	clusterAfter := newCluster(nil, []corev1.Toleration{
		{Key: "key1", Operator: corev1.TolerationOpEqual, Value: "val1", Effect: corev1.TaintEffectNoSchedule},
	}, nil)
	hashAfter, err := computePodSpecHash(clusterAfter, rack)
	if err != nil {
		t.Fatal(err)
	}

	if hashBefore == hashAfter {
		t.Fatal("expected hash to change when cluster-level tolerations are added, but it didn't")
	}
}

func TestPodSpecHash_ClusterNodeSelectorChange(t *testing.T) {
	rack := &asdbv1.Rack{ID: 1}

	clusterBefore := newCluster(nil, nil, nil)
	hashBefore, err := computePodSpecHash(clusterBefore, rack)
	if err != nil {
		t.Fatal(err)
	}

	clusterAfter := newCluster(nil, nil, map[string]string{"disktype": "ssd"})
	hashAfter, err := computePodSpecHash(clusterAfter, rack)
	if err != nil {
		t.Fatal(err)
	}

	if hashBefore == hashAfter {
		t.Fatal("expected hash to change when cluster-level nodeSelector is added, but it didn't")
	}
}

func TestPodSpecHash_ClusterServiceAccountChange(t *testing.T) {
	rack := &asdbv1.Rack{ID: 1}

	clusterBefore := newCluster(nil, nil, nil)
	hashBefore, err := computePodSpecHash(clusterBefore, rack)
	if err != nil {
		t.Fatal(err)
	}

	clusterAfter := newCluster(nil, nil, nil)
	clusterAfter.Spec.PodSpec.ServiceAccountName = "custom-aerospike-sa"
	hashAfter, err := computePodSpecHash(clusterAfter, rack)
	if err != nil {
		t.Fatal(err)
	}

	if hashBefore == hashAfter {
		t.Fatal("expected hash to change when cluster-level serviceAccountName is set, but it didn't")
	}
}

func TestPodSpecHash_NoChangeWhenNothingChanges(t *testing.T) {
	rack := &asdbv1.Rack{ID: 1}

	cluster1 := newCluster(&corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-east-1"}},
					}},
				},
			},
		},
	}, []corev1.Toleration{
		{Key: "key1", Operator: corev1.TolerationOpEqual, Value: "val1", Effect: corev1.TaintEffectNoSchedule},
	}, map[string]string{"disktype": "ssd"})

	cluster2 := newCluster(&corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-east-1"}},
					}},
				},
			},
		},
	}, []corev1.Toleration{
		{Key: "key1", Operator: corev1.TolerationOpEqual, Value: "val1", Effect: corev1.TaintEffectNoSchedule},
	}, map[string]string{"disktype": "ssd"})

	hash1, err := computePodSpecHash(cluster1, rack)
	if err != nil {
		t.Fatal(err)
	}

	hash2, err := computePodSpecHash(cluster2, rack)
	if err != nil {
		t.Fatal(err)
	}

	if hash1 != hash2 {
		t.Fatal("expected hash to be the same when nothing changes")
	}
}

func TestPodSpecHash_RackOverrideDoesNotChangeOnClusterChange(t *testing.T) {
	rackAffinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"eu-west-1"}},
					}},
				},
			},
		},
	}
	rack := &asdbv1.Rack{
		ID: 1,
		PodSpec: asdbv1.RackPodSpec{
			SchedulingPolicy: asdbv1.SchedulingPolicy{
				Affinity:     rackAffinity,
				Tolerations:  []corev1.Toleration{{Key: "rk", Operator: corev1.TolerationOpExists}},
				NodeSelector: map[string]string{"rack": "1"},
			},
		},
	}

	// Cluster with no scheduling policy
	clusterBefore := newCluster(nil, nil, nil)
	hashBefore, err := computePodSpecHash(clusterBefore, rack)
	if err != nil {
		t.Fatal(err)
	}

	// Cluster with different scheduling policy — should not affect hash because rack overrides all fields
	clusterAfter := newCluster(&corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"ap-south-1"}},
					}},
				},
			},
		},
	}, []corev1.Toleration{
		{Key: "other", Operator: corev1.TolerationOpExists},
	}, map[string]string{"cluster": "main"})

	hashAfter, err := computePodSpecHash(clusterAfter, rack)
	if err != nil {
		t.Fatal(err)
	}

	if hashBefore != hashAfter {
		t.Fatal("expected hash to stay the same when rack overrides all scheduling fields, regardless of cluster-level changes")
	}
}
