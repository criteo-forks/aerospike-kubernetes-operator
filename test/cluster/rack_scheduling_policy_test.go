package cluster

import (
	"testing"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
	"github.com/aerospike/aerospike-kubernetes-operator/v4/internal/controller/cluster"
	corev1 "k8s.io/api/core/v1"
)

func TestAffinityWithoutRackOverride(t *testing.T) {
	// cluster-level affinity set, no rack override
	// Expected: both racks should have nil PodSpec.Affinity (no override)
	clusterAffinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-east-1"}},
						},
					},
				},
			},
		},
	}

	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.Affinity = clusterAffinity
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, Size: 1},
		{ID: 2, Size: 1},
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		if rackState.Rack.PodSpec.Affinity != nil {
			t.Errorf("rack %d should have nil PodSpec.Affinity when no rack override is set", rackState.Rack.ID)
		}
	}
}

func TestAffinityWithRackOverride(t *testing.T) {
	// cluster-level affinity set, rack 1 overrides with different affinity via effective PodSpec
	clusterAffinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-east-1"}},
						},
					},
				},
			},
		},
	}
	rackAffinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"eu-west-1"}},
						},
					},
				},
			},
		},
	}

	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.Affinity = clusterAffinity
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, Size: 1, PodSpec: asdbv1.RackPodSpec{
			SchedulingPolicy: asdbv1.SchedulingPolicy{Affinity: rackAffinity},
		}},
		{ID: 2, Size: 1},
	}

	expectedAffinityByID := map[int]*corev1.Affinity{
		1: rackAffinity,
		2: nil,
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expected := expectedAffinityByID[rackState.Rack.ID]
		actual := rackState.Rack.PodSpec.Affinity
		if expected == nil && actual != nil {
			t.Errorf("rack %d expected nil affinity, got non-nil", rackState.Rack.ID)
		}
		if expected != nil && actual == nil {
			t.Errorf("rack %d expected non-nil affinity, got nil", rackState.Rack.ID)
		}
		if expected != nil && actual != nil {
			actualVal := actual.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.
				NodeSelectorTerms[0].MatchExpressions[0].Values[0]
			expectedVal := expected.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.
				NodeSelectorTerms[0].MatchExpressions[0].Values[0]
			if actualVal != expectedVal {
				t.Errorf("rack %d expected affinity zone %s, got %s", rackState.Rack.ID, expectedVal, actualVal)
			}
		}
	}
}

func TestTolerationsWithoutRackOverride(t *testing.T) {
	// cluster-level tolerations set, no rack override
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.Tolerations = []corev1.Toleration{
		{Key: "key1", Operator: corev1.TolerationOpEqual, Value: "val1", Effect: corev1.TaintEffectNoSchedule},
	}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, Size: 1},
		{ID: 2, Size: 1},
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		if len(rackState.Rack.PodSpec.Tolerations) != 0 {
			t.Errorf("rack %d should have empty PodSpec.Tolerations when no rack override is set", rackState.Rack.ID)
		}
	}
}

func TestTolerationsWithRackOverride(t *testing.T) {
	// cluster-level tolerations set, rack 1 overrides via effective PodSpec
	rackTolerations := []corev1.Toleration{
		{Key: "rackKey", Operator: corev1.TolerationOpEqual, Value: "rackVal", Effect: corev1.TaintEffectNoExecute},
	}

	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.Tolerations = []corev1.Toleration{
		{Key: "key1", Operator: corev1.TolerationOpEqual, Value: "val1", Effect: corev1.TaintEffectNoSchedule},
	}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, Size: 1, PodSpec: asdbv1.RackPodSpec{
			SchedulingPolicy: asdbv1.SchedulingPolicy{Tolerations: rackTolerations},
		}},
		{ID: 2, Size: 1},
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		switch rackState.Rack.ID {
		case 1:
			if len(rackState.Rack.PodSpec.Tolerations) != 1 || rackState.Rack.PodSpec.Tolerations[0].Key != "rackKey" {
				t.Errorf("rack 1 expected rack-level toleration override, got %v", rackState.Rack.PodSpec.Tolerations)
			}
		case 2:
			if len(rackState.Rack.PodSpec.Tolerations) != 0 {
				t.Errorf("rack 2 expected no tolerations override, got %v", rackState.Rack.PodSpec.Tolerations)
			}
		}
	}
}

func TestNodeSelectorWithoutRackOverride(t *testing.T) {
	// cluster-level nodeSelector set, no rack override
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.NodeSelector = map[string]string{"disktype": "ssd"}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, Size: 1},
		{ID: 2, Size: 1},
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		if len(rackState.Rack.PodSpec.NodeSelector) != 0 {
			t.Errorf("rack %d should have empty PodSpec.NodeSelector when no rack override is set", rackState.Rack.ID)
		}
	}
}

func TestNodeSelectorWithRackOverride(t *testing.T) {
	// cluster-level nodeSelector set, rack 1 overrides via effective PodSpec
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.NodeSelector = map[string]string{"disktype": "ssd"}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, Size: 1, PodSpec: asdbv1.RackPodSpec{
			SchedulingPolicy: asdbv1.SchedulingPolicy{NodeSelector: map[string]string{"disktype": "hdd"}},
		}},
		{ID: 2, Size: 1},
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		switch rackState.Rack.ID {
		case 1:
			if rackState.Rack.PodSpec.NodeSelector["disktype"] != "hdd" {
				t.Errorf("rack 1 expected rack-level nodeSelector override 'hdd', got %v",
					rackState.Rack.PodSpec.NodeSelector)
			}
		case 2:
			if len(rackState.Rack.PodSpec.NodeSelector) != 0 {
				t.Errorf("rack 2 expected no nodeSelector override, got %v", rackState.Rack.PodSpec.NodeSelector)
			}
		}
	}
}
