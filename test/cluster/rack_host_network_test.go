package cluster

import (
	"testing"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
	"github.com/aerospike/aerospike-kubernetes-operator/v4/internal/controller/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestHostNetworkWithoutRackOverride(t *testing.T) {
	// Test case 1: hostNetwork=true at cluster level, no rack override
	// Expected: both racks should have hostNetwork=true
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = true
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks := []asdbv1.Rack{
		{ID: 1},
		{ID: 2},
	}
	expectedHostNetworkById := map[int]bool{
		1: true,
		2: true,
	}
	aeroCluster.Spec.RackConfig.Racks = racks

	for _, rack := range aeroCluster.Spec.RackConfig.Racks {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(&rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}

	// Test case 2: hostNetwork=false at cluster level, no rack override
	// Expected: both racks should have hostNetwork=false
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = false
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1},
		{ID: 2},
	}
	expectedHostNetworkById = map[int]bool{
		1: false,
		2: false,
	}
	aeroCluster.Spec.RackConfig.Racks = racks

	for _, rack := range aeroCluster.Spec.RackConfig.Racks {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(&rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}
}

func TestHostNetworkWithRackOverride(t *testing.T) {
	// Test case 1: hostNetwork=true at cluster level, rack 1 overrides to false via InputPodSpec
	// Expected: rack 1 = false, rack 2 = true
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = true
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks := []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
		{ID: 2},
	}
	expectedHostNetworkById := map[int]bool{
		1: false,
		2: true,
	}
	aeroCluster.Spec.RackConfig.Racks = racks

	for _, rack := range aeroCluster.Spec.RackConfig.Racks {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(&rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}

	// Test case 2: hostNetwork=false at cluster level, rack 1 overrides to true via InputPodSpec
	// Expected: rack 1 = true, rack 2 = false
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = false
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
		{ID: 2},
	}
	expectedHostNetworkById = map[int]bool{
		1: true,
		2: false,
	}
	aeroCluster.Spec.RackConfig.Racks = racks

	for _, rack := range aeroCluster.Spec.RackConfig.Racks {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(&rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}

	// Test case 3: hostNetwork=true at cluster level, both racks override to true via InputPodSpec
	// Expected: rack 1 = true, rack 2 = true
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = true
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
		{ID: 2, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
	}
	expectedHostNetworkById = map[int]bool{
		1: true,
		2: true,
	}
	aeroCluster.Spec.RackConfig.Racks = racks

	for _, rack := range aeroCluster.Spec.RackConfig.Racks {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(&rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}

	// Test case 4: hostNetwork=true at cluster level, both racks override to false via InputPodSpec
	// Expected: rack 1 = false, rack 2 = false
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = true
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
		{ID: 2, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
	}
	expectedHostNetworkById = map[int]bool{
		1: false,
		2: false,
	}
	aeroCluster.Spec.RackConfig.Racks = racks

	for _, rack := range aeroCluster.Spec.RackConfig.Racks {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(&rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}
}

// TestRackHostNetworkWithRackState tests GetRackHostNetwork through RackState
// similar to how it's used in statefulset.go and configmap.go
func TestRackHostNetworkWithRackState(t *testing.T) {
	// Test case 1: cluster hostNetwork=false, rack 1 overrides to true via InputPodSpec
	// Simulates statefulset.go usage pattern
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = false
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
		{ID: 2},
	}
	expectedHostNetworkById := map[int]bool{
		1: true,
		2: false,
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(rackState.Rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rackState.Rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rackState for rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rackState.Rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}

	// Test case 2: cluster hostNetwork=true, rack 2 overrides to false via InputPodSpec
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = true
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1},
		{ID: 2, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
	}
	expectedHostNetworkById = map[int]bool{
		1: true,
		2: false,
	}

	rackStates = cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(rackState.Rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rackState.Rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rackState for rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rackState.Rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}

	// Test case 3: Mixed configuration - cluster false, rack 1 true, rack 2 false explicit
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = false
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
		{ID: 2, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
	}
	expectedHostNetworkById = map[int]bool{
		1: true,
		2: false,
	}

	rackStates = cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(rackState.Rack, aeroCluster.Spec.PodSpec.HostNetwork)
		expectedHostNetwork := expectedHostNetworkById[rackState.Rack.ID]
		if effectiveHostNetwork != expectedHostNetwork {
			t.Errorf(`rackState for rack %d with hostNetwork %v does not match expected hostNetwork of %v`,
				rackState.Rack.ID, effectiveHostNetwork, expectedHostNetwork)
		}
	}
}

// TestRackHostNetworkNilPointerSafety tests that GetRackHostNetwork handles nil rack safely
func TestRackHostNetworkNilPointerSafety(t *testing.T) {
	// Test with InputPodSpec nil (not set)
	rack := asdbv1.Rack{ID: 1, InputPodSpec: nil}

	// Should return cluster-level value when rack InputPodSpec is nil
	if asdbv1.GetRackHostNetwork(&rack, true) != true {
		t.Error("expected true when rack.InputPodSpec is nil and clusterHostNetwork is true")
	}

	if asdbv1.GetRackHostNetwork(&rack, false) != false {
		t.Error("expected false when rack.InputPodSpec is nil and clusterHostNetwork is false")
	}

	// Test with InputPodSpec set but HostNetwork nil
	rack = asdbv1.Rack{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: nil}}

	if asdbv1.GetRackHostNetwork(&rack, true) != true {
		t.Error("expected true when rack.InputPodSpec.HostNetwork is nil and clusterHostNetwork is true")
	}

	if asdbv1.GetRackHostNetwork(&rack, false) != false {
		t.Error("expected false when rack.InputPodSpec.HostNetwork is nil and clusterHostNetwork is false")
	}

	// Test with InputPodSpec.HostNetwork set to false
	rack = asdbv1.Rack{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}}

	if asdbv1.GetRackHostNetwork(&rack, true) != false {
		t.Error("expected false when rack.InputPodSpec.HostNetwork is ptr.To(false), regardless of clusterHostNetwork")
	}

	// Test with InputPodSpec.HostNetwork set to true
	rack = asdbv1.Rack{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}}

	if asdbv1.GetRackHostNetwork(&rack, false) != true {
		t.Error("expected true when rack.InputPodSpec.HostNetwork is ptr.To(true), regardless of clusterHostNetwork")
	}
}

// TestRackHostNetworkDNSPolicyIntegration tests the integration of rack hostNetwork
// with DNS policy calculation through RackState
func TestRackHostNetworkDNSPolicyIntegration(t *testing.T) {
	// Test case 1: cluster hostNetwork=false (DNSClusterFirst default),
	// rack 1 overrides hostNetwork=true via InputPodSpec
	// Expected: rack 1 should get DNSClusterFirstWithHostNet, rack 2 gets DNSClusterFirst
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = false
	aeroCluster.Spec.PodSpec.InputDNSPolicy = nil // Use defaults
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
		{ID: 2},
	}
	expectedDNSPolicyById := map[int]corev1.DNSPolicy{
		1: corev1.DNSClusterFirstWithHostNet,
		2: corev1.DNSClusterFirst,
	}

	rackStates := cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(rackState.Rack, aeroCluster.Spec.PodSpec.HostNetwork)
		effectiveDNSPolicy := asdbv1.GetRackDNSPolicy(rackState.Rack, aeroCluster.Spec.PodSpec.InputDNSPolicy, effectiveHostNetwork)
		expectedDNSPolicy := expectedDNSPolicyById[rackState.Rack.ID]
		if effectiveDNSPolicy != expectedDNSPolicy {
			t.Errorf(`rack %d with effective DNSPolicy %v does not match expected %v`,
				rackState.Rack.ID, effectiveDNSPolicy, expectedDNSPolicy)
		}
	}

	// Test case 2: cluster hostNetwork=true (DNSClusterFirstWithHostNet default),
	// rack 2 overrides hostNetwork=false via InputPodSpec
	// Expected: rack 1 gets DNSClusterFirstWithHostNet, rack 2 gets DNSClusterFirst
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = true
	aeroCluster.Spec.PodSpec.InputDNSPolicy = nil
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1},
		{ID: 2, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
	}
	expectedDNSPolicyById = map[int]corev1.DNSPolicy{
		1: corev1.DNSClusterFirstWithHostNet,
		2: corev1.DNSClusterFirst,
	}

	rackStates = cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(rackState.Rack, aeroCluster.Spec.PodSpec.HostNetwork)
		effectiveDNSPolicy := asdbv1.GetRackDNSPolicy(rackState.Rack, aeroCluster.Spec.PodSpec.InputDNSPolicy, effectiveHostNetwork)
		expectedDNSPolicy := expectedDNSPolicyById[rackState.Rack.ID]
		if effectiveDNSPolicy != expectedDNSPolicy {
			t.Errorf(`rack %d with effective DNSPolicy %v does not match expected %v`,
				rackState.Rack.ID, effectiveDNSPolicy, expectedDNSPolicy)
		}
	}

	// Test case 3: rack-level DNSPolicy override takes precedence
	// rack 1 has explicit DNSNone, rack 2 uses cluster default
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = true
	aeroCluster.Spec.PodSpec.InputDNSPolicy = nil
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	dnsNone := corev1.DNSNone
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{InputDNSPolicy: &dnsNone}},
		{ID: 2},
	}
	expectedDNSPolicyById = map[int]corev1.DNSPolicy{
		1: corev1.DNSNone,
		2: corev1.DNSClusterFirstWithHostNet,
	}

	rackStates = cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(rackState.Rack, aeroCluster.Spec.PodSpec.HostNetwork)
		effectiveDNSPolicy := asdbv1.GetRackDNSPolicy(rackState.Rack, aeroCluster.Spec.PodSpec.InputDNSPolicy, effectiveHostNetwork)
		expectedDNSPolicy := expectedDNSPolicyById[rackState.Rack.ID]
		if effectiveDNSPolicy != expectedDNSPolicy {
			t.Errorf(`rack %d with effective DNSPolicy %v does not match expected %v`,
				rackState.Rack.ID, effectiveDNSPolicy, expectedDNSPolicy)
		}
	}

	// Test case 4: rack DNSPolicy overrides cluster DNSPolicy
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.PodSpec.HostNetwork = false
	clusterDNS := corev1.DNSClusterFirst
	aeroCluster.Spec.PodSpec.InputDNSPolicy = &clusterDNS
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	rackDNS := corev1.DNSClusterFirstWithHostNet
	aeroCluster.Spec.RackConfig.Racks = []asdbv1.Rack{
		{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{InputDNSPolicy: &rackDNS}},
		{ID: 2},
	}
	expectedDNSPolicyById = map[int]corev1.DNSPolicy{
		1: corev1.DNSClusterFirstWithHostNet, // rack override
		2: corev1.DNSClusterFirst,            // cluster setting
	}

	rackStates = cluster.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		effectiveHostNetwork := asdbv1.GetRackHostNetwork(rackState.Rack, aeroCluster.Spec.PodSpec.HostNetwork)
		effectiveDNSPolicy := asdbv1.GetRackDNSPolicy(rackState.Rack, aeroCluster.Spec.PodSpec.InputDNSPolicy, effectiveHostNetwork)
		expectedDNSPolicy := expectedDNSPolicyById[rackState.Rack.ID]
		if effectiveDNSPolicy != expectedDNSPolicy {
			t.Errorf(`rack %d with effective DNSPolicy %v does not match expected %v`,
				rackState.Rack.ID, effectiveDNSPolicy, expectedDNSPolicy)
		}
	}
}
