package test

import (
	"testing"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/api/v1"
	"github.com/aerospike/aerospike-kubernetes-operator/controllers"
)

func TestClusterSizeWithoutRackSize(t *testing.T) {
	// Cluster size of 4 assigned to 3 racks
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 4}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks := []asdbv1.Rack{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}
	expectedRackSizeById := map[int]int{
		1: 2,
		2: 1,
		3: 1,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates := controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}

	// Cluster size of 7 assigned to 2 racks
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 7}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1},
		{ID: 2},
	}
	expectedRackSizeById = map[int]int{
		1: 4,
		2: 3,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates = controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}

	// Cluster size of 2 assigned to 3 racks
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}
	expectedRackSizeById = map[int]int{
		1: 1,
		2: 1,
		3: 0,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates = controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}

	// Cluster size of 2 assigned to 1 racks
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1},
	}
	expectedRackSizeById = map[int]int{
		1: 2,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates = controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}

	// Cluster size of 2 assigned to 2 racks
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 2}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1},
		{ID: 2},
	}
	expectedRackSizeById = map[int]int{
		1: 1,
		2: 1,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates = controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}
}

func TestClusterSizeWithRackSize(t *testing.T) {
	// Cluster size of 7 assigned to 3 racks
	// All of them has defined size
	aeroCluster := &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 7}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks := []asdbv1.Rack{
		{ID: 1, Size: 3},
		{ID: 2, Size: 3},
		{ID: 3, Size: 1},
	}
	expectedRackSizeById := map[int]int{
		1: 3,
		2: 3,
		3: 1,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates := controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}

	// Cluster size of 7 assigned to 4 racks
	// Two of them has a defined size of 1
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 7}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1, Size: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4, Size: 1},
	}
	expectedRackSizeById = map[int]int{
		1: 1,
		2: 3,
		3: 2,
		4: 1,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates = controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}

	// Cluster size of 4 assigned to 1 racks with size 4
	aeroCluster = &asdbv1.AerospikeCluster{}
	aeroCluster.Spec = asdbv1.AerospikeClusterSpec{Size: 4}
	aeroCluster.Spec.RackConfig = asdbv1.RackConfig{}
	racks = []asdbv1.Rack{
		{ID: 1, Size: 4},
	}
	expectedRackSizeById = map[int]int{
		1: 4,
	}
	aeroCluster.Spec.RackConfig.Racks = racks
	rackStates = controllers.GetConfiguredRackStateList(aeroCluster)
	for _, rackState := range rackStates {
		expectedSize := expectedRackSizeById[rackState.Rack.ID]
		if rackState.Size != expectedSize {
			t.Errorf(`rack %d with size %d does not match expected size of %d`, rackState.Rack.ID, rackState.Size, expectedSize)
		}
	}
}
