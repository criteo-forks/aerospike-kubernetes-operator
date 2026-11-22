package v1

import (
	"testing"

	"github.com/go-logr/logr"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
)

func clusterWithRack(rack asdbv1.Rack) *asdbv1.AerospikeCluster {
	return &asdbv1.AerospikeCluster{
		Spec: asdbv1.AerospikeClusterSpec{
			RackConfig: asdbv1.RackConfig{Racks: []asdbv1.Rack{rack}},
		},
	}
}

func TestValidateRackUpdatePlacementFields(t *testing.T) {
	base := asdbv1.Rack{ID: 1, RackLabel: "label", Zone: "z1", Region: "r1", NodeName: "n1"}

	tests := []struct {
		name    string
		mutate  func(r *asdbv1.Rack)
		oldRack asdbv1.Rack
		wantErr bool
	}{
		{"no change", func(r *asdbv1.Rack) {}, base, false},
		{"clear RackLabel", func(r *asdbv1.Rack) { r.RackLabel = "" }, base, false},
		{
			"set RackLabel from empty", func(r *asdbv1.Rack) { r.RackLabel = "label" },
			asdbv1.Rack{ID: 1, Zone: "z1", Region: "r1", NodeName: "n1"}, false,
		},
		{
			"set RackLabel from empty and change Zone",
			func(r *asdbv1.Rack) { r.RackLabel = "label"; r.Zone = "z2" },
			asdbv1.Rack{ID: 1, Zone: "z1", Region: "r1", NodeName: "n1"}, true,
		},
		{"change RackLabel", func(r *asdbv1.Rack) { r.RackLabel = "other" }, base, true},
		{"change NodeName", func(r *asdbv1.Rack) { r.NodeName = "n2" }, base, true},
		{"change Region", func(r *asdbv1.Rack) { r.Region = "r2" }, base, true},
		{"change Zone", func(r *asdbv1.Rack) { r.Zone = "z2" }, base, true},
		{
			"clear RackLabel and change Zone",
			func(r *asdbv1.Rack) { r.RackLabel = ""; r.Zone = "z2" }, base, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newRack := tt.oldRack
			tt.mutate(&newRack)

			err := validateRackUpdate(logr.Discard(), clusterWithRack(tt.oldRack), clusterWithRack(newRack))
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateRackUpdate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
