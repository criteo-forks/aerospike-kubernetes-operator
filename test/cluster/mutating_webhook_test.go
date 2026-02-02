/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
	webhookv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/internal/webhook/v1"
)

// createMinimalCluster creates a minimal valid AerospikeCluster for testing
func createMinimalCluster() *asdbv1.AerospikeCluster {
	return &asdbv1.AerospikeCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "test-ns",
		},
		Spec: asdbv1.AerospikeClusterSpec{
			Size:  2,
			Image: "aerospike/aerospike-server-enterprise:7.0.0.0",
			AerospikeConfig: &asdbv1.AerospikeConfigSpec{
				Value: map[string]interface{}{
					"service": map[string]interface{}{},
					"network": map[string]interface{}{
						"service": map[string]interface{}{},
						"fabric":  map[string]interface{}{},
						"heartbeat": map[string]interface{}{
							"mode": "mesh",
						},
					},
					"namespaces": []interface{}{
						map[string]interface{}{
							"name":               "test",
							"replication-factor": 2,
							"storage-engine": map[string]interface{}{
								"type": "memory",
							},
						},
					},
				},
			},
		},
	}
}

func TestMutatingWebhook_RackPodSpec_HostNetwork(t *testing.T) {
	tests := []struct {
		name                    string
		clusterHostNetwork      bool
		racks                   []asdbv1.Rack
		expectedHostNetworkByID map[int]bool
	}{
		{
			name:               "cluster hostNetwork=true, no rack override",
			clusterHostNetwork: true,
			racks: []asdbv1.Rack{
				{ID: 1},
				{ID: 2},
			},
			expectedHostNetworkByID: map[int]bool{1: true, 2: true},
		},
		{
			name:               "cluster hostNetwork=false, no rack override",
			clusterHostNetwork: false,
			racks: []asdbv1.Rack{
				{ID: 1},
				{ID: 2},
			},
			expectedHostNetworkByID: map[int]bool{1: false, 2: false},
		},
		{
			name:               "cluster hostNetwork=true, rack 1 overrides to false",
			clusterHostNetwork: true,
			racks: []asdbv1.Rack{
				{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
				{ID: 2},
			},
			expectedHostNetworkByID: map[int]bool{1: false, 2: true},
		},
		{
			name:               "cluster hostNetwork=false, rack 1 overrides to true",
			clusterHostNetwork: false,
			racks: []asdbv1.Rack{
				{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
				{ID: 2},
			},
			expectedHostNetworkByID: map[int]bool{1: true, 2: false},
		},
		{
			name:               "mixed rack overrides",
			clusterHostNetwork: true,
			racks: []asdbv1.Rack{
				{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(false)}},
				{ID: 2, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
				{ID: 3},
			},
			expectedHostNetworkByID: map[int]bool{1: false, 2: true, 3: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := createMinimalCluster()
			cluster.Spec.PodSpec.HostNetwork = tt.clusterHostNetwork
			cluster.Spec.RackConfig.Racks = tt.racks

			// Call the mutating webhook defaulter
			defaulter := &webhookv1.AerospikeClusterCustomDefaulter{}
			err := defaulter.Default(context.Background(), cluster)
			if err != nil {
				t.Fatalf("Default() failed: %v", err)
			}

			for _, rack := range cluster.Spec.RackConfig.Racks {
				expected := tt.expectedHostNetworkByID[rack.ID]
				actual := asdbv1.GetBool(rack.PodSpec.HostNetwork)
				if actual != expected {
					t.Errorf("rack %d: expected hostNetwork=%v, got %v", rack.ID, expected, actual)
				}
			}
		})
	}
}

func TestMutatingWebhook_RackPodSpec_DNSPolicy(t *testing.T) {
	tests := []struct {
		name                  string
		clusterHostNetwork    bool
		clusterInputDNSPolicy *corev1.DNSPolicy
		racks                 []asdbv1.Rack
		expectedDNSPolicyByID map[int]corev1.DNSPolicy
	}{
		{
			name:                  "no overrides, hostNetwork=false -> ClusterFirst",
			clusterHostNetwork:    false,
			clusterInputDNSPolicy: nil,
			racks: []asdbv1.Rack{
				{ID: 1},
			},
			expectedDNSPolicyByID: map[int]corev1.DNSPolicy{1: corev1.DNSClusterFirst},
		},
		{
			name:                  "no overrides, hostNetwork=true -> ClusterFirstWithHostNet",
			clusterHostNetwork:    true,
			clusterInputDNSPolicy: nil,
			racks: []asdbv1.Rack{
				{ID: 1},
			},
			expectedDNSPolicyByID: map[int]corev1.DNSPolicy{1: corev1.DNSClusterFirstWithHostNet},
		},
		{
			name:                  "cluster DNS policy override",
			clusterHostNetwork:    false,
			clusterInputDNSPolicy: ptr.To(corev1.DNSNone),
			racks: []asdbv1.Rack{
				{ID: 1},
			},
			expectedDNSPolicyByID: map[int]corev1.DNSPolicy{1: corev1.DNSNone},
		},
		{
			name:                  "rack DNS policy override takes precedence",
			clusterHostNetwork:    false,
			clusterInputDNSPolicy: ptr.To(corev1.DNSNone),
			racks: []asdbv1.Rack{
				{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{InputDNSPolicy: ptr.To(corev1.DNSDefault)}},
				{ID: 2},
			},
			expectedDNSPolicyByID: map[int]corev1.DNSPolicy{
				1: corev1.DNSDefault,
				2: corev1.DNSNone,
			},
		},
		{
			name:                  "rack hostNetwork override affects DNS policy",
			clusterHostNetwork:    false,
			clusterInputDNSPolicy: nil,
			racks: []asdbv1.Rack{
				{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{HostNetwork: ptr.To(true)}},
				{ID: 2},
			},
			expectedDNSPolicyByID: map[int]corev1.DNSPolicy{
				1: corev1.DNSClusterFirstWithHostNet,
				2: corev1.DNSClusterFirst,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := createMinimalCluster()
			cluster.Spec.PodSpec.HostNetwork = tt.clusterHostNetwork
			cluster.Spec.PodSpec.InputDNSPolicy = tt.clusterInputDNSPolicy
			cluster.Spec.RackConfig.Racks = tt.racks

			// Call the mutating webhook defaulter
			defaulter := &webhookv1.AerospikeClusterCustomDefaulter{}
			err := defaulter.Default(context.Background(), cluster)
			if err != nil {
				t.Fatalf("Default() failed: %v", err)
			}

			for _, rack := range cluster.Spec.RackConfig.Racks {
				expected := tt.expectedDNSPolicyByID[rack.ID]
				if rack.PodSpec.InputDNSPolicy == nil {
					t.Errorf("rack %d: expected DNSPolicy=%v, got nil", rack.ID, expected)
					continue
				}
				actual := *rack.PodSpec.InputDNSPolicy
				if actual != expected {
					t.Errorf("rack %d: expected DNSPolicy=%v, got %v", rack.ID, expected, actual)
				}
			}
		})
	}
}

func TestMutatingWebhook_RackPodSpec_DNSConfig(t *testing.T) {
	clusterDNSConfig := &corev1.PodDNSConfig{
		Nameservers: []string{"8.8.8.8"},
	}
	rackDNSConfig := &corev1.PodDNSConfig{
		Nameservers: []string{"1.1.1.1"},
	}

	tests := []struct {
		name                  string
		clusterDNSConfig      *corev1.PodDNSConfig
		racks                 []asdbv1.Rack
		expectedDNSConfigByID map[int]*corev1.PodDNSConfig
	}{
		{
			name:             "no cluster DNS config, no rack override",
			clusterDNSConfig: nil,
			racks: []asdbv1.Rack{
				{ID: 1},
			},
			expectedDNSConfigByID: map[int]*corev1.PodDNSConfig{1: nil},
		},
		{
			name:             "cluster DNS config, no rack override",
			clusterDNSConfig: clusterDNSConfig,
			racks: []asdbv1.Rack{
				{ID: 1},
			},
			expectedDNSConfigByID: map[int]*corev1.PodDNSConfig{1: clusterDNSConfig},
		},
		{
			name:             "rack DNS config override",
			clusterDNSConfig: clusterDNSConfig,
			racks: []asdbv1.Rack{
				{ID: 1, InputPodSpec: &asdbv1.RackPodSpec{DNSConfig: rackDNSConfig}},
				{ID: 2},
			},
			expectedDNSConfigByID: map[int]*corev1.PodDNSConfig{
				1: rackDNSConfig,
				2: clusterDNSConfig,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := createMinimalCluster()
			cluster.Spec.PodSpec.DNSConfig = tt.clusterDNSConfig
			cluster.Spec.RackConfig.Racks = tt.racks

			// Call the mutating webhook defaulter
			defaulter := &webhookv1.AerospikeClusterCustomDefaulter{}
			err := defaulter.Default(context.Background(), cluster)
			if err != nil {
				t.Fatalf("Default() failed: %v", err)
			}

			for _, rack := range cluster.Spec.RackConfig.Racks {
				expected := tt.expectedDNSConfigByID[rack.ID]
				actual := rack.PodSpec.DNSConfig

				if expected == nil && actual != nil {
					t.Errorf("rack %d: expected DNSConfig=nil, got %v", rack.ID, actual)
				} else if expected != nil && actual == nil {
					t.Errorf("rack %d: expected DNSConfig=%v, got nil", rack.ID, expected)
				} else if expected != nil && actual != nil {
					if len(expected.Nameservers) != len(actual.Nameservers) {
						t.Errorf("rack %d: expected DNSConfig nameservers=%v, got %v",
							rack.ID, expected.Nameservers, actual.Nameservers)
					}
				}
			}
		})
	}
}

func TestMutatingWebhook_RackID_Override(t *testing.T) {
	tests := []struct {
		name           string
		inputRackID    interface{}
		expectedRackID float64
	}{
		{
			name:           "rack-id already set with different value is preserved",
			inputRackID:    float64(99),
			expectedRackID: float64(99),
		},
		{
			name:           "rack-id already set with same value as rack ID is preserved",
			inputRackID:    float64(1),
			expectedRackID: float64(1),
		},
		{
			name:           "rack-id not set gets default from rack ID",
			inputRackID:    nil,
			expectedRackID: float64(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := createMinimalCluster()
			cluster.Spec.RackConfig.Namespaces = []string{"test"}

			// Build rack with optional InputAerospikeConfig containing rack-id override
			rack := asdbv1.Rack{ID: 1}
			if tt.inputRackID != nil {
				rack.InputAerospikeConfig = &asdbv1.AerospikeConfigSpec{
					Value: map[string]interface{}{
						"namespaces": []interface{}{
							map[string]interface{}{
								"name":    "test",
								"rack-id": tt.inputRackID,
							},
						},
					},
				}
			}
			cluster.Spec.RackConfig.Racks = []asdbv1.Rack{rack}

			// Call the mutating webhook defaulter
			defaulter := &webhookv1.AerospikeClusterCustomDefaulter{}
			err := defaulter.Default(context.Background(), cluster)
			if err != nil {
				t.Fatalf("Default() failed: %v", err)
			}

			// Check the rack-id in the rack's aerospike config
			resultRack := cluster.Spec.RackConfig.Racks[0]
			rackNamespaces := resultRack.AerospikeConfig.Value["namespaces"].([]interface{})
			rackNsMap := rackNamespaces[0].(map[string]interface{})
			actualRackID, ok := rackNsMap["rack-id"]
			if !ok {
				t.Fatalf("rack-id not found in namespace config")
			}
			if actualRackID != tt.expectedRackID {
				t.Errorf("expected rack-id=%v, got %v", tt.expectedRackID, actualRackID)
			}
		})
	}
}
