package v1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
)

func pvVolume(name string) asdbv1.VolumeSpec {
	return asdbv1.VolumeSpec{
		Name: name,
		Source: asdbv1.VolumeSource{
			PersistentVolume: &asdbv1.PersistentVolumeSpec{
				StorageClass: "ssd",
			},
		},
	}
}

func configMapVolume(name string) asdbv1.VolumeSpec {
	return asdbv1.VolumeSpec{
		Name: name,
		Source: asdbv1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: name},
			},
		},
	}
}

func storageWith(volumes ...asdbv1.VolumeSpec) *asdbv1.AerospikeStorageSpec {
	return &asdbv1.AerospikeStorageSpec{Volumes: volumes}
}

func volumeNames(volumes []asdbv1.VolumeSpec) []string {
	names := make([]string, 0, len(volumes))
	for idx := range volumes {
		names = append(names, volumes[idx].Name)
	}

	return names
}

func TestValidateAddedOrRemovedVolumes(t *testing.T) {
	testCases := []struct {
		oldStorage      *asdbv1.AerospikeStorageSpec
		newStorage      *asdbv1.AerospikeStorageSpec
		name            string
		expectedAdded   []string
		expectedRemoved []string
		expectErr       bool
	}{
		{
			name:          "adding a persistent volume is allowed",
			oldStorage:    storageWith(pvVolume("data")),
			newStorage:    storageWith(pvVolume("data"), pvVolume("data2")),
			expectErr:     false,
			expectedAdded: []string{"data2"},
		},
		{
			name:       "removing a persistent volume is blocked",
			oldStorage: storageWith(pvVolume("data"), pvVolume("data2")),
			newStorage: storageWith(pvVolume("data")),
			expectErr:  true,
		},
		{
			name:            "adding and removing a config map volume is allowed",
			oldStorage:      storageWith(pvVolume("data"), configMapVolume("cfg-old")),
			newStorage:      storageWith(pvVolume("data"), configMapVolume("cfg-new")),
			expectErr:       false,
			expectedAdded:   []string{"cfg-new"},
			expectedRemoved: []string{"cfg-old"},
		},
		{
			name:       "no change yields no added or removed volumes",
			oldStorage: storageWith(pvVolume("data")),
			newStorage: storageWith(pvVolume("data")),
			expectErr:  false,
		},
	}

	for idx := range testCases {
		tc := testCases[idx]
		t.Run(tc.name, func(t *testing.T) {
			added, removed, err := validateAddedOrRemovedVolumes(tc.oldStorage, tc.newStorage)

			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			assertSameNames(t, "added", tc.expectedAdded, volumeNames(added))
			assertSameNames(t, "removed", tc.expectedRemoved, volumeNames(removed))
		})
	}
}

func assertSameNames(t *testing.T, kind string, expected, got []string) {
	t.Helper()

	if len(expected) != len(got) {
		t.Fatalf("%s volumes: expected %v, got %v", kind, expected, got)
	}

	want := make(map[string]struct{}, len(expected))
	for _, name := range expected {
		want[name] = struct{}{}
	}

	for _, name := range got {
		if _, ok := want[name]; !ok {
			t.Fatalf("%s volumes: unexpected %q (expected %v, got %v)", kind, name, expected, got)
		}
	}
}
