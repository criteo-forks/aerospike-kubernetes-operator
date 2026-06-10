package cluster

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
)

func stsWithVCTs(names ...string) *appsv1.StatefulSet {
	templates := make([]corev1.PersistentVolumeClaim, 0, len(names))
	for _, name := range names {
		templates = append(templates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		})
	}

	return &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			VolumeClaimTemplates: templates,
		},
	}
}

// rackStateWithPVs builds a RackState whose storage spec contains one PV-backed
// volume per provided name (plus, when set, a single non-PV volume to ensure it
// is ignored by the PV-name comparison).
func rackStateWithPVs(pvNames []string, nonPVNames ...string) *RackState {
	volumes := make([]asdbv1.VolumeSpec, 0, len(pvNames)+len(nonPVNames))

	for _, name := range pvNames {
		volumes = append(volumes, asdbv1.VolumeSpec{
			Name: name,
			Source: asdbv1.VolumeSource{
				PersistentVolume: &asdbv1.PersistentVolumeSpec{},
			},
		})
	}

	for _, name := range nonPVNames {
		volumes = append(volumes, asdbv1.VolumeSpec{
			Name: name,
			Source: asdbv1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	}

	return &RackState{
		Rack: &asdbv1.Rack{
			Storage: asdbv1.AerospikeStorageSpec{Volumes: volumes},
		},
	}
}

func TestVolumeClaimTemplatesChanged(t *testing.T) {
	testCases := []struct {
		found     *appsv1.StatefulSet
		rackState *RackState
		name      string
		expected  bool
	}{
		{
			name:      "adding a volume claim template is detected",
			found:     stsWithVCTs("data"),
			rackState: rackStateWithPVs([]string{"data", "data2"}),
			expected:  true,
		},
		{
			name:      "removing a volume claim template is detected",
			found:     stsWithVCTs("data", "data2"),
			rackState: rackStateWithPVs([]string{"data"}),
			expected:  true,
		},
		{
			name:      "no change yields false",
			found:     stsWithVCTs("data", "data2"),
			rackState: rackStateWithPVs([]string{"data", "data2"}),
			expected:  false,
		},
		{
			name:      "same names in different order yields false",
			found:     stsWithVCTs("data", "data2"),
			rackState: rackStateWithPVs([]string{"data2", "data"}),
			expected:  false,
		},
		{
			name:      "non-PV volumes in the spec are ignored",
			found:     stsWithVCTs("data"),
			rackState: rackStateWithPVs([]string{"data"}, "confdir"),
			expected:  false,
		},
		{
			name:      "no templates and no PV volumes yields false",
			found:     stsWithVCTs(),
			rackState: rackStateWithPVs(nil),
			expected:  false,
		},
	}

	for idx := range testCases {
		tc := testCases[idx]
		t.Run(tc.name, func(t *testing.T) {
			got := volumeClaimTemplatesChanged(tc.found, tc.rackState)
			if got != tc.expected {
				t.Fatalf("volumeClaimTemplatesChanged = %v, want %v", got, tc.expected)
			}
		})
	}
}
