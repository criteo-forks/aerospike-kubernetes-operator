package cluster

import (
	"testing"

	asdbv1 "github.com/aerospike/aerospike-kubernetes-operator/v4/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// statusPods builds a status.pods map from the given pod names.
func statusPods(names ...string) map[string]asdbv1.AerospikePodStatus {
	pods := make(map[string]asdbv1.AerospikePodStatus, len(names))
	for _, name := range names {
		pods[name] = asdbv1.AerospikePodStatus{}
	}

	return pods
}

// TestReplaceRecoverySize covers the data-safety-critical sizing used to recreate
// a rack whose StatefulSet is missing: a new rack must not "recover", and an
// existing rack must be sized to re-adopt every orphaned pod (never smaller than
// the highest live ordinal, so no orphan is treated as dangling).
func TestReplaceRecoverySize(t *testing.T) {
	const rackID = 11

	testCases := []struct {
		name         string
		statusPods   map[string]asdbv1.AerospikePodStatus
		desiredSize  int32
		wantSize     int32
		wantExisting bool
		wantErr      bool
	}{
		{
			name:         "no status pods is a brand-new rack",
			statusPods:   statusPods(),
			desiredSize:  3,
			wantSize:     3,
			wantExisting: false,
		},
		{
			name:         "pods only on other racks are ignored (new rack)",
			statusPods:   statusPods("aerospikes99-p02-10-0", "aerospikes99-p02-10-1"),
			desiredSize:  2,
			wantSize:     2,
			wantExisting: false,
		},
		{
			name:         "contiguous pods size to desired",
			statusPods:   statusPods("aerospikes99-p02-11-0", "aerospikes99-p02-11-1", "aerospikes99-p02-11-2"),
			desiredSize:  3,
			wantSize:     3,
			wantExisting: true,
		},
		{
			name:         "highest ordinal beyond desired grows the size to re-adopt all orphans",
			statusPods:   statusPods("aerospikes99-p02-11-0", "aerospikes99-p02-11-4"),
			desiredSize:  2,
			wantSize:     5,
			wantExisting: true,
		},
		{
			name:         "desired larger than ordinals is kept",
			statusPods:   statusPods("aerospikes99-p02-11-0"),
			desiredSize:  4,
			wantSize:     4,
			wantExisting: true,
		},
		{
			name: "only the target rack's pods affect the size",
			statusPods: statusPods(
				"aerospikes99-p02-11-0", "aerospikes99-p02-11-1", "aerospikes99-p02-12-9",
			),
			desiredSize:  1,
			wantSize:     2,
			wantExisting: true,
		},
		{
			name:        "unparseable pod name errors",
			statusPods:  statusPods("badname"),
			desiredSize: 1,
			wantErr:     true,
		},
	}

	for idx := range testCases {
		tc := testCases[idx]
		t.Run(tc.name, func(t *testing.T) {
			r := &SingleClusterReconciler{
				aeroCluster: &asdbv1.AerospikeCluster{},
			}
			r.aeroCluster.Status.Pods = tc.statusPods

			size, existing, err := r.replaceRecoverySize(rackID, tc.desiredSize)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("replaceRecoverySize() expected an error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("replaceRecoverySize() unexpected error: %v", err)
			}

			if existing != tc.wantExisting {
				t.Errorf("hasExistingPods = %v, want %v", existing, tc.wantExisting)
			}

			if size != tc.wantSize {
				t.Errorf("size = %d, want %d", size, tc.wantSize)
			}
		})
	}
}

func TestReorderPodsForRollingRestart(t *testing.T) {
	pods := []*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "aerospikes99-p02-11-2"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "aerospikes99-p02-11-1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "aerospikes99-p02-11-0"}},
	}

	reordered := reorderPodsForRollingRestart(pods)

	got := getPodNames(reordered)
	want := []string{"aerospikes99-p02-11-0", "aerospikes99-p02-11-1", "aerospikes99-p02-11-2"}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("reordered pods = %v, want %v", got, want)
		}
	}

	original := getPodNames(pods)
	wantOriginal := []string{"aerospikes99-p02-11-2", "aerospikes99-p02-11-1", "aerospikes99-p02-11-0"}
	for idx := range wantOriginal {
		if original[idx] != wantOriginal[idx] {
			t.Fatalf("original pods mutated = %v, want %v", original, wantOriginal)
		}
	}
}
