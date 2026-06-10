package cluster

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func stsWithVCTs(templates ...corev1.PersistentVolumeClaim) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			VolumeClaimTemplates: templates,
		},
	}
}

func vct(name, size string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
		},
	}
}

func TestVolumeClaimTemplatesChanged(t *testing.T) {
	testCases := []struct {
		found    *appsv1.StatefulSet
		desired  *appsv1.StatefulSet
		name     string
		expected bool
	}{
		{
			name:     "adding a volume claim template is detected",
			found:    stsWithVCTs(vct("data", "10Gi")),
			desired:  stsWithVCTs(vct("data", "10Gi"), vct("data2", "10Gi")),
			expected: true,
		},
		{
			name:     "removing a volume claim template is detected",
			found:    stsWithVCTs(vct("data", "10Gi"), vct("data2", "10Gi")),
			desired:  stsWithVCTs(vct("data", "10Gi")),
			expected: true,
		},
		{
			name:     "resizing an existing template (same name) is NOT detected by name-set comparison",
			found:    stsWithVCTs(vct("data", "10Gi")),
			desired:  stsWithVCTs(vct("data", "20Gi")),
			expected: false,
		},
		{
			name:     "no change yields false",
			found:    stsWithVCTs(vct("data", "10Gi"), vct("data2", "10Gi")),
			desired:  stsWithVCTs(vct("data", "10Gi"), vct("data2", "10Gi")),
			expected: false,
		},
		{
			name:     "same names in different order yields false",
			found:    stsWithVCTs(vct("data", "10Gi"), vct("data2", "10Gi")),
			desired:  stsWithVCTs(vct("data2", "10Gi"), vct("data", "10Gi")),
			expected: false,
		},
		{
			name:     "no templates on either side yields false",
			found:    stsWithVCTs(),
			desired:  stsWithVCTs(),
			expected: false,
		},
	}

	for idx := range testCases {
		tc := testCases[idx]
		t.Run(tc.name, func(t *testing.T) {
			got := volumeClaimTemplatesChanged(tc.found, tc.desired)
			if got != tc.expected {
				t.Fatalf("volumeClaimTemplatesChanged = %v, want %v", got, tc.expected)
			}
		})
	}
}
