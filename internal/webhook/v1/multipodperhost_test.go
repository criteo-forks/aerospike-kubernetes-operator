package v1

import (
	"testing"

	"k8s.io/utils/ptr"
)

func TestValidateMultiPodPerHostChange(t *testing.T) {
	tests := []struct {
		name     string
		oldValue *bool
		newValue *bool
		wantErr  bool
	}{
		{"nil to nil", nil, nil, false},
		{"nil to false", nil, ptr.To(false), false},
		{"false to nil", ptr.To(false), nil, false},
		{"false to false", ptr.To(false), ptr.To(false), false},
		{"true to true", ptr.To(true), ptr.To(true), false},
		{"false to true", ptr.To(false), ptr.To(true), false},
		{"nil to true", nil, ptr.To(true), false},
		{"true to false", ptr.To(true), ptr.To(false), true},
		{"true to nil", ptr.To(true), nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMultiPodPerHostChange(tt.oldValue, tt.newValue)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateMultiPodPerHostChange() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
