package finalizer

import (
	"testing"

	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHasOtherSystemFinalizers(t *testing.T) {
	for _, tt := range []struct {
		name       string
		finalizers []string
		want       bool
	}{
		{"empty", nil, false},
		{"own", []string{polardbxmeta.Finalizer}, false},
		{"foreground", []string{polardbxmeta.Finalizer, metav1.FinalizerDeleteDependents}, false},
		{"foreground first", []string{metav1.FinalizerDeleteDependents, polardbxmeta.Finalizer}, false},
		{"foreground only", []string{metav1.FinalizerDeleteDependents}, false},
		{"external", []string{polardbxmeta.Finalizer, "example.com/cleanup"}, true},
		{"external and foreground", []string{polardbxmeta.Finalizer, metav1.FinalizerDeleteDependents, "example.com/cleanup"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasOtherSystemFinalizers(tt.finalizers); got != tt.want {
				t.Fatalf("hasOtherSystemFinalizers(%v) = %v, want %v", tt.finalizers, got, tt.want)
			}
		})
	}
}
