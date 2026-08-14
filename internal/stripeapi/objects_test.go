package stripeapi

import (
	"slices"
	"testing"
)

// TestObjectPathsCoverAllTypes pins objectPaths to the canonical list: a
// type added to one without the other fails here instead of at runtime.
func TestObjectPathsCoverAllTypes(t *testing.T) {
	if len(objectPaths) != len(AllObjectTypes) {
		t.Errorf("objectPaths has %d entries, AllObjectTypes %d", len(objectPaths), len(AllObjectTypes))
	}
	for _, objectType := range AllObjectTypes {
		if _, ok := objectPaths[objectType]; !ok {
			t.Errorf("objectPaths missing %q", objectType)
		}
	}
}

// TestAllObjectTypesAreDistinct guards against a copy-paste duplicate in
// the constant list itself.
func TestAllObjectTypesAreDistinct(t *testing.T) {
	sorted := slices.Clone(AllObjectTypes)
	slices.Sort(sorted)
	if len(slices.Compact(sorted)) != len(AllObjectTypes) {
		t.Errorf("AllObjectTypes contains duplicates: %v", AllObjectTypes)
	}
	if len(AllObjectTypes) != 13 {
		t.Errorf("AllObjectTypes has %d entries, the v1 contract is 13", len(AllObjectTypes))
	}
}
