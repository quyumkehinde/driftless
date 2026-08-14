package apply

import (
	"testing"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// TestMirrorTablesCoverAllTypes keeps the upsert whitelist in lockstep with
// the canonical object type list.
func TestMirrorTablesCoverAllTypes(t *testing.T) {
	if len(mirrorTables) != len(stripeapi.AllObjectTypes) {
		t.Errorf("mirrorTables has %d entries, want %d", len(mirrorTables), len(stripeapi.AllObjectTypes))
	}
	for _, objectType := range stripeapi.AllObjectTypes {
		if _, ok := mirrorTables[objectType]; !ok {
			t.Errorf("mirrorTables missing %q", objectType)
		}
	}
}

// TestMappingTargetsAreCanonical asserts every object type the event
// mapping can produce is a canonical type the mirror can store.
func TestMappingTargetsAreCanonical(t *testing.T) {
	canonical := make(map[string]bool, len(stripeapi.AllObjectTypes))
	for _, objectType := range stripeapi.AllObjectTypes {
		canonical[objectType] = true
	}
	for eventType, objectType := range exactTypes {
		if !canonical[objectType] {
			t.Errorf("exactTypes[%q] = %q, not a canonical object type", eventType, objectType)
		}
	}
	for _, family := range prefixFamilies {
		if !canonical[family.objectType] {
			t.Errorf("prefix %q maps to %q, not a canonical object type", family.prefix, family.objectType)
		}
	}
}
