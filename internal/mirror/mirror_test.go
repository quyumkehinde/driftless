package mirror

import (
	"testing"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// TestTablesCoverAllTypes keeps the write whitelist in lockstep with the
// canonical object type list.
func TestTablesCoverAllTypes(t *testing.T) {
	if len(tables) != len(stripeapi.AllObjectTypes) {
		t.Errorf("tables has %d entries, want %d", len(tables), len(stripeapi.AllObjectTypes))
	}
	for _, objectType := range stripeapi.AllObjectTypes {
		if _, ok := tables[objectType]; !ok {
			t.Errorf("tables missing %q", objectType)
		}
	}
}
