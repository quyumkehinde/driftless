package fakestripe

import (
	"testing"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// TestPluralPathsCoverAllTypes keeps the double's routing in lockstep with
// the canonical object type list. Checkout sessions route explicitly, so
// the plural map carries the other twelve.
func TestPluralPathsCoverAllTypes(t *testing.T) {
	covered := map[string]bool{stripeapi.ObjectCheckoutSession: true}
	for _, objectType := range pluralPaths {
		covered[objectType] = true
	}
	if len(covered) != len(stripeapi.AllObjectTypes) {
		t.Errorf("fakestripe routes %d object types, want %d", len(covered), len(stripeapi.AllObjectTypes))
	}
	for _, objectType := range stripeapi.AllObjectTypes {
		if !covered[objectType] {
			t.Errorf("fakestripe has no route for %q", objectType)
		}
	}
}
