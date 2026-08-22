package apply

import (
	"fmt"
	"strings"
	"testing"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

func TestResolveType(t *testing.T) {
	tests := []struct {
		eventType  string
		objectType stripeapi.ObjectType
		ok         bool
	}{
		// customer: exact three only
		{"customer.created", "customer", true},
		{"customer.updated", "customer", true},
		{"customer.deleted", "customer", true},

		// customer.subscription.*
		{"customer.subscription.created", "subscription", true},
		{"customer.subscription.updated", "subscription", true},
		{"customer.subscription.deleted", "subscription", true},
		{"customer.subscription.paused", "subscription", true},
		{"customer.subscription.resumed", "subscription", true},
		{"customer.subscription.trial_will_end", "subscription", true},

		{"product.created", "product", true},
		{"product.updated", "product", true},
		{"product.deleted", "product", true},

		{"price.created", "price", true},
		{"price.updated", "price", true},
		{"price.deleted", "price", true},

		// invoice.*
		{"invoice.created", "invoice", true},
		{"invoice.updated", "invoice", true},
		{"invoice.finalized", "invoice", true},
		{"invoice.paid", "invoice", true},
		{"invoice.payment_failed", "invoice", true},
		{"invoice.voided", "invoice", true},
		{"invoice.marked_uncollectible", "invoice", true},
		{"invoice.deleted", "invoice", true},

		{"charge.succeeded", "charge", true},
		{"charge.failed", "charge", true},
		{"charge.refunded", "charge", true},
		{"charge.updated", "charge", true},
		{"charge.captured", "charge", true},
		{"charge.expired", "charge", true},

		// charge.dispute.*
		{"charge.dispute.created", "dispute", true},
		{"charge.dispute.updated", "dispute", true},
		{"charge.dispute.closed", "dispute", true},
		{"charge.dispute.funds_withdrawn", "dispute", true},
		{"charge.dispute.funds_reinstated", "dispute", true},

		// legacy refund family
		{"charge.refund.updated", "refund", true},

		// payment_intent.*
		{"payment_intent.created", "payment_intent", true},
		{"payment_intent.succeeded", "payment_intent", true},
		{"payment_intent.canceled", "payment_intent", true},
		{"payment_intent.payment_failed", "payment_intent", true},
		{"payment_intent.amount_capturable_updated", "payment_intent", true},
		{"payment_intent.requires_action", "payment_intent", true},

		{"payment_method.attached", "payment_method", true},
		{"payment_method.updated", "payment_method", true},
		{"payment_method.automatically_updated", "payment_method", true},
		{"payment_method.detached", "payment_method", true},

		// setup_intent.*
		{"setup_intent.created", "setup_intent", true},
		{"setup_intent.succeeded", "setup_intent", true},
		{"setup_intent.canceled", "setup_intent", true},
		{"setup_intent.setup_failed", "setup_intent", true},

		{"checkout.session.completed", "checkout_session", true},
		{"checkout.session.expired", "checkout_session", true},
		{"checkout.session.async_payment_succeeded", "checkout_session", true},
		{"checkout.session.async_payment_failed", "checkout_session", true},

		{"refund.created", "refund", true},
		{"refund.updated", "refund", true},
		{"refund.failed", "refund", true},

		// unknown: never map through a bare family prefix
		{"customer.tax_id.created", "", false},
		{"customer.discount.created", "", false},
		{"customer.source.updated", "", false},
		{"charge.captured.something", "", false},
		{"invoiceitem.created", "", false},
		{"plan.created", "", false},
		{"payout.paid", "", false},
		{"account.updated", "", false},
		{"balance.available", "", false},
		{"checkout.session.unknown_future_type", "", false},
		{"payment_method.card_automatically_updated_legacy", "", false},
		{"", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.eventType, func(t *testing.T) {
			objectType, ok := ResolveType(tc.eventType)
			if ok != tc.ok || objectType != tc.objectType {
				t.Errorf("ResolveType(%q) = (%q, %v), want (%q, %v)",
					tc.eventType, objectType, ok, tc.objectType, tc.ok)
			}
		})
	}
}

func TestResolveEvent(t *testing.T) {
	payload := func(id string) []byte {
		return fmt.Appendf(nil, `{"id":"evt_1","data":{"object":{"id":%q,"object":"customer"}}}`, id)
	}

	t.Run("mapped with id", func(t *testing.T) {
		target, ok, err := ResolveEvent("customer.updated", payload("cus_123"))
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		if target != (Target{ObjectType: "customer", ObjectID: "cus_123"}) {
			t.Errorf("target = %+v", target)
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		_, ok, err := ResolveEvent("plan.created", payload("plan_1"))
		if ok || err != nil {
			t.Errorf("ok=%v err=%v, want ok=false err=nil", ok, err)
		}
	})

	t.Run("mapped without object id", func(t *testing.T) {
		_, ok, err := ResolveEvent("customer.updated", []byte(`{"data":{"object":{}}}`))
		if !ok || err == nil {
			t.Errorf("ok=%v err=%v, want ok=true with error", ok, err)
		}
	})

	t.Run("malformed payload", func(t *testing.T) {
		_, ok, err := ResolveEvent("customer.updated", []byte(`{not json`))
		if !ok || err == nil {
			t.Errorf("ok=%v err=%v, want ok=true with error", ok, err)
		}
	})
}

// TestSweepEventTypesAreStripeCompatible guards the sweeper's events-list
// filter: GET /v1/events types[] rejects wildcards, so every pattern the
// sweeper sends must be an exact type name that ResolveType maps.
func TestSweepEventTypesAreStripeCompatible(t *testing.T) {
	for _, eventType := range SubscribedEventTypes() {
		if strings.Contains(eventType, "*") {
			t.Errorf("SubscribedEventTypes() = %q: wildcard patterns are rejected by GET /v1/events", eventType)
		}
		if _, ok := ResolveType(eventType); !ok {
			t.Errorf("SubscribedEventTypes() = %q: not resolvable to an object type", eventType)
		}
	}
}

// TestMappingTargetsAreCanonical asserts every object type the event
// mapping can produce is a canonical type the mirror can store.
func TestMappingTargetsAreCanonical(t *testing.T) {
	canonical := make(map[stripeapi.ObjectType]bool, len(stripeapi.AllObjectTypes))
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
