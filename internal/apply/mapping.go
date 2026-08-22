// Package apply turns claimed jobs into mirror writes: it resolves events
// to the objects they concern and applies fresh state under the per-object
// lock, in fetch or payload mode.
package apply

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// Target identifies the one primary object an event pokes. Related objects
// are never fanned out; they have their own events.
type Target struct {
	ObjectType stripeapi.ObjectType
	ObjectID   string
}

// exactTypes maps event types that are enumerated individually. These take
// precedence over prefix families: customer.tax_id.created must not resolve
// through a bare customer prefix, because its data.object is a tax id.
var exactTypes = map[string]stripeapi.ObjectType{
	"customer.created": stripeapi.ObjectCustomer,
	"customer.updated": stripeapi.ObjectCustomer,
	"customer.deleted": stripeapi.ObjectCustomer,

	"product.created": stripeapi.ObjectProduct,
	"product.updated": stripeapi.ObjectProduct,
	"product.deleted": stripeapi.ObjectProduct,

	"price.created": stripeapi.ObjectPrice,
	"price.updated": stripeapi.ObjectPrice,
	"price.deleted": stripeapi.ObjectPrice,

	"charge.succeeded": stripeapi.ObjectCharge,
	"charge.failed":    stripeapi.ObjectCharge,
	"charge.refunded":  stripeapi.ObjectCharge,
	"charge.updated":   stripeapi.ObjectCharge,
	"charge.captured":  stripeapi.ObjectCharge,
	"charge.expired":   stripeapi.ObjectCharge,

	// legacy refund event family
	"charge.refund.updated": stripeapi.ObjectRefund,

	"payment_method.attached":              stripeapi.ObjectPaymentMethod,
	"payment_method.updated":               stripeapi.ObjectPaymentMethod,
	"payment_method.automatically_updated": stripeapi.ObjectPaymentMethod,
	"payment_method.detached":              stripeapi.ObjectPaymentMethod,

	"checkout.session.completed":               stripeapi.ObjectCheckoutSession,
	"checkout.session.expired":                 stripeapi.ObjectCheckoutSession,
	"checkout.session.async_payment_succeeded": stripeapi.ObjectCheckoutSession,
	"checkout.session.async_payment_failed":    stripeapi.ObjectCheckoutSession,

	"refund.created": stripeapi.ObjectRefund,
	"refund.updated": stripeapi.ObjectRefund,
	"refund.failed":  stripeapi.ObjectRefund,
}

// prefixFamilies covers the event families the contract matches by prefix.
// Order matters: charge.dispute. must be consulted while bare charge events
// resolve through exactTypes only.
var prefixFamilies = []struct {
	prefix     string
	objectType stripeapi.ObjectType
	events     []string
}{
	{"customer.subscription.", stripeapi.ObjectSubscription, []string{
		"customer.subscription.created", "customer.subscription.deleted",
		"customer.subscription.paused", "customer.subscription.resumed",
		"customer.subscription.trial_will_end", "customer.subscription.updated",
	}},
	{"invoice.", stripeapi.ObjectInvoice, []string{
		"invoice.created", "invoice.deleted", "invoice.finalized",
		"invoice.marked_uncollectible", "invoice.paid",
		"invoice.payment_action_required", "invoice.payment_failed",
		"invoice.payment_succeeded", "invoice.sent", "invoice.updated",
		"invoice.voided",
	}},
	{"charge.dispute.", stripeapi.ObjectDispute, []string{
		"charge.dispute.closed", "charge.dispute.created",
		"charge.dispute.funds_reinstated", "charge.dispute.funds_withdrawn",
		"charge.dispute.updated",
	}},
	{"payment_intent.", stripeapi.ObjectPaymentIntent, []string{
		"payment_intent.amount_capturable_updated", "payment_intent.canceled",
		"payment_intent.created", "payment_intent.partially_funded",
		"payment_intent.payment_failed", "payment_intent.processing",
		"payment_intent.requires_action", "payment_intent.succeeded",
	}},
	{"setup_intent.", stripeapi.ObjectSetupIntent, []string{
		"setup_intent.canceled", "setup_intent.created",
		"setup_intent.requires_action", "setup_intent.setup_failed",
		"setup_intent.succeeded",
	}},
}

// SubscribedEventTypes returns the exact event types the contract covers,
// sorted, the shape GET /v1/events types[] accepts. Webhook endpoint
// registration may keep using wildcards; the sweeper must use this.
func SubscribedEventTypes() []string {
	patterns := make([]string, 0, len(exactTypes))
	for eventType := range exactTypes {
		patterns = append(patterns, eventType)
	}
	for _, family := range prefixFamilies {
		patterns = append(patterns, family.events...)
	}
	slices.Sort(patterns)
	return patterns
}

// ResolveType maps an event type to the object type it pokes. ok is false
// for unknown types; callers must still store and count those, never
// silently discard them.
func ResolveType(eventType string) (objectType stripeapi.ObjectType, ok bool) {
	if t, found := exactTypes[eventType]; found {
		return t, true
	}
	for _, f := range prefixFamilies {
		if strings.HasPrefix(eventType, f.prefix) {
			return f.objectType, true
		}
	}
	return "", false
}

// ResolveEvent resolves an event to its target object using the event type
// and the payload's data.object.id. ok is false for unmapped types. An
// error means the type is mapped but the payload has no object id, which is
// malformed input.
func ResolveEvent(eventType string, payload []byte) (target Target, ok bool, err error) {
	objectType, ok := ResolveType(eventType)
	if !ok {
		return Target{}, false, nil
	}
	var envelope struct {
		Data struct {
			Object struct {
				ID string `json:"id"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return Target{}, true, fmt.Errorf("parse event payload: %w", err)
	}
	if envelope.Data.Object.ID == "" {
		return Target{}, true, fmt.Errorf("event %s has no data.object.id", eventType)
	}
	return Target{ObjectType: objectType, ObjectID: envelope.Data.Object.ID}, true, nil
}
