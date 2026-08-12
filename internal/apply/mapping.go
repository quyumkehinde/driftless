// Package apply resolves events to the objects they concern and, in later
// stages, applies object state to the mirror schema.
package apply

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Target identifies the one primary object an event pokes. Related objects
// are never fanned out; they have their own events.
type Target struct {
	ObjectType string
	ObjectID   string
}

// exactTypes maps event types that are enumerated individually. These take
// precedence over prefix families: customer.tax_id.created must not resolve
// through a bare customer prefix, because its data.object is a tax id.
var exactTypes = map[string]string{
	"customer.created": "customer",
	"customer.updated": "customer",
	"customer.deleted": "customer",

	"product.created": "product",
	"product.updated": "product",
	"product.deleted": "product",

	"price.created": "price",
	"price.updated": "price",
	"price.deleted": "price",

	"charge.succeeded": "charge",
	"charge.failed":    "charge",
	"charge.refunded":  "charge",
	"charge.updated":   "charge",
	"charge.captured":  "charge",
	"charge.expired":   "charge",

	// legacy refund event family
	"charge.refund.updated": "refund",

	"payment_method.attached":              "payment_method",
	"payment_method.updated":               "payment_method",
	"payment_method.automatically_updated": "payment_method",
	"payment_method.detached":              "payment_method",

	"checkout.session.completed":               "checkout_session",
	"checkout.session.expired":                 "checkout_session",
	"checkout.session.async_payment_succeeded": "checkout_session",
	"checkout.session.async_payment_failed":    "checkout_session",

	"refund.created": "refund",
	"refund.updated": "refund",
	"refund.failed":  "refund",
}

// prefixFamilies covers the families the contract defines with a wildcard.
// Order matters: charge.dispute. must be consulted while bare charge events
// resolve through exactTypes only.
var prefixFamilies = []struct {
	prefix     string
	objectType string
}{
	{"customer.subscription.", "subscription"},
	{"invoice.", "invoice"},
	{"charge.dispute.", "dispute"},
	{"payment_intent.", "payment_intent"},
	{"setup_intent.", "setup_intent"},
}

// ResolveType maps an event type to the object type it pokes. ok is false
// for unknown types; callers must still store and count those, never
// silently discard them.
func ResolveType(eventType string) (objectType string, ok bool) {
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
