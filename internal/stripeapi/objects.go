package stripeapi

// The v1 object types, as stored in jobs.object_type, object_state, and the
// mirror schema. Untyped constants so they flow into plain-string fields
// without conversion.
const (
	ObjectCustomer         = "customer"
	ObjectSubscription     = "subscription"
	ObjectSubscriptionItem = "subscription_item"
	ObjectProduct          = "product"
	ObjectPrice            = "price"
	ObjectInvoice          = "invoice"
	ObjectCharge           = "charge"
	ObjectPaymentIntent    = "payment_intent"
	ObjectPaymentMethod    = "payment_method"
	ObjectSetupIntent      = "setup_intent"
	ObjectRefund           = "refund"
	ObjectDispute          = "dispute"
	ObjectCheckoutSession  = "checkout_session"
)

// AllObjectTypes is the canonical list; consistency tests assert every
// per-type map in the codebase covers exactly this set.
var AllObjectTypes = []string{
	ObjectCustomer,
	ObjectSubscription,
	ObjectSubscriptionItem,
	ObjectProduct,
	ObjectPrice,
	ObjectInvoice,
	ObjectCharge,
	ObjectPaymentIntent,
	ObjectPaymentMethod,
	ObjectSetupIntent,
	ObjectRefund,
	ObjectDispute,
	ObjectCheckoutSession,
}
