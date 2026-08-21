package stripeapi

// ObjectType is one of the v1 object types, as stored in jobs.object_type,
// object_state, and the mirror schema.
type ObjectType string

// The v1 object types.
const (
	ObjectCustomer         ObjectType = "customer"
	ObjectSubscription     ObjectType = "subscription"
	ObjectSubscriptionItem ObjectType = "subscription_item"
	ObjectProduct          ObjectType = "product"
	ObjectPrice            ObjectType = "price"
	ObjectInvoice          ObjectType = "invoice"
	ObjectCharge           ObjectType = "charge"
	ObjectPaymentIntent    ObjectType = "payment_intent"
	ObjectPaymentMethod    ObjectType = "payment_method"
	ObjectSetupIntent      ObjectType = "setup_intent"
	ObjectRefund           ObjectType = "refund"
	ObjectDispute          ObjectType = "dispute"
	ObjectCheckoutSession  ObjectType = "checkout_session"
)

// MaxPageLimit is the largest page size the list APIs accept.
const MaxPageLimit = 100

// AllObjectTypes is the canonical list; consistency tests assert every
// per-type map in the codebase covers exactly this set.
var AllObjectTypes = []ObjectType{
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
