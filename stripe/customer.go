package stripe

import (
	"context"
	"errors"

	stripego "github.com/stripe/stripe-go/v82"

	"github.com/contract-to-cash/core/application/port"
)

// Gateway also implements port.CustomerGateway: integrators create a Stripe
// customer with CreateCustomer before charging, persist the returned
// Customer.ID ("cus_..."), and pass that ID as ChargeRequest.CustomerID /
// AuthorizeRequest.CustomerID. Passing an internal account ID (e.g. a core
// AccountID ULID) instead is rejected by Stripe with "No such customer".
var _ port.CustomerGateway = (*Gateway)(nil)

// MetadataInternalIDKey is the Stripe customer metadata key under which
// CreateCustomerRequest.InternalID (the integrator's own account ID) is
// stored, so the platform-side ID can be recovered from the Stripe customer
// (e.g. in webhook processing or the Stripe dashboard).
const MetadataInternalIDKey = "internal_id"

// CreateCustomer creates a Stripe customer. The returned Customer.ID is the
// Stripe customer ID ("cus_..."); persist it on your account record and use it
// as the CustomerID for charges and payment method registration.
// CreateCustomerRequest.InternalID, when set, is stored in the customer's
// metadata under MetadataInternalIDKey.
func (g *Gateway) CreateCustomer(ctx context.Context, req *port.CreateCustomerRequest) (*port.Customer, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}

	params := &stripego.CustomerParams{}
	if req.Email != "" {
		params.Email = stripego.String(req.Email)
	}
	if req.Name != "" {
		params.Name = stripego.String(req.Name)
	}
	if req.Description != "" {
		params.Description = stripego.String(req.Description)
	}
	if req.Phone != "" {
		params.Phone = stripego.String(req.Phone)
	}
	params.Address = toStripeAddressParams(req.Address)
	// CustomerParams carries its own Metadata field (the embedded
	// Params.Metadata is deprecated for customers and must not be mixed in),
	// so metadata goes through CustomerParams.AddMetadata here.
	for k, v := range req.Metadata {
		params.AddMetadata(k, v)
	}
	if req.InternalID != "" {
		params.AddMetadata(MetadataInternalIDKey, req.InternalID)
	}
	setContext(&params.Params, ctx)

	c, err := g.client.customers.New(params)
	if err != nil {
		return nil, g.wrapCustomerError("create customer", err)
	}
	return toCustomer(c), nil
}

// UpdateCustomer updates a Stripe customer. Only non-nil scalar fields are
// sent to Stripe; a pointer to an empty string clears that field on the
// customer. Metadata keys are merged into the existing metadata (Stripe
// semantics: an empty value unsets that key). A non-nil Address updates only
// its non-empty sub-fields — the port Address uses plain (non-pointer) strings,
// so an unset sub-field is indistinguishable from an intentional blank and is
// left untouched rather than clearing City/State/etc. on Stripe.
func (g *Gateway) UpdateCustomer(ctx context.Context, req *port.UpdateCustomerRequest) (*port.Customer, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.CustomerID == "" {
		return nil, &ValidationError{Field: "CustomerID", Message: "must not be empty"}
	}

	params := &stripego.CustomerParams{
		Email:       req.Email,
		Name:        req.Name,
		Description: req.Description,
		Phone:       req.Phone,
	}
	params.Address = toStripeAddressParams(req.Address)
	for k, v := range req.Metadata {
		params.AddMetadata(k, v)
	}
	setContext(&params.Params, ctx)

	c, err := g.client.customers.Update(req.CustomerID, params)
	if err != nil {
		return nil, g.wrapCustomerError("update customer", err)
	}
	return toCustomer(c), nil
}

// GetCustomer retrieves a Stripe customer by ID. A customer that does not
// exist — or has been deleted (Stripe returns a deleted stub rather than a
// 404) — surfaces as a *port.GatewayError with ErrorCodeCustomerNotFound.
func (g *Gateway) GetCustomer(ctx context.Context, customerID string) (*port.Customer, error) {
	if customerID == "" {
		return nil, &ValidationError{Field: "customerID", Message: "must not be empty"}
	}
	params := &stripego.CustomerParams{}
	setContext(&params.Params, ctx)

	c, err := g.client.customers.Get(customerID, params)
	if err != nil {
		return nil, g.wrapCustomerError("get customer", err)
	}
	if c.Deleted {
		return nil, &port.GatewayError{
			Code:    port.ErrorCodeCustomerNotFound,
			Message: "stripe: customer " + customerID + " is deleted",
		}
	}
	return toCustomer(c), nil
}

// DeleteCustomer permanently deletes a Stripe customer. Deleting a customer
// that does not exist returns a *port.GatewayError with
// ErrorCodeCustomerNotFound.
func (g *Gateway) DeleteCustomer(ctx context.Context, customerID string) error {
	if customerID == "" {
		return &ValidationError{Field: "customerID", Message: "must not be empty"}
	}
	params := &stripego.CustomerParams{}
	setContext(&params.Params, ctx)

	if _, err := g.client.customers.Del(customerID, params); err != nil {
		return g.wrapCustomerError("delete customer", err)
	}
	return nil
}

// wrapCustomerError wraps an SDK error like wrapGatewayError, additionally
// mapping Stripe's resource_missing (the "No such customer" 404) to
// ErrorCodeCustomerNotFound: within a customer operation the missing resource
// is by definition the customer itself.
func (g *Gateway) wrapCustomerError(op string, err error) error {
	wrapped := g.wrapGatewayError(op, err)
	var ge *port.GatewayError
	var se *stripego.Error
	if errors.As(wrapped, &ge) && errors.As(err, &se) && se.Code == stripego.ErrorCodeResourceMissing {
		ge.Code = port.ErrorCodeCustomerNotFound
	}
	return wrapped
}

// toCustomer maps a Stripe customer to the port representation. Stripe does
// not expose a last-updated timestamp on customers, so UpdatedAt mirrors
// CreatedAt (both derived from the Stripe "created" field).
func toCustomer(c *stripego.Customer) *port.Customer {
	out := &port.Customer{
		ID:          c.ID,
		Email:       c.Email,
		Name:        c.Name,
		Description: c.Description,
		Phone:       c.Phone,
		Address:     fromStripeAddress(c.Address),
		Metadata:    c.Metadata,
		CreatedAt:   unixTime(c.Created),
		UpdatedAt:   unixTime(c.Created),
	}
	if c.InvoiceSettings != nil && c.InvoiceSettings.DefaultPaymentMethod != nil {
		id := c.InvoiceSettings.DefaultPaymentMethod.ID
		out.DefaultPaymentMethodID = &id
	}
	return out
}

// toStripeAddressParams maps a port Address to Stripe params, emitting only
// non-empty sub-fields. Because the port Address sub-fields are plain strings
// (no pointers), sending them unconditionally would encode empties as
// address[state]= etc., which Stripe treats as "clear this field" — so a
// partial-address update would silently wipe the omitted sub-fields. Omitting
// empties keeps updates non-destructive; on create the omitted fields are
// simply unset.
func toStripeAddressParams(a *port.Address) *stripego.AddressParams {
	if a == nil {
		return nil
	}
	p := &stripego.AddressParams{}
	if a.Line1 != "" {
		p.Line1 = stripego.String(a.Line1)
	}
	if a.Line2 != "" {
		p.Line2 = stripego.String(a.Line2)
	}
	if a.City != "" {
		p.City = stripego.String(a.City)
	}
	if a.State != "" {
		p.State = stripego.String(a.State)
	}
	if a.PostalCode != "" {
		p.PostalCode = stripego.String(a.PostalCode)
	}
	if a.Country != "" {
		p.Country = stripego.String(a.Country)
	}
	return p
}

func fromStripeAddress(a *stripego.Address) *port.Address {
	if a == nil {
		return nil
	}
	return &port.Address{
		Line1:      a.Line1,
		Line2:      a.Line2,
		City:       a.City,
		State:      a.State,
		PostalCode: a.PostalCode,
		Country:    a.Country,
	}
}
