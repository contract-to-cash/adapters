package fincode

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/contract-to-cash/core/application/port"
)

// Gateway also implements port.CustomerGateway on top of the fincode
// customer and card APIs.
var _ port.CustomerGateway = (*Gateway)(nil)

// CreateCustomer creates a fincode customer (POST /v1/customers).
//
// InternalID mapping: unlike Stripe (which has no caller-chosen customer ID
// and instead stores InternalID in metadata, see stripe.MetadataInternalIDKey),
// fincode lets the caller choose the customer ID at creation time.
// CreateCustomerRequest.InternalID, when set, is therefore sent as the
// fincode customer id directly — the returned Customer.ID *is* the caller's
// InternalID, and no separate metadata lookup is needed to recover it. When
// InternalID is empty, fincode assigns an ID. fincode's exact ID
// character-set / length constraints are not confirmed from primary sources
// reachable during implementation (see the "Customer types" section of
// types.go for what was and wasn't confirmed); an invalid ID is rejected by
// fincode itself as an *HTTPError with a 4xx status, so no client-side
// validation beyond non-nil is imposed here.
//
// Metadata is not supported by fincode customers: a non-empty req.Metadata
// returns a *ValidationError rather than being silently dropped, so a caller
// relying on metadata finds out immediately instead of losing data invisibly.
// req.Description has no fincode equivalent either and is rejected the same
// way when non-empty; the resulting Customer.Metadata is always nil and
// Customer.Description is always "".
//
// Phone: fincode splits phone_cc (country code) / phone_no (national number)
// into two fields; this adapter keeps mapping simple and lossless by sending
// req.Phone as phone_no and leaving phone_cc unset, and reads phone_no back
// into Customer.Phone (see toCustomer). Splitting a caller's phone number
// into cc/no is not attempted since the port API gives no indication of the
// caller's intended split.
//
// Address: Line1/Line2/City/State/PostalCode/Country map to fincode's
// addr_line_1/addr_line_2/addr_city/addr_state/addr_post_code/addr_country
// respectively.
// addr_country is ISO 3166-1 *numeric* per fincode's customer schema (e.g.
// "392" for Japan); port.Address.Country is passed through unmodified, so
// callers must supply the numeric code fincode expects rather than an
// ISO alpha-2/3 code. fincode also has addr_line_3, a third address line;
// port.Address has no equivalent third line, so addr_line_3 is never
// populated by this adapter (a limitation of the port type, not of fincode).
func (g *Gateway) CreateCustomer(ctx context.Context, req *port.CreateCustomerRequest) (*port.Customer, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if len(req.Metadata) > 0 {
		return nil, &ValidationError{Field: "Metadata", Message: "fincode customers do not support metadata"}
	}
	if req.Description != "" {
		return nil, &ValidationError{Field: "Description", Message: "fincode customers do not support a description field"}
	}

	create := &CreateCustomerRequest{
		ID:      req.InternalID,
		Name:    req.Name,
		Email:   req.Email,
		PhoneNo: req.Phone,
	}
	applyAddressToCreate(create, req.Address)

	resp, err := g.client.CreateCustomer(ctx, create)
	if err != nil {
		return nil, g.wrapCustomerError("create customer", err)
	}
	return g.toCustomer(ctx, resp)
}

// UpdateCustomer updates a fincode customer (PUT /v1/customers/{id}). Only
// non-nil fields of the fincode request are sent, so fincode leaves the
// corresponding field on the customer untouched — mirroring the Stripe
// adapter's "only send what's set" semantics, adapted to fincode's
// pointer/omitempty request fields (see UpdateCustomerRequest in types.go)
// since fincode has no metadata-driven params builder to piggyback on.
// req.Email/Name/Phone map straight through as pointers (nil omitted,
// pointer-to-"" sent as an explicit clear per the fincode SDK's nullable
// field typing — not independently confirmed against a live fincode API call
// during implementation).
//
// req.Address, when non-nil, is applied non-destructively like Stripe: only
// its non-empty sub-fields are sent, so a partial Address{City: "Osaka"}
// leaves Line1/State/etc. untouched on fincode rather than clearing them.
// port.Address uses plain (non-pointer) strings, so — same limitation the
// Stripe adapter documents — there is no way to distinguish "leave this
// sub-field alone" from "clear it"; this adapter always chooses "leave
// alone" for an empty sub-field, meaning UpdateCustomer cannot be used to
// clear a single address sub-field while preserving the others.
//
// req.Metadata (non-empty) and a non-empty req.Description are rejected the
// same way as CreateCustomer.
func (g *Gateway) UpdateCustomer(ctx context.Context, req *port.UpdateCustomerRequest) (*port.Customer, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.CustomerID == "" {
		return nil, &ValidationError{Field: "CustomerID", Message: "must not be empty"}
	}
	if len(req.Metadata) > 0 {
		return nil, &ValidationError{Field: "Metadata", Message: "fincode customers do not support metadata"}
	}
	if req.Description != nil && *req.Description != "" {
		return nil, &ValidationError{Field: "Description", Message: "fincode customers do not support a description field"}
	}

	update := &UpdateCustomerRequest{
		Name:    req.Name,
		Email:   req.Email,
		PhoneNo: req.Phone,
	}
	applyAddressToUpdate(update, req.Address)

	resp, err := g.client.UpdateCustomer(ctx, req.CustomerID, update)
	if err != nil {
		return nil, g.wrapCustomerError("update customer", err)
	}
	return g.toCustomer(ctx, resp)
}

// GetCustomer retrieves a fincode customer by ID (GET /v1/customers/{id}).
// A missing customer surfaces as *port.GatewayError with
// ErrorCodeCustomerNotFound (see wrapCustomerError). This assumes fincode
// responds with HTTP 404 for a nonexistent OR previously-deleted customer
// ID; that could not be independently confirmed against a live fincode API
// call during implementation (the interactive docs site at
// https://docs.fincode.jp/api is a client-rendered SPA that could not be
// scraped), but it is consistent with fincode's officially published
// CustomerObject response schema, which carries no delete_flag / deleted
// marker that a Stripe-style "deleted stub" response would need — i.e.
// there is no documented shape for fincode to return a 200 for a deleted
// customer, unlike Stripe.
//
// DefaultPaymentMethodID: fincode's customer response carries only a boolean
// card_registration flag, not the default card itself, so when
// card_registration == "1" this makes a second call (ListCards) and returns
// the composite "<customer_id>/<card_id>" ID of the card with
// default_flag == "1" (nil if none is marked default). When
// card_registration is "0" or absent, no extra call is made. Errors from the
// extra ListCards call are propagated to the caller rather than silently
// leaving DefaultPaymentMethodID nil, since that would misreport a lookup
// failure as "no default payment method".
func (g *Gateway) GetCustomer(ctx context.Context, customerID string) (*port.Customer, error) {
	if customerID == "" {
		return nil, &ValidationError{Field: "customerID", Message: "must not be empty"}
	}
	resp, err := g.client.RetrieveCustomer(ctx, customerID)
	if err != nil {
		return nil, g.wrapCustomerError("get customer", err)
	}
	return g.toCustomer(ctx, resp)
}

// DeleteCustomer deletes a fincode customer (DELETE /v1/customers/{id}).
// Deleting a customer that does not exist, or was already deleted, surfaces
// as *port.GatewayError with ErrorCodeCustomerNotFound — same assumption and
// caveat as GetCustomer.
func (g *Gateway) DeleteCustomer(ctx context.Context, customerID string) error {
	if customerID == "" {
		return &ValidationError{Field: "customerID", Message: "must not be empty"}
	}
	if _, err := g.client.DeleteCustomer(ctx, customerID); err != nil {
		return g.wrapCustomerError("delete customer", err)
	}
	return nil
}

// SetDefaultPaymentMethod marks a stored card as the customer's default
// (PUT /v1/customers/{customer_id}/cards/{card_id}, default_flag=1).
// paymentMethodID is the composite "<customer_id>/<card_id>" returned by
// Gateway.RegisterPaymentMethod / ListPaymentMethods; the embedded
// customer_id must match customerID — a mismatch (e.g. a caller passing
// another customer's payment method ID) is rejected as a *ValidationError
// before any network call, rather than silently reassigning the card to a
// different customer or letting fincode reject it with a less specific
// error.
func (g *Gateway) SetDefaultPaymentMethod(ctx context.Context, customerID, paymentMethodID string) error {
	if customerID == "" {
		return &ValidationError{Field: "customerID", Message: "must not be empty"}
	}
	if paymentMethodID == "" {
		return &ValidationError{Field: "paymentMethodID", Message: "must not be empty"}
	}
	pmCustomerID, cardID, err := splitPaymentMethodID(paymentMethodID)
	if err != nil {
		return err
	}
	if pmCustomerID != customerID {
		return &ValidationError{
			Field:   "paymentMethodID",
			Message: fmt.Sprintf("payment method %q belongs to customer %q, not %q", paymentMethodID, pmCustomerID, customerID),
		}
	}
	if _, err := g.client.UpdateCard(ctx, customerID, cardID, &UpdateCardRequest{DefaultFlag: "1"}); err != nil {
		return g.wrapGatewayError("set default payment method", err)
	}
	return nil
}

// wrapCustomerError wraps a client-level error like wrapGatewayError,
// additionally mapping fincode's not-found signal on customer endpoints
// (HTTP 404) to ErrorCodeCustomerNotFound — mirroring the Stripe adapter's
// wrapCustomerError, which maps Stripe's resource_missing 404 the same way:
// within a customer operation, the missing resource is by definition the
// customer itself. Unlike the Stripe mapping, this relies on the HTTP status
// alone rather than a fincode-specific error_code: fincode does not appear to
// publish a distinct, groundable error_code for "no such customer" (see the
// caveat in GetCustomer's doc comment), but HTTP 404 for a missing resource
// is standard REST behavior and is already how this adapter treats a missing
// payment order elsewhere (see the 404 handling in resolveExistingOrder,
// gateway.go).
func (g *Gateway) wrapCustomerError(op string, err error) error {
	wrapped := g.wrapGatewayError(op, err)
	var ge *port.GatewayError
	var he *HTTPError
	if errors.As(wrapped, &ge) && errors.As(err, &he) && he.StatusCode == http.StatusNotFound {
		ge.Code = port.ErrorCodeCustomerNotFound
	}
	return wrapped
}

// applyAddressToCreate copies a port Address onto a fincode CreateCustomerRequest.
// CreateCustomerRequest's address fields are plain strings with `omitempty`,
// so an empty sub-field is simply omitted from the request — there is no
// "existing value to preserve" on create, unlike update.
func applyAddressToCreate(req *CreateCustomerRequest, a *port.Address) {
	if a == nil {
		return
	}
	req.AddrLine1 = a.Line1
	req.AddrLine2 = a.Line2
	req.AddrCity = a.City
	req.AddrState = a.State
	req.AddrPostCode = a.PostalCode
	req.AddrCountry = a.Country
}

// applyAddressToUpdate copies only the non-empty sub-fields of a port Address
// onto a fincode UpdateCustomerRequest, so an omitted sub-field is left nil
// (and therefore omitted from the request) rather than sent as an explicit
// empty string that would clear it on fincode. This mirrors the Stripe
// adapter's toStripeAddressParams non-destructive update semantics.
func applyAddressToUpdate(req *UpdateCustomerRequest, a *port.Address) {
	if a == nil {
		return
	}
	if a.Line1 != "" {
		req.AddrLine1 = &a.Line1
	}
	if a.Line2 != "" {
		req.AddrLine2 = &a.Line2
	}
	if a.City != "" {
		req.AddrCity = &a.City
	}
	if a.State != "" {
		req.AddrState = &a.State
	}
	if a.PostalCode != "" {
		req.AddrPostCode = &a.PostalCode
	}
	if a.Country != "" {
		req.AddrCountry = &a.Country
	}
}

// toCustomer maps a fincode CustomerResponse to the port representation,
// resolving DefaultPaymentMethodID via an extra ListCards call when the
// response indicates a card is registered (see GetCustomer's doc comment).
// Used by CreateCustomer, UpdateCustomer, and GetCustomer alike so all three
// report DefaultPaymentMethodID consistently — a freshly created customer
// can never have card_registration == "1" (cards are registered in a
// separate call after the customer exists), so the extra lookup is only ever
// actually made from UpdateCustomer/GetCustomer in practice.
func (g *Gateway) toCustomer(ctx context.Context, resp *CustomerResponse) (*port.Customer, error) {
	createdAt := g.timeOrNow(resp.Created)
	updatedAt := createdAt
	if resp.Updated != "" {
		updatedAt = g.timeOrNow(resp.Updated)
	}

	out := &port.Customer{
		ID:        resp.ID,
		Name:      resp.Name,
		Email:     resp.Email,
		Phone:     resp.PhoneNo,
		Address:   addressFromCustomerResponse(resp),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	if resp.CardRegistration == "1" {
		cards, err := g.client.ListCards(ctx, resp.ID)
		if err != nil {
			return nil, g.wrapGatewayError("list cards for customer default payment method", err)
		}
		for i := range cards.List {
			if cards.List[i].DefaultFlag == "1" {
				id := joinPaymentMethodID(resp.ID, cards.List[i].ID)
				out.DefaultPaymentMethodID = &id
				break
			}
		}
	}
	return out, nil
}

// addressFromCustomerResponse maps a fincode CustomerResponse's addr_* fields
// to a port Address, or nil when none of them are set (mirroring Stripe's
// fromStripeAddress, which returns nil for a nil Stripe address). addr_line_3
// is not mapped: port.Address has no third address line (see CreateCustomer's
// doc comment).
func addressFromCustomerResponse(resp *CustomerResponse) *port.Address {
	if resp.AddrLine1 == "" && resp.AddrLine2 == "" && resp.AddrCity == "" &&
		resp.AddrState == "" && resp.AddrPostCode == "" && resp.AddrCountry == "" {
		return nil
	}
	return &port.Address{
		Line1:      resp.AddrLine1,
		Line2:      resp.AddrLine2,
		City:       resp.AddrCity,
		State:      resp.AddrState,
		PostalCode: resp.AddrPostCode,
		Country:    resp.AddrCountry,
	}
}
