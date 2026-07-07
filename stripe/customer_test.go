package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/contract-to-cash/core/application/port"
)

func customerJSON(id string, extra map[string]any) string {
	obj := map[string]any{
		"id":      id,
		"object":  "customer",
		"email":   "a@example.com",
		"name":    "Alice",
		"created": 1_700_000_000,
	}
	for k, v := range extra {
		obj[k] = v
	}
	b, _ := json.Marshal(obj)
	return string(b)
}

func TestGateway_CreateCustomer(t *testing.T) {
	f := newFakeStripe(t)
	f.on("POST /v1/customers", customerJSON("cus_1", map[string]any{
		"description": "tenant account",
		"phone":       "+81-3-0000-0000",
		"metadata":    map[string]string{"internal_id": "01KWXC4EC5G6S07MYSR2SY8QS7", "tier": "gold"},
		"address":     map[string]string{"line1": "1-2-3", "city": "Chiyoda", "country": "JP"},
	}))
	g := f.gateway()

	c, err := g.CreateCustomer(context.Background(), &port.CreateCustomerRequest{
		Email:       "a@example.com",
		Name:        "Alice",
		Description: "tenant account",
		Phone:       "+81-3-0000-0000",
		Address:     &port.Address{Line1: "1-2-3", City: "Chiyoda", Country: "JP"},
		Metadata:    map[string]string{"tier": "gold"},
		InternalID:  "01KWXC4EC5G6S07MYSR2SY8QS7",
	})
	if err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}
	if c.ID != "cus_1" {
		t.Errorf("ID = %q, want cus_1", c.ID)
	}
	if c.Email != "a@example.com" || c.Name != "Alice" {
		t.Errorf("Email/Name = %q/%q", c.Email, c.Name)
	}
	if c.Address == nil || c.Address.City != "Chiyoda" {
		t.Errorf("Address = %+v, want city Chiyoda", c.Address)
	}
	if c.Metadata["internal_id"] != "01KWXC4EC5G6S07MYSR2SY8QS7" {
		t.Errorf("Metadata = %v, want internal_id mapped back", c.Metadata)
	}
	if c.DefaultPaymentMethodID != nil {
		t.Errorf("DefaultPaymentMethodID = %v, want nil", *c.DefaultPaymentMethodID)
	}

	// Request form assertions: all fields forwarded, InternalID stored as
	// metadata under the documented key.
	form := f.lastForm
	for key, want := range map[string]string{
		"email":                 "a@example.com",
		"name":                  "Alice",
		"description":           "tenant account",
		"phone":                 "+81-3-0000-0000",
		"address[line1]":        "1-2-3",
		"address[city]":         "Chiyoda",
		"address[country]":      "JP",
		"metadata[tier]":        "gold",
		"metadata[internal_id]": "01KWXC4EC5G6S07MYSR2SY8QS7",
	} {
		if got := form.Get(key); got != want {
			t.Errorf("form %s = %q, want %q", key, got, want)
		}
	}
}

func TestGateway_CreateCustomer_MinimalOmitsEmptyFields(t *testing.T) {
	f := newFakeStripe(t)
	f.on("POST /v1/customers", customerJSON("cus_min", nil))
	g := f.gateway()

	if _, err := g.CreateCustomer(context.Background(), &port.CreateCustomerRequest{}); err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}
	for _, key := range []string{"email", "name", "description", "phone", "address[line1]"} {
		if _, ok := f.lastForm[key]; ok {
			t.Errorf("form should not contain %s, got %q", key, f.lastForm.Get(key))
		}
	}
}

func TestGateway_UpdateCustomer_OnlySetFieldsSent(t *testing.T) {
	f := newFakeStripe(t)
	f.on("POST /v1/customers/cus_1", customerJSON("cus_1", map[string]any{"name": "Bob"}))
	g := f.gateway()

	name := "Bob"
	c, err := g.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID: "cus_1",
		Name:       &name,
		Metadata:   map[string]string{"tier": "silver"},
	})
	if err != nil {
		t.Fatalf("UpdateCustomer: %v", err)
	}
	if c.Name != "Bob" {
		t.Errorf("Name = %q, want Bob", c.Name)
	}

	if got := f.lastForm.Get("name"); got != "Bob" {
		t.Errorf("form name = %q, want Bob", got)
	}
	if got := f.lastForm.Get("metadata[tier]"); got != "silver" {
		t.Errorf("form metadata[tier] = %q, want silver", got)
	}
	// Nil pointers must not be sent at all (sending "" would clear them).
	for _, key := range []string{"email", "description", "phone"} {
		if _, ok := f.lastForm[key]; ok {
			t.Errorf("form should not contain %s, got %q", key, f.lastForm.Get(key))
		}
	}
}

func TestGateway_UpdateCustomer_EmptyStringClearsField(t *testing.T) {
	f := newFakeStripe(t)
	f.on("POST /v1/customers/cus_1", customerJSON("cus_1", nil))
	g := f.gateway()

	empty := ""
	if _, err := g.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID: "cus_1",
		Email:      &empty,
	}); err != nil {
		t.Fatalf("UpdateCustomer: %v", err)
	}
	// A pointer to "" must be sent as an explicit empty value (email=) so
	// Stripe clears the field, not omitted. url.Values.Get can't tell absent
	// from present-empty, so assert on presence of the key itself.
	if _, ok := f.lastForm["email"]; !ok {
		t.Errorf("form should contain empty email= to clear the field, got keys %v", f.lastForm)
	}
}

func TestGateway_UpdateCustomer_PartialAddressPreservesOtherFields(t *testing.T) {
	f := newFakeStripe(t)
	f.on("POST /v1/customers/cus_1", customerJSON("cus_1", nil))
	g := f.gateway()

	if _, err := g.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID: "cus_1",
		Address:    &port.Address{Line1: "new line", City: "Osaka"},
	}); err != nil {
		t.Fatalf("UpdateCustomer: %v", err)
	}
	if got := f.lastForm.Get("address[line1]"); got != "new line" {
		t.Errorf("form address[line1] = %q, want %q", got, "new line")
	}
	if got := f.lastForm.Get("address[city]"); got != "Osaka" {
		t.Errorf("form address[city] = %q, want Osaka", got)
	}
	// Empty sub-fields must NOT be sent (sending address[state]= would clear
	// state on Stripe, wiping data the caller didn't intend to touch).
	for _, key := range []string{"address[line2]", "address[state]", "address[postal_code]", "address[country]"} {
		if _, ok := f.lastForm[key]; ok {
			t.Errorf("form should not contain empty %s, got %q", key, f.lastForm.Get(key))
		}
	}
}

func TestGateway_UpdateCustomer_RequiresID(t *testing.T) {
	f := newFakeStripe(t)
	g := f.gateway()
	_, err := g.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

func TestGateway_GetCustomer_DefaultPaymentMethodResolved(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/customers/cus_1", customerJSON("cus_1", map[string]any{
		"invoice_settings": map[string]any{
			"default_payment_method": map[string]any{"id": "pm_def", "object": "payment_method"},
		},
	}))
	g := f.gateway()

	c, err := g.GetCustomer(context.Background(), "cus_1")
	if err != nil {
		t.Fatalf("GetCustomer: %v", err)
	}
	if c.DefaultPaymentMethodID == nil || *c.DefaultPaymentMethodID != "pm_def" {
		t.Errorf("DefaultPaymentMethodID = %v, want pm_def", c.DefaultPaymentMethodID)
	}
	if c.CreatedAt.IsZero() || !c.CreatedAt.Equal(c.UpdatedAt) {
		t.Errorf("CreatedAt/UpdatedAt = %v/%v, want equal non-zero", c.CreatedAt, c.UpdatedAt)
	}
}

func TestGateway_GetCustomer_DeletedIsNotFound(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/customers/cus_gone", `{"id":"cus_gone","object":"customer","deleted":true}`)
	g := f.gateway()

	_, err := g.GetCustomer(context.Background(), "cus_gone")
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCustomerNotFound {
		t.Fatalf("err = %v, want GatewayError customer_not_found", err)
	}
}

func TestGateway_GetCustomer_MissingIsNotFound(t *testing.T) {
	f := newFakeStripe(t)
	f.onStatus("GET /v1/customers/cus_nope", 404,
		`{"error":{"type":"invalid_request_error","code":"resource_missing","param":"id","message":"No such customer: 'cus_nope'"}}`)
	g := f.gateway()

	_, err := g.GetCustomer(context.Background(), "cus_nope")
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCustomerNotFound {
		t.Fatalf("err = %v, want GatewayError customer_not_found", err)
	}
}

func TestGateway_DeleteCustomer(t *testing.T) {
	f := newFakeStripe(t)
	f.on("DELETE /v1/customers/cus_1", `{"id":"cus_1","object":"customer","deleted":true}`)
	g := f.gateway()

	if err := g.DeleteCustomer(context.Background(), "cus_1"); err != nil {
		t.Fatalf("DeleteCustomer: %v", err)
	}
	if f.lastPath != "/v1/customers/cus_1" {
		t.Errorf("path = %q, want /v1/customers/cus_1", f.lastPath)
	}
}

func TestGateway_DeleteCustomer_MissingIsNotFound(t *testing.T) {
	f := newFakeStripe(t)
	f.onStatus("DELETE /v1/customers/cus_nope", 404,
		`{"error":{"type":"invalid_request_error","code":"resource_missing","param":"id","message":"No such customer: 'cus_nope'"}}`)
	g := f.gateway()

	err := g.DeleteCustomer(context.Background(), "cus_nope")
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCustomerNotFound {
		t.Fatalf("err = %v, want GatewayError customer_not_found", err)
	}
}

func TestGateway_CustomerNilAndEmptyGuards(t *testing.T) {
	f := newFakeStripe(t)
	g := f.gateway()
	ctx := context.Background()

	if _, err := g.CreateCustomer(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("CreateCustomer(nil) = %v, want validation error", err)
	}
	if _, err := g.UpdateCustomer(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("UpdateCustomer(nil) = %v, want validation error", err)
	}
	if _, err := g.GetCustomer(ctx, ""); !errors.Is(err, ErrValidation) {
		t.Errorf("GetCustomer(\"\") = %v, want validation error", err)
	}
	if err := g.DeleteCustomer(ctx, ""); !errors.Is(err, ErrValidation) {
		t.Errorf("DeleteCustomer(\"\") = %v, want validation error", err)
	}
}
