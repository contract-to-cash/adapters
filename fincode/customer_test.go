package fincode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/contract-to-cash/core/application/port"
)

// routeResponse is a canned response for one "METHOD path" route in
// fakeCustomerAPI.
type routeResponse struct {
	status int
	body   any
}

// fakeCustomerAPI is a minimal fincode-like server for the customer/card
// endpoints, keyed by "METHOD path" like the payment fakeFincode server in
// gateway_test.go but built around an explicit route table instead of a
// switch, since customer tests need to compose different combinations of
// routes (e.g. GetCustomer sometimes chains into ListCards, sometimes not).
// A request to an unregistered route returns 404 with a fincode-shaped
// ErrorResponse, which doubles as this fake's "not found" simulation.
type fakeCustomerAPI struct {
	t        *testing.T
	routes   map[string]routeResponse
	lastBody map[string][]byte
	calls    map[string]int
}

func newFakeCustomerAPI(t *testing.T) *fakeCustomerAPI {
	t.Helper()
	return &fakeCustomerAPI{
		t:        t,
		routes:   map[string]routeResponse{},
		lastBody: map[string][]byte{},
		calls:    map[string]int{},
	}
}

func (f *fakeCustomerAPI) on(methodPath string, status int, body any) {
	f.routes[methodPath] = routeResponse{status: status, body: body}
}

func (f *fakeCustomerAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		f.calls[key]++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			f.t.Fatalf("read request body: %v", err)
		}
		f.lastBody[key] = body

		resp, ok := f.routes[key]
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E9999", ErrorMessage: "not found: " + key}},
			})
			return
		}
		status := resp.status
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		if resp.body != nil {
			_ = json.NewEncoder(w).Encode(resp.body)
		}
	})
}

// setupCustomerGateway starts a fakeCustomerAPI server and wires up a Gateway
// against it. The caller is responsible for calling the returned close func.
func setupCustomerGateway(t *testing.T) (*Gateway, *fakeCustomerAPI, func()) {
	t.Helper()
	fake := newFakeCustomerAPI(t)
	srv := httptest.NewServer(fake.handler())
	client := mustNewClient(Config{APIKey: "sk_test_cust", BaseURL: srv.URL})
	gw := NewGateway(client)
	return gw, fake, srv.Close
}

// decodeJSONMap decodes raw request/response bytes into a generic map so
// tests can assert on the presence/absence of individual JSON keys — the
// thing that actually matters for "only non-nil fields are sent" semantics,
// which a typed decode (defaulting absent fields to their zero value) cannot
// distinguish from "sent as empty".
func decodeJSONMap(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if len(b) == 0 {
		return m
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode JSON body: %v (body=%s)", err, b)
	}
	return m
}

// --- CreateCustomer ---

func TestGateway_CreateCustomer_WithInternalID(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("POST /v1/customers", http.StatusOK, CustomerResponse{
		ID: "acct_internal_1", Name: "Alice", Email: "a@example.com",
		PhoneNo: "09012345678", AddrLine1: "1-2-3", AddrCity: "Chiyoda", AddrCountry: "392",
		Created: "2026/06/01 12:00:00.000",
	})

	c, err := gw.CreateCustomer(context.Background(), &port.CreateCustomerRequest{
		InternalID: "acct_internal_1",
		Name:       "Alice",
		Email:      "a@example.com",
		Phone:      "09012345678",
		Address:    &port.Address{Line1: "1-2-3", City: "Chiyoda", Country: "392"},
	})
	if err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}
	if c.ID != "acct_internal_1" {
		t.Errorf("ID = %q, want acct_internal_1 (InternalID mapped to fincode customer id)", c.ID)
	}
	if c.Phone != "09012345678" {
		t.Errorf("Phone = %q, want 09012345678", c.Phone)
	}
	if c.Address == nil || c.Address.City != "Chiyoda" {
		t.Errorf("Address = %+v, want city Chiyoda", c.Address)
	}

	body := decodeJSONMap(t, fake.lastBody["POST /v1/customers"])
	if body["id"] != "acct_internal_1" {
		t.Errorf("request id = %v, want acct_internal_1", body["id"])
	}
	if body["phone_no"] != "09012345678" {
		t.Errorf("request phone_no = %v, want 09012345678", body["phone_no"])
	}
	if _, ok := body["phone_cc"]; ok {
		t.Errorf("request should not contain phone_cc, got %v", body["phone_cc"])
	}
}

func TestGateway_CreateCustomer_WithoutInternalID_OmitsID(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("POST /v1/customers", http.StatusOK, CustomerResponse{
		ID: "cust_assigned_by_fincode", Created: "2026/06/01 12:00:00.000",
	})

	c, err := gw.CreateCustomer(context.Background(), &port.CreateCustomerRequest{Name: "Bob"})
	if err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}
	if c.ID != "cust_assigned_by_fincode" {
		t.Errorf("ID = %q, want cust_assigned_by_fincode", c.ID)
	}

	body := decodeJSONMap(t, fake.lastBody["POST /v1/customers"])
	if _, ok := body["id"]; ok {
		t.Errorf("request should not contain id when InternalID is empty, got %v", body["id"])
	}
}

func TestGateway_CreateCustomer_RejectsMetadata(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	_, err := gw.CreateCustomer(context.Background(), &port.CreateCustomerRequest{
		Metadata: map[string]string{"tier": "gold"},
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

func TestGateway_CreateCustomer_RejectsDescription(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	_, err := gw.CreateCustomer(context.Background(), &port.CreateCustomerRequest{
		Description: "tenant account",
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

// --- UpdateCustomer ---

func TestGateway_UpdateCustomer_OnlyNonNilFieldsSent(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("PUT /v1/customers/cust_1", http.StatusOK, CustomerResponse{
		ID: "cust_1", Name: "Bob", Created: "2026/06/01 12:00:00.000",
	})

	name := "Bob"
	c, err := gw.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID: "cust_1",
		Name:       &name,
	})
	if err != nil {
		t.Fatalf("UpdateCustomer: %v", err)
	}
	if c.Name != "Bob" {
		t.Errorf("Name = %q, want Bob", c.Name)
	}

	body := decodeJSONMap(t, fake.lastBody["PUT /v1/customers/cust_1"])
	if body["name"] != "Bob" {
		t.Errorf("request name = %v, want Bob", body["name"])
	}
	for _, key := range []string{"email", "phone_no", "phone_cc", "addr_line_1", "addr_city", "addr_country", "addr_state", "addr_post_code"} {
		if _, ok := body[key]; ok {
			t.Errorf("request should not contain %s, got %v", key, body[key])
		}
	}
}

func TestGateway_UpdateCustomer_PartialAddressPreservesOtherFields(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("PUT /v1/customers/cust_1", http.StatusOK, CustomerResponse{ID: "cust_1", Created: "2026/06/01 12:00:00.000"})

	_, err := gw.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID: "cust_1",
		Address:    &port.Address{Line1: "new line", City: "Osaka"},
	})
	if err != nil {
		t.Fatalf("UpdateCustomer: %v", err)
	}

	body := decodeJSONMap(t, fake.lastBody["PUT /v1/customers/cust_1"])
	if body["addr_line_1"] != "new line" {
		t.Errorf("request addr_line_1 = %v, want %q", body["addr_line_1"], "new line")
	}
	if body["addr_city"] != "Osaka" {
		t.Errorf("request addr_city = %v, want Osaka", body["addr_city"])
	}
	for _, key := range []string{"addr_line_2", "addr_state", "addr_post_code", "addr_country"} {
		if _, ok := body[key]; ok {
			t.Errorf("request should not contain empty %s, got %v", key, body[key])
		}
	}
}

func TestGateway_UpdateCustomer_RequiresCustomerID(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	_, err := gw.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

func TestGateway_UpdateCustomer_RejectsMetadata(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	_, err := gw.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID: "cust_1",
		Metadata:   map[string]string{"tier": "gold"},
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

func TestGateway_UpdateCustomer_RejectsNonEmptyDescription(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	desc := "tenant account"
	_, err := gw.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID:  "cust_1",
		Description: &desc,
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

func TestGateway_UpdateCustomer_EmptyDescriptionIsNoop(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("PUT /v1/customers/cust_1", http.StatusOK, CustomerResponse{ID: "cust_1", Created: "2026/06/01 12:00:00.000"})

	empty := ""
	if _, err := gw.UpdateCustomer(context.Background(), &port.UpdateCustomerRequest{
		CustomerID:  "cust_1",
		Description: &empty,
	}); err != nil {
		t.Fatalf("UpdateCustomer: %v", err)
	}
}

// --- GetCustomer ---

func TestGateway_GetCustomer_NotFound(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()
	// No route registered for GET /v1/customers/cust_nope -> the fake returns
	// 404, simulating fincode's not-found response.

	_, err := gw.GetCustomer(context.Background(), "cust_nope")
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCustomerNotFound {
		t.Fatalf("err = %v, want GatewayError customer_not_found", err)
	}
}

func TestGateway_GetCustomer_DefaultPaymentMethodResolvedViaListCards(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("GET /v1/customers/cust_1", http.StatusOK, CustomerResponse{
		ID: "cust_1", CardRegistration: "1", Created: "2026/06/01 12:00:00.000",
	})
	fake.on("GET /v1/customers/cust_1/cards", http.StatusOK, CardListResponse{List: []CardResponse{
		{CustomerID: "cust_1", ID: "card_1", DefaultFlag: "0"},
		{CustomerID: "cust_1", ID: "card_2", DefaultFlag: "1"},
	}})

	c, err := gw.GetCustomer(context.Background(), "cust_1")
	if err != nil {
		t.Fatalf("GetCustomer: %v", err)
	}
	if c.DefaultPaymentMethodID == nil || *c.DefaultPaymentMethodID != "cust_1/card_2" {
		t.Errorf("DefaultPaymentMethodID = %v, want cust_1/card_2", c.DefaultPaymentMethodID)
	}
	if fake.calls["GET /v1/customers/cust_1/cards"] != 1 {
		t.Errorf("ListCards call count = %d, want 1", fake.calls["GET /v1/customers/cust_1/cards"])
	}
}

func TestGateway_GetCustomer_NoCardRegistration_SkipsListCards(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("GET /v1/customers/cust_1", http.StatusOK, CustomerResponse{
		ID: "cust_1", CardRegistration: "0", Created: "2026/06/01 12:00:00.000",
	})
	// Deliberately no route for GET /v1/customers/cust_1/cards: if the
	// gateway called it anyway, GetCustomer would fail with a 404-derived
	// error, so a passing test also proves the call was skipped.

	c, err := gw.GetCustomer(context.Background(), "cust_1")
	if err != nil {
		t.Fatalf("GetCustomer: %v", err)
	}
	if c.DefaultPaymentMethodID != nil {
		t.Errorf("DefaultPaymentMethodID = %v, want nil", *c.DefaultPaymentMethodID)
	}
	if n := fake.calls["GET /v1/customers/cust_1/cards"]; n != 0 {
		t.Errorf("ListCards call count = %d, want 0", n)
	}
}

func TestGateway_GetCustomer_TimestampParsing(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("GET /v1/customers/cust_1", http.StatusOK, CustomerResponse{
		ID: "cust_1", Created: "2026/06/01 12:00:00.000", Updated: "2026/06/02 08:30:00.000",
	})

	c, err := gw.GetCustomer(context.Background(), "cust_1")
	if err != nil {
		t.Fatalf("GetCustomer: %v", err)
	}
	if c.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
	if c.UpdatedAt.IsZero() || c.UpdatedAt.Equal(c.CreatedAt) {
		t.Errorf("UpdatedAt = %v, want distinct from CreatedAt = %v", c.UpdatedAt, c.CreatedAt)
	}
}

func TestGateway_GetCustomer_NoUpdatedMirrorsCreated(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("GET /v1/customers/cust_1", http.StatusOK, CustomerResponse{
		ID: "cust_1", Created: "2026/06/01 12:00:00.000",
	})

	c, err := gw.GetCustomer(context.Background(), "cust_1")
	if err != nil {
		t.Fatalf("GetCustomer: %v", err)
	}
	if !c.UpdatedAt.Equal(c.CreatedAt) {
		t.Errorf("UpdatedAt = %v, want equal to CreatedAt = %v", c.UpdatedAt, c.CreatedAt)
	}
}

// --- DeleteCustomer ---

func TestGateway_DeleteCustomer(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("DELETE /v1/customers/cust_1", http.StatusOK, DeleteCustomerResponse{ID: "cust_1", DeleteFlag: "1"})

	if err := gw.DeleteCustomer(context.Background(), "cust_1"); err != nil {
		t.Fatalf("DeleteCustomer: %v", err)
	}
	if fake.calls["DELETE /v1/customers/cust_1"] != 1 {
		t.Errorf("call count = %d, want 1", fake.calls["DELETE /v1/customers/cust_1"])
	}
}

func TestGateway_DeleteCustomer_NotFound(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	err := gw.DeleteCustomer(context.Background(), "cust_nope")
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCustomerNotFound {
		t.Fatalf("err = %v, want GatewayError customer_not_found", err)
	}
}

// --- SetDefaultPaymentMethod ---

func TestGateway_SetDefaultPaymentMethod(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()
	fake.on("PUT /v1/customers/cust_1/cards/card_1", http.StatusOK, CardResponse{
		CustomerID: "cust_1", ID: "card_1", DefaultFlag: "1",
	})

	if err := gw.SetDefaultPaymentMethod(context.Background(), "cust_1", "cust_1/card_1"); err != nil {
		t.Fatalf("SetDefaultPaymentMethod: %v", err)
	}

	body := decodeJSONMap(t, fake.lastBody["PUT /v1/customers/cust_1/cards/card_1"])
	if body["default_flag"] != "1" {
		t.Errorf("request default_flag = %v, want 1", body["default_flag"])
	}
}

func TestGateway_SetDefaultPaymentMethod_CustomerMismatch(t *testing.T) {
	gw, fake, closeFn := setupCustomerGateway(t)
	defer closeFn()

	err := gw.SetDefaultPaymentMethod(context.Background(), "cust_1", "cust_2/card_1")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
	if len(fake.calls) != 0 {
		t.Errorf("expected no network calls on mismatch, got %v", fake.calls)
	}
}

func TestGateway_SetDefaultPaymentMethod_RequiresCustomerID(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	err := gw.SetDefaultPaymentMethod(context.Background(), "", "cust_1/card_1")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

func TestGateway_SetDefaultPaymentMethod_RequiresPaymentMethodID(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	err := gw.SetDefaultPaymentMethod(context.Background(), "cust_1", "")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

func TestGateway_SetDefaultPaymentMethod_RejectsMalformedPaymentMethodID(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()

	err := gw.SetDefaultPaymentMethod(context.Background(), "cust_1", "not-composite")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation error", err)
	}
}

// --- Nil / empty guards ---

func TestGateway_CustomerNilAndEmptyGuards(t *testing.T) {
	gw, _, closeFn := setupCustomerGateway(t)
	defer closeFn()
	ctx := context.Background()

	if _, err := gw.CreateCustomer(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("CreateCustomer(nil) = %v, want validation error", err)
	}
	if _, err := gw.UpdateCustomer(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("UpdateCustomer(nil) = %v, want validation error", err)
	}
	if _, err := gw.GetCustomer(ctx, ""); !errors.Is(err, ErrValidation) {
		t.Errorf("GetCustomer(\"\") = %v, want validation error", err)
	}
	if err := gw.DeleteCustomer(ctx, ""); !errors.Is(err, ErrValidation) {
		t.Errorf("DeleteCustomer(\"\") = %v, want validation error", err)
	}
}

// --- Compile-time conformance ---

func TestGateway_ImplementsCustomerGateway(t *testing.T) {
	var _ port.CustomerGateway = (*Gateway)(nil)
}
