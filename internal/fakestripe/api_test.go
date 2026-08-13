package fakestripe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode
}

type listResponse struct {
	Object  string           `json:"object"`
	HasMore bool             `json:"has_more"`
	Data    []map[string]any `json:"data"`
}

func TestAccountEndpoint(t *testing.T) {
	s := New(t, "whsec_test")
	var account map[string]any
	if status := getJSON(t, s.URL()+"/v1/account", &account); status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if account["id"] != AccountID {
		t.Errorf("account id = %v", account["id"])
	}
}

func TestGetObjectAndMissing(t *testing.T) {
	s := New(t, "whsec_test")
	s.Put("customer", "cus_1", map[string]any{"email": "a@b.c"}, "customer.created")

	var obj map[string]any
	if status := getJSON(t, s.URL()+"/v1/customers/cus_1", &obj); status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if obj["email"] != "a@b.c" {
		t.Errorf("obj = %v", obj)
	}

	var errBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if status := getJSON(t, s.URL()+"/v1/customers/cus_missing", &errBody); status != http.StatusNotFound {
		t.Fatalf("missing object status = %d, want 404", status)
	}
	if errBody.Error.Code != "resource_missing" {
		t.Errorf("error code = %q, want resource_missing", errBody.Error.Code)
	}

	// checkout sessions use their two-segment path
	s.Put("checkout_session", "cs_1", nil, "checkout.session.completed")
	if status := getJSON(t, s.URL()+"/v1/checkout/sessions/cs_1", &obj); status != http.StatusOK {
		t.Errorf("checkout session status = %d", status)
	}
}

func TestListPagination(t *testing.T) {
	s := New(t, "whsec_test")
	for i := range 25 {
		s.Put("customer", fmt.Sprintf("cus_%02d", i), nil, "customer.created")
	}

	// newest first, default limit 10
	var page listResponse
	getJSON(t, s.URL()+"/v1/customers", &page)
	if len(page.Data) != 10 || !page.HasMore {
		t.Fatalf("first page: n=%d has_more=%v", len(page.Data), page.HasMore)
	}
	if page.Data[0]["id"] != "cus_24" {
		t.Errorf("newest first: got %v", page.Data[0]["id"])
	}

	// walk the full list with the cursor
	var all []string
	cursor := ""
	for {
		url := s.URL() + "/v1/customers?limit=10"
		if cursor != "" {
			url += "&starting_after=" + cursor
		}
		var p listResponse
		getJSON(t, url, &p)
		for _, obj := range p.Data {
			all = append(all, obj["id"].(string))
		}
		if !p.HasMore {
			break
		}
		cursor = p.Data[len(p.Data)-1]["id"].(string)
	}
	if len(all) != 25 {
		t.Errorf("walked %d objects, want 25", len(all))
	}
	seen := make(map[string]bool)
	for _, id := range all {
		if seen[id] {
			t.Errorf("duplicate %s in pagination walk", id)
		}
		seen[id] = true
	}
}

func TestEventsEndpoint(t *testing.T) {
	s := New(t, "whsec_test")
	s.Put("customer", "cus_1", nil, "customer.created")
	s.Advance(time.Hour)
	cutoff := s.Put("customer", "cus_1", nil, "customer.updated")
	s.Put("customer", "cus_2", nil, "customer.created")

	// newest first
	var page listResponse
	getJSON(t, s.URL()+"/v1/events", &page)
	if len(page.Data) != 3 {
		t.Fatalf("events = %d, want 3", len(page.Data))
	}
	if page.Data[0]["type"] != "customer.created" || page.Data[2]["type"] != "customer.created" {
		t.Errorf("unexpected order: %v, %v", page.Data[0]["type"], page.Data[2]["type"])
	}

	// created[gte] filter cuts off the first event
	url := fmt.Sprintf("%s/v1/events?created[gte]=%d", s.URL(), cutoff.Created.Unix())
	var filtered listResponse
	getJSON(t, url, &filtered)
	if len(filtered.Data) != 2 {
		t.Errorf("filtered events = %d, want 2", len(filtered.Data))
	}
}
