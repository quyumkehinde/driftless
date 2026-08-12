package ingest

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v84/webhook"
)

const (
	testSecret    = "whsec_test_primary_secret"
	testSecondary = "whsec_test_secondary_secret"
	tolerance     = 300 * time.Second
)

// fixedNow keeps the verifier's clock still so timestamp cases are exact.
var fixedNow = time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

// signHeader produces a valid Stripe-Signature header for body at time t.
// fakestripe's webhook driver carries its own signer; the cross-check tests
// pin both against stripe-go so they cannot drift apart.
func signHeader(secret string, t time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%d.", t.Unix())
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", t.Unix(), hex.EncodeToString(mac.Sum(nil)))
}

func newTestVerifier(secondary string) *Verifier {
	v := NewVerifier(testSecret, secondary, tolerance)
	v.now = func() time.Time { return fixedNow }
	return v
}

func TestVerify(t *testing.T) {
	body := []byte(`{"id": "evt_123", "type": "customer.updated"}`)

	tests := []struct {
		name    string
		header  string
		body    []byte
		wantErr error
	}{
		{
			name:   "valid",
			header: signHeader(testSecret, fixedNow, body),
			body:   body,
		},
		{
			name:   "valid at positive tolerance edge",
			header: signHeader(testSecret, fixedNow.Add(-tolerance), body),
			body:   body,
		},
		{
			name:    "expired timestamp",
			header:  signHeader(testSecret, fixedNow.Add(-tolerance-time.Second), body),
			body:    body,
			wantErr: ErrTimestampRange,
		},
		{
			name:    "future timestamp",
			header:  signHeader(testSecret, fixedNow.Add(tolerance+time.Second), body),
			body:    body,
			wantErr: ErrTimestampRange,
		},
		{
			name:    "wrong secret",
			header:  signHeader("whsec_wrong", fixedNow, body),
			body:    body,
			wantErr: ErrNoValidSignature,
		},
		{
			name:    "tampered body",
			header:  signHeader(testSecret, fixedNow, body),
			body:    []byte(`{"id": "evt_123", "type": "customer.deleted"}`),
			wantErr: ErrNoValidSignature,
		},
		{
			name:    "missing header",
			header:  "",
			body:    body,
			wantErr: ErrMissingHeader,
		},
		{
			name:    "garbage header",
			header:  "not a signature",
			body:    body,
			wantErr: ErrMalformedHeader,
		},
		{
			name:    "no v1 values",
			header:  "t=1755000000,v0=abc",
			body:    body,
			wantErr: ErrMalformedHeader,
		},
		{
			name:    "no timestamp",
			header:  "v1=" + strings.Repeat("ab", 32),
			body:    body,
			wantErr: ErrMalformedHeader,
		},
		{
			name:    "duplicate timestamp",
			header:  fmt.Sprintf("t=%d,t=%d,v1=%s", fixedNow.Unix(), fixedNow.Unix(), strings.Repeat("ab", 32)),
			body:    body,
			wantErr: ErrMalformedHeader,
		},
		{
			name: "multiple v1 one valid",
			header: fmt.Sprintf("t=%d,v1=%s,%s",
				fixedNow.Unix(), strings.Repeat("ab", 32),
				strings.TrimPrefix(signHeader(testSecret, fixedNow, body), fmt.Sprintf("t=%d,", fixedNow.Unix()))),
			body: body,
		},
		{
			name:    "multiple v1 none valid",
			header:  fmt.Sprintf("t=%d,v1=%s,v1=%s", fixedNow.Unix(), strings.Repeat("ab", 32), strings.Repeat("cd", 32)),
			body:    body,
			wantErr: ErrNoValidSignature,
		},
		{
			name:   "invalid hex v1 skipped when another matches",
			header: signHeader(testSecret, fixedNow, body) + ",v1=nothex",
			body:   body,
		},
		{
			name:   "non-utf8 body",
			header: signHeader(testSecret, fixedNow, []byte{0xff, 0xfe, 0x00, 0x80}),
			body:   []byte{0xff, 0xfe, 0x00, 0x80},
		},
		{
			name:   "whitespace between parts",
			header: strings.ReplaceAll(signHeader(testSecret, fixedNow, body), ",", ", "),
			body:   body,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := newTestVerifier("").Verify(tc.header, tc.body)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Verify() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestVerifySecondarySecret(t *testing.T) {
	body := []byte(`{"id": "evt_rotate"}`)
	header := signHeader(testSecondary, fixedNow, body)

	if err := newTestVerifier(testSecondary).Verify(header, body); err != nil {
		t.Errorf("secondary-signed payload rejected during rotation: %v", err)
	}
	if err := newTestVerifier("").Verify(header, body); !errors.Is(err, ErrNoValidSignature) {
		t.Errorf("secondary-signed payload without secondary configured = %v, want ErrNoValidSignature", err)
	}
	// primary still accepted with secondary configured
	if err := newTestVerifier(testSecondary).Verify(signHeader(testSecret, fixedNow, body), body); err != nil {
		t.Errorf("primary-signed payload rejected while secondary configured: %v", err)
	}
}

// TestCrossCheckStripeGo verifies our implementation agrees with stripe-go's
// webhook helper on the same inputs, in both directions: headers we produce
// and headers stripe-go produces.
func TestCrossCheckStripeGo(t *testing.T) {
	body := []byte(`{"id": "evt_cross", "object": "event"}`)

	cases := []struct {
		name   string
		header string
		body   []byte
	}{
		{"valid ours", signHeader(testSecret, fixedNow, body), body},
		{"wrong secret", signHeader("whsec_other", fixedNow, body), body},
		{"tampered body", signHeader(testSecret, fixedNow, body), []byte(`{"id": "evt_x"}`)},
		{"multiple v1 one valid", fmt.Sprintf("t=%d,v1=%s,%s",
			fixedNow.Unix(), strings.Repeat("ab", 32),
			strings.TrimPrefix(signHeader(testSecret, fixedNow, body), fmt.Sprintf("t=%d,", fixedNow.Unix()))), body},
		{"garbage v1", fmt.Sprintf("t=%d,v1=%s", fixedNow.Unix(), strings.Repeat("00", 32)), body},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ourErr := newTestVerifier("").Verify(tc.header, tc.body)
			stripeErr := webhook.ValidatePayloadIgnoringTolerance(tc.body, tc.header, testSecret)
			if (ourErr == nil) != (stripeErr == nil) {
				t.Errorf("disagreement: ours = %v, stripe-go = %v", ourErr, stripeErr)
			}
		})
	}
}

// TestStripeGoAcceptsOurHeaders pins the signing helper itself against
// stripe-go; fakestripe's driver signer must satisfy the same test.
func TestStripeGoAcceptsOurHeaders(t *testing.T) {
	body := []byte(`{"id": "evt_sign", "object": "event", "api_version": "2026-01-01"}`)
	header := signHeader(testSecret, time.Now(), body)
	if err := webhook.ValidatePayload(body, header, testSecret); err != nil {
		t.Errorf("stripe-go rejected a header from signHeader: %v", err)
	}
}

func TestParseHeaderTimestamp(t *testing.T) {
	body := []byte("x")
	header := signHeader(testSecret, fixedNow, body)
	ts, sigs, err := parseHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if ts != fixedNow.Unix() {
		t.Errorf("ts = %d, want %d", ts, fixedNow.Unix())
	}
	if len(sigs) != 1 || len(sigs[0]) != 32 {
		t.Errorf("sigs = %d entries, first len %d; want 1 entry of 32 bytes", len(sigs), len(sigs[0]))
	}
	if _, err := hex.DecodeString(strings.Split(header, "v1=")[1]); err != nil {
		t.Errorf("v1 is not hex: %v", err)
	}
}
