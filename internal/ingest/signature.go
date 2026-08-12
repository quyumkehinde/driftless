// Package ingest implements the webhook receiving side: signature
// verification and the HTTP server that records events.
package ingest

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Signature verification errors, distinguished so the server can count and
// log the failure reason without ever logging the payload.
var (
	ErrMissingHeader    = errors.New("missing Stripe-Signature header")
	ErrMalformedHeader  = errors.New("malformed Stripe-Signature header")
	ErrTimestampRange   = errors.New("signature timestamp outside tolerance")
	ErrNoValidSignature = errors.New("no signature matched any configured secret")
)

// Verifier checks Stripe webhook signatures. It supports a primary and an
// optional secondary secret so secrets can rotate without a gap.
type Verifier struct {
	secrets   [][]byte
	tolerance time.Duration
	now       func() time.Time
}

// NewVerifier builds a Verifier. secondary may be empty. tolerance bounds
// |now - t| to limit replay; the event-log dedupe bounds it further.
func NewVerifier(primary, secondary string, tolerance time.Duration) *Verifier {
	secrets := [][]byte{[]byte(primary)}
	if secondary != "" {
		secrets = append(secrets, []byte(secondary))
	}
	return &Verifier{
		secrets:   secrets,
		tolerance: tolerance,
		now:       time.Now,
	}
}

// Verify checks header against the raw request body exactly as received.
// The body must not be re-serialized before the check.
func (v *Verifier) Verify(header string, body []byte) error {
	if header == "" {
		return ErrMissingHeader
	}
	ts, sigs, err := parseHeader(header)
	if err != nil {
		return err
	}

	// The window is symmetric: a stale timestamp is a replay, and a
	// far-future one (clock skew) would stay replayable until it expires.
	if v.now().Sub(time.Unix(ts, 0)).Abs() > v.tolerance {
		return ErrTimestampRange
	}

	signed := fmt.Sprintf("%d.", ts)
	for _, secret := range v.secrets {
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(signed))
		mac.Write(body)
		expected := mac.Sum(nil)
		for _, sig := range sigs {
			if hmac.Equal(expected, sig) {
				return nil
			}
		}
	}
	return ErrNoValidSignature
}

// parseHeader extracts the timestamp and every v1 signature from a header of
// the form "t=<unix>,v1=<hex>,v1=<hex>,...". Unknown schemes are ignored;
// v1 values that are not valid hex are skipped rather than rejected, since
// any one matching signature is sufficient.
func parseHeader(header string) (ts int64, sigs [][]byte, err error) {
	sawTimestamp := false
	for part := range strings.SplitSeq(header, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			return 0, nil, ErrMalformedHeader
		}
		switch key {
		case "t":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || sawTimestamp {
				return 0, nil, ErrMalformedHeader
			}
			ts = parsed
			sawTimestamp = true
		case "v1":
			decoded, err := hex.DecodeString(value)
			if err != nil {
				continue
			}
			sigs = append(sigs, decoded)
		}
	}
	if !sawTimestamp || len(sigs) == 0 {
		return 0, nil, ErrMalformedHeader
	}
	return ts, sigs, nil
}
