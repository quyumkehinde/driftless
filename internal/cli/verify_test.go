package cli

import (
	"testing"
)

func TestVerifyDefaultsToFullMode(t *testing.T) {
	opts, err := verifyOptions(false, false, nil, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.Full {
		t.Error("bare verify must run full mode; quick is the explicit opt-in")
	}

	opts, err = verifyOptions(true, false, nil, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Full {
		t.Error("--quick must select quick mode")
	}
}

func TestVerifyRejectsBothModes(t *testing.T) {
	if _, err := verifyOptions(true, true, nil, "", false); err == nil {
		t.Error("--quick with --full must be a usage error")
	}
}

func TestVerifyRejectsUnverifiableType(t *testing.T) {
	if _, err := verifyOptions(false, false, []string{"payment_methods"}, "", false); err == nil {
		t.Error("payment_methods must be rejected before a run starts")
	}
}
