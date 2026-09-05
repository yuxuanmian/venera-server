package scan

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateCheckpointKeepsOpaqueBoundedObject(t *testing.T) {
	if err := ValidateCheckpoint([]byte(`{"parserVersion":"manwa-scanning-v1","nextOffset":15}`), 128); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCheckpoint([]byte(`{"nested":[1]}`), 128); err != nil {
		t.Fatalf("opaque checkpoint error = %v", err)
	}
	for _, raw := range []string{"", "null", "[]", "not-json"} {
		if err := ValidateCheckpoint([]byte(raw), 128); !errors.Is(err, ErrCheckpointInvalid) {
			t.Fatalf("checkpoint %q error = %v", raw, err)
		}
	}
	if err := ValidateCheckpoint([]byte(`{"value":"`+strings.Repeat("x", 20)+`"}`), 10); !errors.Is(err, ErrCheckpointInvalid) {
		t.Fatalf("oversized checkpoint error = %v", err)
	}
}
