package v2scan

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"venera-server/internal/v2manifest"
	"venera-server/internal/v2worker"
)

func TestDecodeProbeResultValidatesManifestContract(t *testing.T) {
	contract := manwaProbeContract()
	result, err := decodeProbeResult([]byte(`{"identity":{"scheme":"manwa-username-v1","value":"fixture-account"},"display":{"name":"昵称"},"attributes":{"accountLevel":2},"visibilityScope":"manwa:level:2","sessionPatch":null}`), contract)
	if err != nil {
		t.Fatal(err)
	}
	if result.IdentityScheme != "manwa-username-v1" || result.IdentityValue != "fixture-account" || result.AttributesJSON != `{"accountLevel":2}` {
		t.Fatalf("result = %#v", result)
	}

	for name, raw := range map[string]string{
		"unknown field":  `{"identity":{"scheme":"manwa-username-v1","value":"fixture"},"display":{"name":"name"},"attributes":{"accountLevel":2,"secret":"x"},"visibilityScope":"manwa:level:2","sessionPatch":null}`,
		"wrong scheme":   `{"identity":{"scheme":"other","value":"fixture"},"display":{"name":"name"},"attributes":{"accountLevel":2},"visibilityScope":"manwa:level:2","sessionPatch":null}`,
		"wrong scope":    `{"identity":{"scheme":"manwa-username-v1","value":"fixture"},"display":{"name":"name"},"attributes":{"accountLevel":2},"visibilityScope":"manwa:all","sessionPatch":null}`,
		"bad level":      `{"identity":{"scheme":"manwa-username-v1","value":"fixture"},"display":{"name":"name"},"attributes":{"accountLevel":-1},"visibilityScope":"manwa:level:0","sessionPatch":null}`,
		"explicit patch": `{"identity":{"scheme":"manwa-username-v1","value":"fixture"},"display":{"name":"name"},"attributes":{"accountLevel":2},"visibilityScope":"manwa:level:2","sessionPatch":{"set":{}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeProbeResult([]byte(raw), contract); err == nil {
				t.Fatal("invalid probe output was accepted")
			}
		})
	}
}

func TestAccountProbeRunnerUsesWorkerAndKeepsSessionCookiesHostSide(t *testing.T) {
	worker, err := v2worker.NewWorker(v2worker.WorkerConfig{
		ExtensionScript: []byte(`SourceServerExtensions.register({capabilities:{scanning:{probeAccount: async function() { return {identity:{scheme:"manwa-username-v1",value:"fixture-account"},display:{name:"fixture"},attributes:{accountLevel:2},visibilityScope:"manwa:level:2",sessionPatch:null}; }}}});`),
		AllowedOrigins:  []string{"https://manwa.me"},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := NewAccountProbeRunner(worker)
	result, err := runner.Probe(context.Background(), ProbeRequest{
		RequestID: "probe-test", ArtifactID: "manwa", PackageReleaseID: "rel_1550ffd2a053cf3190d0",
		Contract: manwaProbeContract(), SessionCookies: []http.Cookie{{Name: "session", Value: "secret", Domain: "manwa.me", Path: "/", Secure: true}},
		RequestedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IdentityValue != "fixture-account" || len(result.SessionCookies) != 1 || result.SessionCookies[0].Value != "secret" {
		t.Fatalf("probe result = %#v", result)
	}
	if string(result.SessionCookies[0].Value) == string(result.AttributesJSON) {
		t.Fatal("session cookie entered attributes")
	}
}

func manwaProbeContract() v2manifest.AccountProbeContract {
	return v2manifest.AccountProbeContract{
		ID: "manwa-account-probe-v1", Version: 1, IdentitySchemes: []string{"manwa-username-v1"},
		AttributeFields: []string{"accountLevel"}, VisibilityScopePattern: `^manwa:level:[0-9]+$`,
	}
}

func TestProbeAttributesRemainValidJSON(t *testing.T) {
	result, err := decodeProbeResult([]byte(`{"identity":{"scheme":"manwa-username-v1","value":"fixture"},"attributes":{"accountLevel":2},"visibilityScope":"manwa:level:2"}`), manwaProbeContract())
	if err != nil {
		t.Fatal(err)
	}
	var attributes map[string]any
	if err := json.Unmarshal([]byte(result.AttributesJSON), &attributes); err != nil || attributes["accountLevel"] != float64(2) {
		t.Fatalf("attributes = %#v, err=%v", attributes, err)
	}
}
