package v2scan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"venera-server/internal/v2manifest"
	"venera-server/internal/v2worker"
)

type ProbeRequest struct {
	RequestID        string
	ArtifactID       string
	PackageReleaseID string
	Contract         v2manifest.AccountProbeContract
	SessionCookies   []http.Cookie
	Reason           string
	RequestedAt      time.Time
}

type ProbeResult struct {
	IdentityScheme   string
	IdentityValue    string
	DisplayName      string
	DisplaySecondary string
	AttributesJSON   string
	VisibilityScope  string
	SessionCookies   []http.Cookie
}

type ProbeError struct {
	Code              string
	RetryAfterSeconds *int
	Cause             error
}

func (err *ProbeError) Error() string {
	if err == nil {
		return ""
	}
	return err.Code
}

func (err *ProbeError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

type AccountProbe interface {
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
}

type Runner struct {
	Worker *v2worker.Worker
	Clock  func() time.Time
}

func NewAccountProbeRunner(worker *v2worker.Worker) *Runner {
	return &Runner{Worker: worker, Clock: time.Now}
}

func (runner *Runner) Probe(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
	if runner == nil || runner.Worker == nil || request.ArtifactID == "" || request.PackageReleaseID == "" || request.Contract.ID == "" {
		return ProbeResult{}, &ProbeError{Code: "extension_failure"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if request.RequestID == "" {
		clock := runner.Clock
		if clock == nil {
			clock = time.Now
		}
		request.RequestID = fmt.Sprintf("probe-%d", clock().UnixNano())
	}
	if request.Reason == "" {
		request.Reason = "candidate_validation"
	}
	if request.RequestedAt.IsZero() {
		clock := runner.Clock
		if clock == nil {
			clock = time.Now
		}
		request.RequestedAt = clock().UTC()
	}
	workerResult, err := runner.Worker.Run(ctx, v2worker.RunRequest{
		RequestID:        request.RequestID,
		Operation:        v2worker.OperationProbeAccount,
		Input:            map[string]string{"reason": request.Reason, "requestedAt": request.RequestedAt.UTC().Format(time.RFC3339Nano)},
		ArtifactID:       request.ArtifactID,
		PackageReleaseID: request.PackageReleaseID,
		Budget:           v2worker.OperationBudget{MaxRequests: 2, MaxItems: 1},
		SessionCookies:   append([]http.Cookie(nil), request.SessionCookies...),
	})
	if err != nil {
		return ProbeResult{}, mapProbeError(err)
	}
	result, err := decodeProbeResult(workerResult.Output, request.Contract)
	if err != nil {
		return ProbeResult{}, &ProbeError{Code: "contract_drift", Cause: err}
	}
	result.SessionCookies = append([]http.Cookie(nil), workerResult.SessionCookies...)
	return result, nil
}

func decodeProbeResult(raw []byte, contract v2manifest.AccountProbeContract) (ProbeResult, error) {
	if len(raw) == 0 {
		return ProbeResult{}, errors.New("probe output is empty")
	}
	var wire probeOutput
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&wire); err != nil {
		return ProbeResult{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return ProbeResult{}, errors.New("probe output has multiple JSON values")
		}
		return ProbeResult{}, err
	}
	if err := validateProbeOutput(wire, contract); err != nil {
		return ProbeResult{}, err
	}
	attributes, err := json.Marshal(wire.Attributes)
	if err != nil {
		return ProbeResult{}, err
	}
	result := ProbeResult{
		IdentityScheme: wire.Identity.Scheme, IdentityValue: wire.Identity.Value,
		AttributesJSON: string(attributes), VisibilityScope: wire.VisibilityScope,
	}
	if wire.Display != nil {
		result.DisplayName = wire.Display.Name
		result.DisplaySecondary = wire.Display.Secondary
	}
	return result, nil
}

// DecodeProbeResult exposes the same strict host-side contract validation to
// runtime adapters that use a testable worker boundary instead of the
// concrete QuickJS worker.
func DecodeProbeResult(raw []byte, contract v2manifest.AccountProbeContract) (ProbeResult, error) {
	return decodeProbeResult(raw, contract)
}

type probeOutput struct {
	Identity        probeIdentity              `json:"identity"`
	Display         *probeDisplay              `json:"display,omitempty"`
	Attributes      map[string]json.RawMessage `json:"attributes"`
	VisibilityScope string                     `json:"visibilityScope"`
	SessionPatch    json.RawMessage            `json:"sessionPatch,omitempty"`
}

type probeIdentity struct {
	Scheme string `json:"scheme"`
	Value  string `json:"value"`
}

type probeDisplay struct {
	Name      string `json:"name"`
	Secondary string `json:"secondary,omitempty"`
}

func validateProbeOutput(output probeOutput, contract v2manifest.AccountProbeContract) error {
	if output.Identity.Scheme == "" || output.Identity.Value == "" || !contains(contract.IdentitySchemes, output.Identity.Scheme) {
		return errors.New("probe identity is not approved")
	}
	if !validProbeString(output.Identity.Value, 512) || strings.TrimSpace(output.Identity.Value) != output.Identity.Value {
		return errors.New("probe identity value is invalid")
	}
	if output.Display != nil {
		if !validProbeString(output.Display.Name, 512) || output.Display.Secondary != "" && !validProbeString(output.Display.Secondary, 512) {
			return errors.New("probe display is invalid")
		}
	}
	if output.Attributes == nil {
		return errors.New("probe attributes are required")
	}
	allowed := make(map[string]struct{}, len(contract.AttributeFields))
	for _, field := range contract.AttributeFields {
		if field == "" {
			return errors.New("probe contract has an empty attribute field")
		}
		allowed[field] = struct{}{}
	}
	for field, raw := range output.Attributes {
		if _, ok := allowed[field]; !ok || len(raw) == 0 || !json.Valid(raw) {
			return errors.New("probe attribute is not approved")
		}
		if err := validateJSONValue(raw); err != nil {
			return err
		}
		if field == "accountLevel" {
			var number json.Number
			if err := json.Unmarshal(raw, &number); err != nil || number == "" {
				return errors.New("account level is invalid")
			}
			level, err := number.Int64()
			if err != nil || level < 0 {
				return errors.New("account level is invalid")
			}
		}
	}
	if contract.VisibilityScopePattern == "" {
		return errors.New("probe scope contract is empty")
	}
	pattern, err := regexp.Compile(contract.VisibilityScopePattern)
	if err != nil || !pattern.MatchString(output.VisibilityScope) || !validProbeString(output.VisibilityScope, 512) {
		return errors.New("probe visibility scope is invalid")
	}
	if len(output.SessionPatch) > 0 && string(output.SessionPatch) != "null" {
		return errors.New("explicit session patch is not approved")
	}
	return nil
}

func validateJSONValue(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return errors.New("probe attribute is not JSON")
	}
	return validateJSONNode(value)
}

func validateJSONNode(value any) error {
	switch typed := value.(type) {
	case nil, bool, string, json.Number:
		return nil
	case []any:
		for _, child := range typed {
			if err := validateJSONNode(child); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, child := range typed {
			if err := validateJSONNode(child); err != nil {
				return err
			}
		}
	default:
		return errors.New("probe value is not JSON-compatible")
	}
	return nil
}

func validProbeString(value string, maxBytes int) bool {
	return value != "" && len([]byte(value)) <= maxBytes && !strings.ContainsAny(value, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f")
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func mapProbeError(err error) error {
	var structured v2worker.StructuredError
	if errors.As(err, &structured) {
		return &ProbeError{Code: structured.Code, RetryAfterSeconds: structured.RetryAfterSeconds, Cause: err}
	}
	if errors.Is(err, v2worker.ErrWorkerDeadline) {
		return &ProbeError{Code: "transient", Cause: err}
	}
	return &ProbeError{Code: "extension_failure", Cause: err}
}
