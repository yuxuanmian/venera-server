package scan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"venera-server/internal/tracking/domain"
	"venera-server/internal/tracking/worker"
)

// AccountProbeContract is the small, source-owned contract needed to turn a
// scanner's account probe into a usable visibility boundary. It intentionally
// contains no package or candidate metadata.
type AccountProbeContract struct {
	ID                     string
	Version                int
	IdentitySchemes        []string
	AttributeFields        []string
	VisibilityScopePattern string
}

type ProbeRequest struct {
	RequestID         string
	Artifact          domain.ArtifactIdentity
	Revision          string
	RuntimeGeneration int64
	Contract          AccountProbeContract
	SessionCookies    []http.Cookie
	Reason            string
	RequestedAt       time.Time
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
	if err.Code == "" {
		return "account probe failed"
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

type AccountProbeRunner struct {
	Worker *worker.Worker
	Clock  func() time.Time
}

func NewAccountProbeRunner(runtimeWorker *worker.Worker) *AccountProbeRunner {
	return &AccountProbeRunner{Worker: runtimeWorker, Clock: time.Now}
}

func (runner *AccountProbeRunner) Probe(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
	if runner == nil || runner.Worker == nil {
		return ProbeResult{}, &ProbeError{Code: "extension_failure"}
	}
	if err := request.Artifact.Validate(); err != nil {
		return ProbeResult{}, &ProbeError{Code: "extension_failure", Cause: err}
	}
	if request.Contract.ID == "" {
		return ProbeResult{}, &ProbeError{Code: "extension_failure"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	clock := runner.Clock
	if clock == nil {
		clock = time.Now
	}
	if request.RequestID == "" {
		request.RequestID = "probe-" + request.Artifact.SourceKey + "-" + clock().UTC().Format("20060102150405.000000000")
	}
	if request.Reason == "" {
		request.Reason = "account_probe"
	}
	if request.RequestedAt.IsZero() {
		request.RequestedAt = clock().UTC()
	}
	workerResult, err := runner.Worker.Run(ctx, worker.RunRequest{
		RequestID: request.RequestID,
		Operation: worker.OperationProbeAccount,
		Input: map[string]string{
			"reason":      request.Reason,
			"requestedAt": request.RequestedAt.UTC().Format(time.RFC3339Nano),
		},
		ArtifactID:        request.Artifact.SourceKey,
		FileName:          request.Artifact.FileName,
		Revision:          request.Revision,
		RuntimeGeneration: request.RuntimeGeneration,
		Budget:            worker.OperationBudget{MaxRequests: 2, MaxItems: 1},
		SessionCookies:    append([]http.Cookie(nil), request.SessionCookies...),
	})
	if err != nil {
		return ProbeResult{}, mapProbeError(err)
	}
	result, err := DecodeProbeResult(workerResult.Output, request.Contract)
	if err != nil {
		return ProbeResult{}, &ProbeError{Code: "contract_drift", Cause: err}
	}
	result.SessionCookies = append([]http.Cookie(nil), workerResult.SessionCookies...)
	return result, nil
}

// DecodeProbeResult is exported so a process/IPC worker can use exactly the
// same strict host-side validation as the in-process QuickJS worker.
func DecodeProbeResult(raw []byte, contract AccountProbeContract) (ProbeResult, error) {
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
		IdentityScheme:  wire.Identity.Scheme,
		IdentityValue:   wire.Identity.Value,
		AttributesJSON:  string(attributes),
		VisibilityScope: wire.VisibilityScope,
	}
	if wire.Display != nil {
		result.DisplayName = wire.Display.Name
		result.DisplaySecondary = wire.Display.Secondary
	}
	return result, nil
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

func validateProbeOutput(output probeOutput, contract AccountProbeContract) error {
	if output.Identity.Scheme == "" || output.Identity.Value == "" ||
		!contains(contract.IdentitySchemes, output.Identity.Scheme) {
		return errors.New("probe identity is not approved")
	}
	if !validProbeString(output.Identity.Value, 512) || strings.TrimSpace(output.Identity.Value) != output.Identity.Value {
		return errors.New("probe identity value is invalid")
	}
	if output.Display != nil {
		if !validProbeString(output.Display.Name, 512) ||
			(output.Display.Secondary != "" && !validProbeString(output.Display.Secondary, 512)) {
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
	if err != nil || !pattern.MatchString(output.VisibilityScope) ||
		!validProbeString(output.VisibilityScope, 512) {
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
	return value != "" && len([]byte(value)) <= maxBytes &&
		!strings.ContainsAny(value, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x7f")
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
	var structured worker.StructuredError
	if errors.As(err, &structured) {
		return &ProbeError{Code: structured.Code, RetryAfterSeconds: structured.RetryAfterSeconds, Cause: err}
	}
	if errors.Is(err, worker.ErrWorkerDeadline) {
		return &ProbeError{Code: "transient", Cause: err}
	}
	return &ProbeError{Code: "extension_failure", Cause: err}
}

var _ AccountProbe = (*AccountProbeRunner)(nil)
