package v2domain

import "time"

type ClientID string
type SourceAccountID string
type ArtifactID string
type PackageReleaseID string
type CandidateID string
type PreparationID string
type SnapshotReceiptID string
type Revision int64
type ChangeSeq int64

type ClientState string

const (
	ClientActive  ClientState = "active"
	ClientRevoked ClientState = "revoked"
)

type SourceAccountState string

const (
	SourceAccountActive         SourceAccountState = "active"
	SourceAccountReauthRequired SourceAccountState = "reauthRequired"
	SourceAccountDisabled       SourceAccountState = "disabled"
)

type LinkState string

const (
	LinkLinked   LinkState = "linked"
	LinkUnlinked LinkState = "unlinked"
)

type ClaimState string

const (
	ClaimActive    ClaimState = "active"
	ClaimSuspended ClaimState = "suspended"
)

type CompatibilityState string

const (
	CompatibilityCompatible   CompatibilityState = "compatible"
	CompatibilityIncompatible CompatibilityState = "incompatible"
	CompatibilityUnknown      CompatibilityState = "unknown"
)

type Client struct {
	ID          ClientID
	DisplayName string
	Platform    string
	AppVersion  string
	State       ClientState
	Revision    Revision
	LastSeenAt  *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	RevokedAt   *time.Time
}

type SourceAccount struct {
	ID               SourceAccountID
	ArtifactID       ArtifactID
	State            SourceAccountState
	IdentityScheme   string
	IdentityDigest   string
	IdentityDisplay  string
	AttributesJSON   string
	VisibilityScope  string
	SessionEpoch     int64
	SessionRevision  int64
	IdentityVerified time.Time
	ScopeFreshUntil  time.Time
	Revision         Revision
}

type SourceAccountSummary struct {
	SourceAccountID SourceAccountID
	ArtifactID      ArtifactID
	IdentityScheme  string
	IdentityDisplay string
	AttributesJSON  string
	VisibilityScope string
	AccountState    SourceAccountState
	LinkState       LinkState
	Selected        bool
	AccountRevision Revision
	LinkRevision    Revision
	Revision        Revision
}

type InventoryEntry struct {
	ArtifactID                   ArtifactID
	PackageReleaseID             PackageReleaseID
	ManagementMode               string
	CompatibilityState           CompatibilityState
	CoreHash                     string
	ClientExtensionsJSON         string
	ObservationContractID        string
	AccountObservationContractID string
	AccountProbeContractID       string
}

type SourceState struct {
	ArtifactID              ArtifactID
	SelectedSourceAccountID SourceAccountID
	LinkState               LinkState
	ClaimState              string
	EffectiveState          string
	CompatibilityState      CompatibilityState
	SourceStatus            string
	FreshUntil              *time.Time
	NextEvaluationAt        *time.Time
	BlockedReason           string
	Revision                Revision
}

type CandidateState string

const (
	CandidateProbing                    CandidateState = "probing"
	CandidateReadyLink                  CandidateState = "readyLink"
	CandidateSwitchConfirmationRequired CandidateState = "switchConfirmationRequired"
	CandidateActivated                  CandidateState = "activated"
	CandidateCancelled                  CandidateState = "cancelled"
	CandidateExpired                    CandidateState = "expired"
	CandidateFailed                     CandidateState = "failed"
)

type PreparationState string

const (
	PreparationPreparing     PreparationState = "preparing"
	PreparationSnapshotReady PreparationState = "snapshotReady"
	PreparationCommitted     PreparationState = "committed"
	PreparationCancelled     PreparationState = "cancelled"
	PreparationExpired       PreparationState = "expired"
	PreparationFailed        PreparationState = "failed"
)

type PreparationStage string

const (
	PreparationValidateAccount   PreparationStage = "validateAccount"
	PreparationValidateInventory PreparationStage = "validateInventory"
	PreparationWaitSnapshot      PreparationStage = "waitSnapshot"
	PreparationDeliverSnapshot   PreparationStage = "deliverSnapshot"
	PreparationDone              PreparationStage = "done"
)
