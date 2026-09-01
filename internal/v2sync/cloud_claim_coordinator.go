package v2sync

import (
	"context"
	"errors"

	"venera-server/internal/v2store"
)

// CloudClaimCoordinator keeps a claim mutation and the resulting client
// projection in one repository write transaction. Planner wake-ups remain
// outside this transaction and are therefore only derived follow-up work.
type CloudClaimCoordinator struct {
	repo               *v2store.Repository
	projectionFault    func(string) error
	highChangeSeqFault func() error
}

// CloudClaimMutationResult contains the claim mutation and the client change
// watermark observed after the projection in the same write transaction.
// Callers can therefore construct a response cursor without a post-commit
// read that could observe a different state.
type CloudClaimMutationResult struct {
	Claim         v2store.CloudClaimResult
	HighChangeSeq int64
}

func NewCloudClaimCoordinator(repo *v2store.Repository) *CloudClaimCoordinator {
	return &CloudClaimCoordinator{repo: repo}
}

func (c *CloudClaimCoordinator) CommitCloudClaim(ctx context.Context, request v2store.CommitCloudClaimRequest) (CloudClaimMutationResult, error) {
	if c == nil || c.repo == nil {
		return CloudClaimMutationResult{}, errors.New("cloud claim coordinator is invalid")
	}
	var result CloudClaimMutationResult
	err := c.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		claim, err := c.repo.CommitCloudClaimTx(ctx, tx, request)
		if err != nil {
			return err
		}
		result.Claim = claim
		if err := projectClientTxWithFault(ctx, tx, request.ClientID, c.projectionFault); err != nil {
			return err
		}
		return c.loadHighChangeSeq(ctx, tx, request.ClientID, &result.HighChangeSeq)
	})
	return result, err
}

func (c *CloudClaimCoordinator) DeleteCloudClaim(ctx context.Context, request v2store.DeleteCloudClaimRequest) (CloudClaimMutationResult, error) {
	if c == nil || c.repo == nil {
		return CloudClaimMutationResult{}, errors.New("cloud claim coordinator is invalid")
	}
	var result CloudClaimMutationResult
	err := c.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		claim, err := c.repo.DeleteCloudClaimTx(ctx, tx, request)
		if err != nil {
			return err
		}
		result.Claim = claim
		if err := projectClientTxWithFault(ctx, tx, request.ClientID, c.projectionFault); err != nil {
			return err
		}
		return c.loadHighChangeSeq(ctx, tx, request.ClientID, &result.HighChangeSeq)
	})
	return result, err
}

func (c *CloudClaimCoordinator) loadHighChangeSeq(ctx context.Context, tx *v2store.Tx, clientID string, destination *int64) error {
	if c.highChangeSeqFault != nil {
		if err := c.highChangeSeqFault(); err != nil {
			return err
		}
	}
	return tx.QueryRowContext(ctx, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID).Scan(destination)
}
