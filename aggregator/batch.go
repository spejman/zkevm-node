package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/log"
	"github.com/0xPolygonHermez/zkevm-node/state"
)

func (a *Aggregator) tryGenerateBatchProof(ctx context.Context, prover proverInterface) (bool, error) {
	log := log.WithFields(
		"prover", prover.Name(),
		"proverId", prover.ID(),
		"proverAddr", prover.Addr(),
	)
	log.Debug("tryGenerateBatchProof start")

	batchToProve, proof, err0 := a.getAndLockBatchToProve(ctx, prover)
	if errors.Is(err0, state.ErrNotFound) {
		// nothing to proof, swallow the error
		log.Debug("Nothing to generate proof")
		return false, nil
	}
	if err0 != nil {
		return false, err0
	}

	log = log.WithFields("batch", batchToProve.BatchNumber)

	var (
		genProofID *string
		err        error
	)

	defer func() {
		if err != nil {
			err2 := a.State.DeleteBatchProofs(a.ctx, proof.BatchNumber, proof.BatchNumberFinal, nil)
			if err2 != nil {
				log.Errorf("Failed to delete proof in progress, err: %v", err2)
			}
		}
		log.Debug("tryGenerateBatchProof end")
	}()

	log.Info("Generating proof from batch")

	log.Infof("Sending zki + batch to the prover, batchNumber [%d]", batchToProve.BatchNumber)
	inputProver, err := a.buildBatchProverInputs(ctx, batchToProve)
	if err != nil {
		err = fmt.Errorf("failed to build input prover, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	b, err := json.Marshal(inputProver)
	if err != nil {
		err = fmt.Errorf("failed to serialize input prover, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	proof.InputProver = string(b)

	log.Infof("Sending a batch to the prover, oldStateRoot [%#x]", inputProver.PublicInputs.OldStateRoot)

	genProofID, err = prover.BatchProof(inputProver)
	if err != nil {
		err = fmt.Errorf("failed to get batch proof id, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	proof.Id = genProofID

	log.Infof("Proof ID %v", *proof.Id)
	log = log.WithFields("proofId", *proof.Id)

	proofData, err := prover.WaitRecursiveProof(ctx, *proof.Id)
	if err != nil {
		err = fmt.Errorf("failed to get proof from prover, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	log.Info("Batch proof generated")

	proof.Data = proofData

	/*// NOTE(pg): the defer func is useless from now on, use a different variable
	// name for errors (or shadow err in inner scopes) to not trigger it.

	finalProofBuilt, finalProofErr := a.tryBuildFinalProof(ctx, prover, proof)
	if finalProofErr != nil {
		// just log the error and continue to handle the generated proof
		log.Errorf("Error trying to build final proof: %v", finalProofErr)
	}

	// NOTE(pg): prover is done, use a.ctx from now on

	if !finalProofBuilt {*/
	proof.GeneratingSince = nil

	// final proof has not been generated, update the batch proof
	err = a.State.UpdateBatchProof(a.ctx, proof, nil)
	if err != nil {
		err = fmt.Errorf("failed to store batch proof result, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}
	//}

	return true, nil
}

func (a *Aggregator) getAndLockBatchToProve(ctx context.Context, prover proverInterface) (*state.Batch, *state.BatchProof, error) {
	proverID := prover.ID()
	proverName := prover.Name()

	log := log.WithFields(
		"prover", proverName,
		"proverId", proverID,
		"proverAddr", prover.Addr(),
	)

	a.StateDBMutex.Lock()
	defer a.StateDBMutex.Unlock()

	lastVerifiedBatch, err := a.State.GetLastVerifiedBatch(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	// Calculate max L1 block number for getting next virtual batch to prove
	maxL1BlockNumber, err := a.getMaxL1BlockNumber(ctx)
	if err != nil {
		log.Errorf("Failed to get max L1 block number, err: %v", err)
		return nil, nil, err
	}

	log.Debugf("Max L1 block number for getting next virtual batch to prove: %d", maxL1BlockNumber)

	// Get virtual batch pending to generate proof
	batchToVerify, err := a.State.GetVirtualBatchToProve(ctx, lastVerifiedBatch.BatchNumber, maxL1BlockNumber, nil)
	if err != nil {
		return nil, nil, err
	}

	log.Infof("Found virtual batch %d pending to generate proof", batchToVerify.BatchNumber)
	log = log.WithFields("batch", batchToVerify.BatchNumber)

	log.Info("Checking profitability to aggregate batch")

	// pass pol collateral as zero here, bcs in smart contract fee for aggregator is not defined yet
	isProfitable, err := a.ProfitabilityChecker.IsProfitable(ctx, big.NewInt(0))
	if err != nil {
		log.Errorf("Failed to check aggregator profitability, err: %v", err)
		return nil, nil, err
	}

	if !isProfitable {
		log.Infof("Batch is not profitable, pol collateral %d", big.NewInt(0))
		return nil, nil, err
	}

	now := time.Now().Round(time.Microsecond)
	proof := &state.BatchProof{
		BatchNumber:      batchToVerify.BatchNumber,
		BatchNumberFinal: batchToVerify.BatchNumber,
		Proof: state.Proof{
			Prover:          &proverName,
			ProverID:        &proverID,
			GeneratingSince: &now,
		},
	}

	// Avoid other prover to process the same batch
	err = a.State.AddBatchProof(ctx, proof, nil)
	if err != nil {
		log.Errorf("Failed to add batch proof, err: %v", err)
		return nil, nil, err
	}

	return batchToVerify, proof, nil
}

func (a *Aggregator) tryAggregateBatchProofs(ctx context.Context, prover proverInterface) (bool, error) {
	proverName := prover.Name()
	proverID := prover.ID()

	log := log.WithFields(
		"prover", proverName,
		"proverId", proverID,
		"proverAddr", prover.Addr(),
	)
	log.Debug("tryAggregateProofs start")

	proof1, proof2, err0 := a.getAndLockBatchProofsToAggregate(ctx, prover)
	if errors.Is(err0, state.ErrNotFound) {
		// nothing to aggregate, swallow the error
		log.Debug("Nothing to aggregate")
		return false, nil
	}
	if err0 != nil {
		return false, err0
	}

	var (
		aggrProofID *string
		err         error
	)

	defer func() {
		if err != nil {
			err2 := a.unlockBatchProofsToAggregate(a.ctx, proof1, proof2)
			if err2 != nil {
				log.Errorf("Failed to release aggregated proofs, err: %v", err2)
			}
		}
		log.Debug("tryAggregateProofs end")
	}()

	log.Infof("Aggregating proofs: %d-%d and %d-%d", proof1.BatchNumber, proof1.BatchNumberFinal, proof2.BatchNumber, proof2.BatchNumberFinal)

	batches := fmt.Sprintf("%d-%d", proof1.BatchNumber, proof2.BatchNumberFinal)
	log = log.WithFields("batches", batches)

	inputProver := map[string]interface{}{
		"recursive_proof_1": proof1.Proof,
		"recursive_proof_2": proof2.Proof,
	}
	b, err := json.Marshal(inputProver)
	if err != nil {
		err = fmt.Errorf("failed to serialize input prover, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	proof := &state.BatchProof{
		BatchNumber:      proof1.BatchNumber,
		BatchNumberFinal: proof2.BatchNumberFinal,
		Proof: state.Proof{
			Prover:      &proverName,
			ProverID:    &proverID,
			InputProver: string(b),
		},
	}

	aggrProofID, err = prover.AggregatedProof(proof1.Data, proof2.Data)
	if err != nil {
		err = fmt.Errorf("failed to get aggregated proof id, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	proof.Id = aggrProofID

	log.Infof("Proof ID for aggregated proof: %v", *proof.Id)
	log = log.WithFields("proofId", *proof.Id)

	proofData, err := prover.WaitRecursiveProof(ctx, *proof.Id)
	if err != nil {
		err = fmt.Errorf("failed to get aggregated proof from prover, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	log.Info("Aggregated proof generated")

	proof.Data = proofData

	// update the state by removing the 2 aggregated proofs and storing the
	// newly generated recursive proof
	dbTx, err := a.State.BeginStateTransaction(ctx)
	if err != nil {
		err = fmt.Errorf("failed to begin transaction to update proof aggregation state, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	err = a.State.DeleteBatchProofs(ctx, proof1.BatchNumber, proof2.BatchNumberFinal, dbTx)
	if err != nil {
		if err := dbTx.Rollback(ctx); err != nil {
			err := fmt.Errorf("failed to rollback proof aggregation state, %w", err)
			log.Error(FirstToUpper(err.Error()))
			return false, err
		}
		err = fmt.Errorf("failed to delete previously aggregated proofs, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	now := time.Now().Round(time.Microsecond)
	proof.GeneratingSince = &now

	err = a.State.AddBatchProof(ctx, proof, dbTx)
	if err != nil {
		if err := dbTx.Rollback(ctx); err != nil {
			err := fmt.Errorf("failed to rollback proof aggregation state, %w", err)
			log.Error(FirstToUpper(err.Error()))
			return false, err
		}
		err = fmt.Errorf("failed to store the recursive proof, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	err = dbTx.Commit(ctx)
	if err != nil {
		err = fmt.Errorf("failed to store the recursive proof, %w", err)
		log.Error(FirstToUpper(err.Error()))
		return false, err
	}

	// The defer func is useless from now on, use a different variable
	// name for errors (or shadow err in inner scopes) to not trigger it.

	// state is up to date, check if we can send the final proof using the
	// one just crafted.
	finalProofBuilt, finalProofErr := a.tryBuildFinalProof(ctx, prover, proof)
	if finalProofErr != nil {
		// just log the error and continue to handle the aggregated proof
		log.Errorf("Failed trying to check if recursive proof can be verified: %v", finalProofErr)
	}

	// Prover is done, use a.ctx from now on

	if !finalProofBuilt {
		proof.GeneratingSince = nil

		// final proof has not been generated, update the recursive proof
		err := a.State.UpdateBatchProof(a.ctx, proof, nil)
		if err != nil {
			err = fmt.Errorf("failed to store batch proof result, %w", err)
			log.Error(FirstToUpper(err.Error()))
			return false, err
		}
	}

	return true, nil
}

func (a *Aggregator) getAndLockBatchProofsToAggregate(ctx context.Context, prover proverInterface) (*state.BatchProof, *state.BatchProof, error) {
	log := log.WithFields(
		"prover", prover.Name(),
		"proverId", prover.ID(),
		"proverAddr", prover.Addr(),
	)

	a.StateDBMutex.Lock()
	defer a.StateDBMutex.Unlock()

	proof1, proof2, err := a.State.GetBatchProofsToAggregate(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	// Set proofs in generating state in a single transaction
	dbTx, err := a.State.BeginStateTransaction(ctx)
	if err != nil {
		log.Errorf("Failed to begin transaction to set proof aggregation state, err: %v", err)
		return nil, nil, err
	}

	now := time.Now().Round(time.Microsecond)
	proof1.GeneratingSince = &now
	err = a.State.UpdateBatchProof(ctx, proof1, dbTx)
	if err == nil {
		proof2.GeneratingSince = &now
		err = a.State.UpdateBatchProof(ctx, proof2, dbTx)
	}

	if err != nil {
		if err := dbTx.Rollback(ctx); err != nil {
			err := fmt.Errorf("failed to rollback proof aggregation state %w", err)
			log.Error(FirstToUpper(err.Error()))
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("failed to set proof aggregation state %w", err)
	}

	err = dbTx.Commit(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set proof aggregation state %w", err)
	}

	return proof1, proof2, nil
}

func (a *Aggregator) unlockBatchProofsToAggregate(ctx context.Context, proof1 *state.BatchProof, proof2 *state.BatchProof) error {
	// Release proofs from generating state in a single transaction
	dbTx, err := a.State.BeginStateTransaction(ctx)
	if err != nil {
		log.Warnf("Failed to begin transaction to release proof aggregation state, err: %v", err)
		return err
	}

	proof1.GeneratingSince = nil
	err = a.State.UpdateBatchProof(ctx, proof1, dbTx)
	if err == nil {
		proof2.GeneratingSince = nil
		err = a.State.UpdateBatchProof(ctx, proof2, dbTx)
	}

	if err != nil {
		if err := dbTx.Rollback(ctx); err != nil {
			err := fmt.Errorf("failed to rollback proof aggregation state: %w", err)
			log.Error(FirstToUpper(err.Error()))
			return err
		}
		return fmt.Errorf("failed to release proof aggregation state: %w", err)
	}

	err = dbTx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to release proof aggregation state %w", err)
	}

	return nil
}
