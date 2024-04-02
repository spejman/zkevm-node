package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/log"
	"github.com/0xPolygonHermez/zkevm-node/state"
)

func (a *Aggregator) tryGenerateBlobInnerProof(ctx context.Context, prover proverInterface) (bool, error) {
	log.Debug("tryGenerateBlobInnerProof started, prover: %s, proverId: %s", prover.Name(), prover.ID())

	handleError := func(blobInnerNum uint64, logError error) (bool, error) {
		log.Errorf(logError.Error())

		err := a.State.DeleteBlobInnerProof(a.ctx, blobInnerNum, nil)
		if err != nil {
			log.Errorf("failed to delete blobInner %d proof in progress, error: %v", blobInnerNum, err)
		}

		log.Debug("tryGenerateBlobInnerProof ended with errors")

		return false, logError
	}

	blobInnerToProve, blobInnerProof, err := a.getAndLockBlobInnerToProve(ctx, prover)
	if errors.Is(err, state.ErrNotFound) {
		log.Debug("no blobInner pending to generate proof")
		return false, nil
	}
	if err != nil {
		return false, err
	}

	log.Info("generating proof for blobInner %d", blobInnerToProve.BlobInnerNum)

	inputProver, err := a.buildBlobInnerProverInputs(ctx, blobInnerToProve)
	if err != nil {
		err := fmt.Errorf("failed to build prover blobInner %d proof request, error: %v", blobInnerToProve.BlobInnerNum, err)
		return handleError(blobInnerToProve.BlobInnerNum, err)
	}

	b, err := json.Marshal(inputProver)
	if err != nil {
		err = fmt.Errorf("failed to serialize prover request, error: %v", err)
		return handleError(blobInnerToProve.BlobInnerNum, err)
	}

	blobInnerProof.InputProver = string(b)

	log.Infof("sending blobInner %d proof request to the prover", blobInnerToProve.BlobInnerNum)

	proofId, err := prover.BlobInnerProof(inputProver)
	if err != nil {
		err = fmt.Errorf("failed to get blobInner %d proof id, error: %v", blobInnerToProve.BlobInnerNum, err)
		return handleError(blobInnerToProve.BlobInnerNum, err)
	}

	blobInnerProof.Id = proofId

	log.Infof("generating blobInner %d proof, Id: %s", blobInnerToProve.BlobInnerNum, *blobInnerProof.Id)

	proofData, err := prover.WaitRecursiveProof(ctx, *blobInnerProof.Id)
	if err != nil {
		err = fmt.Errorf("failed to get blobInner proof id %s from prover, error: %v", blobInnerProof.Id, err)
		return handleError(blobInnerToProve.BlobInnerNum, err)
	}

	log.Info("generated blobInner %d proof, Id: %s", blobInnerToProve.BlobInnerNum, *blobInnerProof.Id)

	blobInnerProof.Data = proofData
	blobInnerProof.GeneratingSince = nil

	err = a.State.UpdateBlobInnerProof(a.ctx, blobInnerProof, nil)
	if err != nil {
		err = fmt.Errorf("failed to store blobInner %d proof, error: %v", blobInnerToProve.BlobInnerNum, err)
		return handleError(blobInnerToProve.BlobInnerNum, err)
	}

	log.Debug("tryGenerateBlobInnerProof ended")

	return true, nil
}

func (a *Aggregator) getAndLockBlobInnerToProve(ctx context.Context, prover proverInterface) (*state.BlobInner, *state.BlobInnerProof, error) {
	proverID := prover.ID()
	proverName := prover.Name()

	a.StateDBMutex.Lock()
	defer a.StateDBMutex.Unlock()

	lastVerifiedBlobInner, err := a.State.GetLastVerifiedBlobInner(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	// Get max L1 block number for getting next blobInner to prove
	maxL1BlockNumber, err := a.getMaxL1BlockNumber(ctx)
	if err != nil {
		log.Errorf("failed to get max L1 block number, error: %v", err)
		return nil, nil, err
	}

	log.Debugf("max L1 block number for getting next blobInner to prove: %d", maxL1BlockNumber)

	// Get blobInner pending to generate proof
	blobInnerToProve, err := a.State.GetBlobInnerToProve(ctx, lastVerifiedBlobInner.BlobInnerNum, maxL1BlockNumber, nil)
	if err != nil {
		return nil, nil, err
	}

	log.Infof("found blobInner %d pending to generate proof", lastVerifiedBlobInner.BlobInnerNum)

	now := time.Now().Round(time.Microsecond)
	proof := &state.BlobInnerProof{
		BlobInnerNumber: blobInnerToProve.BlobInnerNum,
		Proof: state.Proof{
			Prover:          &proverName,
			ProverID:        &proverID,
			GeneratingSince: &now,
		},
	}

	// Avoid other prover to process the same blobInner
	err = a.State.AddBlobInnerProof(ctx, proof, nil)
	if err != nil {
		log.Errorf("failed to add blobInner %d proof, error: %v", proof.BlobInnerNumber, err)
		return nil, nil, err
	}

	return blobInnerToProve, proof, nil
}
