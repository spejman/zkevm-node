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

func (a *Aggregator) tryGenerateBlobOuterProof(ctx context.Context, prover proverInterface) (bool, error) {
	log.Debug("tryGenerateBlobOuterProof started, prover: %s, proverId: %s", prover.Name(), prover.ID())

	handleError := func(blobOuterNumber uint64, logError error) (bool, error) {
		log.Errorf(logError.Error())

		err := a.State.DeleteBlobOuterProof(a.ctx, blobOuterNumber, blobOuterNumber, nil)
		if err != nil {
			log.Errorf("failed to delete blobOuter %d proof in progress, error: %v", blobOuterNumber, err)
		}

		log.Debug("tryGenerateBlobOuterProof ended with errors")

		return false, logError
	}

	batchProof, blobInnerProof, blobOuterProof, err := a.getAndLockBlobOuterToProve(ctx, prover)
	if errors.Is(err, state.ErrNotFound) {
		log.Debug("no blobOuter pending to generate proof")
		return false, nil
	}
	if err != nil {
		return false, err
	}

	log.Info("generating proof for blobOuter %d", blobOuterProof.BlobOuterNumber)

	inputProver := map[string]interface{}{
		"batch_proof":      batchProof.Data,
		"blob_inner_proof": blobInnerProof.Data,
	}
	b, err := json.Marshal(inputProver)
	if err != nil {
		err = fmt.Errorf("failed to serialize blobOuter %d proof request, error: %v", blobOuterProof.BlobOuterNumber, err)
		return handleError(blobOuterProof.BlobOuterNumber, err)
	}

	blobOuterProof.InputProver = string(b)

	proofId, err := prover.BlobOuterProof(batchProof.Data, blobInnerProof.Data)
	if err != nil {
		err = fmt.Errorf("failed to get blobOuter %d proof id, error: %v", blobOuterProof.BlobOuterNumber, err)
		return handleError(blobOuterProof.BlobOuterNumber, err)
	}

	blobOuterProof.Id = proofId

	log.Infof("generating blobOuter %d proof, Id: %s", blobOuterProof.BlobOuterNumber, blobOuterProof.Id)

	proofData, err := prover.WaitRecursiveProof(ctx, *blobOuterProof.Id)
	if err != nil {
		err = fmt.Errorf("failed to get blobOuter proof id %s from prover, error: %v", blobInnerProof.Id, err)
		return handleError(blobOuterProof.BlobOuterNumber, err)
	}

	log.Info("generated blobOuter %d proof, Id: %s", blobOuterProof.BlobOuterNumber, blobOuterProof.Id)

	blobOuterProof.Data = proofData
	blobOuterProof.GeneratingSince = nil

	err = a.State.UpdateBlobOuterProof(a.ctx, blobOuterProof, nil)
	if err != nil {
		err = fmt.Errorf("failed to store blobOuter %d proof, Id: %s, error: %v", blobOuterProof.BlobOuterNumber, blobOuterProof.Id, err)
		return handleError(blobOuterProof.BlobOuterNumber, err)
	}

	log.Debug("tryGenerateBlobOuterProof ended")

	return true, nil
}

func (a *Aggregator) getAndLockBlobOuterToProve(ctx context.Context, prover proverInterface) (*state.BatchProof, *state.BlobInnerProof, *state.BlobOuterProof, error) {
	proverID := prover.ID()
	proverName := prover.Name()

	a.StateDBMutex.Lock()
	defer a.StateDBMutex.Unlock()

	// Get blobOuter pending to generate proof
	batchProof, blobInnerProof, err := a.State.GetBlobOuterToProve(ctx, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	log.Infof("found blobOuter %d (batchProof[%d-%d]) pending to generate proof", blobInnerProof.BlobInnerNumber, batchProof.BatchNumber, batchProof.BatchNumberFinal)

	now := time.Now().Round(time.Microsecond)
	blobOuterProof := &state.BlobOuterProof{
		BlobOuterNumber:      blobInnerProof.BlobInnerNumber,
		BlobOuterNumberFinal: blobInnerProof.BlobInnerNumber,
		Proof: state.Proof{
			Prover:          &proverName,
			ProverID:        &proverID,
			GeneratingSince: &now,
		},
	}

	// Avoid other prover to process the same blobInner
	err = a.State.AddBlobOuterProof(ctx, blobOuterProof, nil)
	if err != nil {
		log.Errorf("failed to add blobOuter %d proof, err: %v", blobOuterProof.BlobOuterNumber, err)
		return nil, nil, nil, err
	}

	return batchProof, blobInnerProof, blobOuterProof, nil
}

func (a *Aggregator) tryAggregateBlobOuterProofs(ctx context.Context, prover proverInterface) (bool, error) {
	return true, nil
}
