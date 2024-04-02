package pgstatestorage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/state"
	"github.com/jackc/pgx/v4"
)

// CheckProofContainsCompleteSequences checks if a recursive proof contains complete sequences
func (p *PostgresStorage) CheckProofContainsCompleteSequences(ctx context.Context, proof *state.BatchProof, dbTx pgx.Tx) (bool, error) {
	const getProofContainsCompleteSequencesSQL = `
		SELECT EXISTS (SELECT 1 FROM state.sequences s1 WHERE s1.from_batch_num = $1) AND
			   EXISTS (SELECT 1 FROM state.sequences s2 WHERE s2.to_batch_num = $2)
		`
	e := p.getExecQuerier(dbTx)
	var exists bool
	err := e.QueryRow(ctx, getProofContainsCompleteSequencesSQL, proof.BatchNumber, proof.BatchNumberFinal).Scan(&exists)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return exists, err
	}
	return exists, nil
}

// GetVirtualBatchToProve return the next batch that is not proved, neither in proved process.
func (p *PostgresStorage) GetVirtualBatchToProve(ctx context.Context, lastVerfiedBatchNumber uint64, maxL1Block uint64, dbTx pgx.Tx) (*state.Batch, error) {
	const query = `
		SELECT
			b.batch_num, b.global_exit_root, b.local_exit_root,	b.acc_input_hash, b.state_root,	v.timestamp_batch_etrog, b.coinbase, b.raw_txs_data, b.forced_batch_num, b.batch_resources, b.wip
		FROM
			state.batch b,
			state.virtual_batch v
		WHERE
			b.batch_num > $1 AND b.batch_num = v.batch_num AND
			v.block_num <= $2 AND
			NOT EXISTS (
				SELECT p.batch_num FROM state.batch_proof p 
				WHERE v.batch_num >= p.batch_num AND v.batch_num <= p.batch_num_final
			)
		ORDER BY b.batch_num ASC LIMIT 1
		`
	e := p.getExecQuerier(dbTx)
	row := e.QueryRow(ctx, query, lastVerfiedBatchNumber, maxL1Block)
	batch, err := scanBatch(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, state.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return &batch, nil
}

// GetProofReadyForFinal return the proof that is ready to generate the final proof
func (p *PostgresStorage) GetProofReadyForFinal(ctx context.Context, lastVerfiedBatchNumber uint64, dbTx pgx.Tx) (*state.BatchProof, error) {
	const getProofReadyForFinalSQL = `
		SELECT 
			p.batch_num, p.batch_num_final, p.proof, p.proof_id, p.input_prover, p.prover, p.prover_id, p.generating_since, p.created_at, p.updated_at
		FROM state.batch_proof p
		WHERE batch_num = $1 AND generating_since IS NULL AND
			EXISTS (SELECT 1 FROM state.sequences s1 WHERE s1.from_batch_num = p.batch_num) AND
			EXISTS (SELECT 1 FROM state.sequences s2 WHERE s2.to_batch_num = p.batch_num_final)		
		`

	proof := &state.BatchProof{}

	e := p.getExecQuerier(dbTx)
	row := e.QueryRow(ctx, getProofReadyForFinalSQL, lastVerfiedBatchNumber+1)
	err := row.Scan(&proof.BatchNumber, &proof.BatchNumberFinal, &proof.Data, &proof.Id, &proof.InputProver, &proof.Prover, &proof.ProverID, &proof.GeneratingSince, &proof.CreatedAt, &proof.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, state.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	return proof, err
}

// GetBatchProofsToAggregate return the next 2 batch proofs that it are possible to aggregate
func (p *PostgresStorage) GetBatchProofsToAggregate(ctx context.Context, dbTx pgx.Tx) (*state.BatchProof, *state.BatchProof, error) {
	// TODO: add comments to explain the query
	const getBatchProofsToAggregateSQL = `
		SELECT 
			p1.batch_num, p1.batch_num_final, p1.proof, p1.proof_id, p1.input_prover, p1.prover, p1.prover_id, p1.generating_since,	p1.created_at, p1.updated_at,
			p2.batch_num, p2.batch_num_final, p2.proof,	p2.proof_id, p2.input_prover, p2.prover, p2.prover_id, p2.generating_since, p2.created_at, p2.updated_at
		FROM state.batch_proof p1 INNER JOIN state.batch_proof p2 ON p1.batch_num_final = p2.batch_num - 1
		WHERE p1.blob_inner_num = p2.blob_inner_num AND
			  p1.generating_since IS NULL AND p2.generating_since IS NULL AND 
		 	  p1.proof IS NOT NULL AND p2.proof IS NOT NULL
		ORDER BY p1.batch_num ASC
		LIMIT 1
		`

	proof1 := &state.BatchProof{}
	proof2 := &state.BatchProof{}

	e := p.getExecQuerier(dbTx)
	row := e.QueryRow(ctx, getBatchProofsToAggregateSQL)
	err := row.Scan(
		&proof1.BatchNumber, &proof1.BatchNumberFinal, &proof1.Data, &proof1.Id, &proof1.InputProver, &proof1.Prover, &proof1.ProverID, &proof1.GeneratingSince, &proof1.CreatedAt, &proof1.UpdatedAt,
		&proof2.BatchNumber, &proof2.BatchNumberFinal, &proof2.Data, &proof2.Id, &proof2.InputProver, &proof2.Prover, &proof2.ProverID, &proof2.GeneratingSince, &proof2.CreatedAt, &proof2.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, state.ErrNotFound
	} else if err != nil {
		return nil, nil, err
	}

	return proof1, proof2, err
}

// AddBatchProof adds a batch proof to the storage
func (p *PostgresStorage) AddBatchProof(ctx context.Context, proof *state.BatchProof, dbTx pgx.Tx) error {
	const addBatchProofSQL = "INSERT INTO state.batch_proof (batch_num, batch_num_final, proof, proof_id, input_prover, prover, prover_id, generating_since, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	e := p.getExecQuerier(dbTx)
	now := time.Now().UTC().Round(time.Microsecond)
	_, err := e.Exec(ctx, addBatchProofSQL, proof.BatchNumber, proof.BatchNumberFinal, proof.Data, proof.Id, proof.InputProver, proof.Prover, proof.ProverID, proof.GeneratingSince, now, now)
	return err
}

// UpdateBatchProof updates a batch proof in the storage
func (p *PostgresStorage) UpdateBatchProof(ctx context.Context, proof *state.BatchProof, dbTx pgx.Tx) error {
	const addBatchProofSQL = "UPDATE state.batch_proof SET proof = $3, proof_id = $4, input_prover = $5, prover = $6, prover_id = $7, generating_since = $8, updated_at = $9 WHERE batch_num = $1 AND batch_num_final = $2"
	e := p.getExecQuerier(dbTx)
	now := time.Now().UTC().Round(time.Microsecond)
	_, err := e.Exec(ctx, addBatchProofSQL, proof.BatchNumber, proof.BatchNumberFinal, proof.Data, proof.Id, proof.InputProver, proof.Prover, proof.ProverID, proof.GeneratingSince, now)
	return err
}

// DeleteBatchProofs deletes from the storage the batch proofs falling inside the batch numbers range.
func (p *PostgresStorage) DeleteBatchProofs(ctx context.Context, batchNumber uint64, batchNumberFinal uint64, dbTx pgx.Tx) error {
	const deleteBatchProofSQL = "DELETE FROM state.batch_proof WHERE batch_num >= $1 AND batch_num_final <= $2"
	e := p.getExecQuerier(dbTx)
	_, err := e.Exec(ctx, deleteBatchProofSQL, batchNumber, batchNumberFinal)
	return err
}

// CleanupBatchProofs deletes from the storage the batch proofs up to the specified batch number included.
func (p *PostgresStorage) CleanupBatchProofs(ctx context.Context, batchNumber uint64, dbTx pgx.Tx) error {
	const deleteBatchProofSQL = "DELETE FROM state.batch_proof WHERE batch_num_final <= $1"
	e := p.getExecQuerier(dbTx)
	_, err := e.Exec(ctx, deleteBatchProofSQL, batchNumber)
	return err
}

// CleanupLockedBatchProofs deletes from the storage the proofs locked in generating state for more than the provided threshold.
func (p *PostgresStorage) CleanupLockedBatchProofs(ctx context.Context, duration string, dbTx pgx.Tx) (int64, error) {
	interval, err := toPostgresInterval(duration)
	if err != nil {
		return 0, err
	}
	sql := fmt.Sprintf("DELETE FROM state.batch_proof WHERE generating_since < (NOW() - interval '%s')", interval)
	e := p.getExecQuerier(dbTx)
	ct, err := e.Exec(ctx, sql)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

// DeleteUngeneratedBatchProofs deletes ungenerated proofs. This method is meant to be use during aggregator boot-up sequence
func (p *PostgresStorage) DeleteUngeneratedBatchProofs(ctx context.Context, dbTx pgx.Tx) error {
	const deleteUngeneratedProofsSQL = "DELETE FROM state.batch_proof WHERE generating_since IS NOT NULL"
	e := p.getExecQuerier(dbTx)
	_, err := e.Exec(ctx, deleteUngeneratedProofsSQL)
	return err
}

// GetBlobInnerToProve returns the next blobInner that is not proved, neither in proved process.
func (p *PostgresStorage) GetBlobInnerToProve(ctx context.Context, lastVerfiedBlobInnerNumber uint64, maxL1Block uint64, dbTx pgx.Tx) (*state.BlobInner, error) {
	const query = `
		SELECT
			bi.blob_inner_num, bi.data,	bi.block_num
		FROM
			state.blob_inner bi
		WHERE
			bi.blob_inner_num > $1 AND bi.block_num <= $2 AND
			NOT EXISTS ( -- check that we are not already generating a proof for the blobInner (bi)
				SELECT p.blob_inner_num FROM state.blob_inner_proof p 
				WHERE p.blob_inner_num = bi.blob_inner_num
			)
		ORDER BY bi.blob_inner_num ASC LIMIT 1
		`

	blobInner := &state.BlobInner{}

	e := p.getExecQuerier(dbTx)
	row := e.QueryRow(ctx, query, lastVerfiedBlobInnerNumber, maxL1Block)
	err := row.Scan(&blobInner.BlobInnerNum, &blobInner.Data, &blobInner.BlockNumber)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, state.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return blobInner, nil
}

// AddBlobInnerProof adds a blobInner proof to the storage
func (p *PostgresStorage) AddBlobInnerProof(ctx context.Context, proof *state.BlobInnerProof, dbTx pgx.Tx) error {
	const addBlobInnerProofSQL = "INSERT INTO state.blob_inner_proof (blob_inner_num, proof, proof_id, input_prover, prover, prover_id, generating_since, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	e := p.getExecQuerier(dbTx)
	now := time.Now().UTC().Round(time.Microsecond)
	_, err := e.Exec(ctx, addBlobInnerProofSQL, proof.BlobInnerNumber, proof.Data, proof.Id, proof.InputProver, proof.Prover, proof.ProverID, proof.GeneratingSince, now, now)
	return err
}

// UpdateBlobInnerProof updates a blobInner proof in the storage
func (p *PostgresStorage) UpdateBlobInnerProof(ctx context.Context, proof *state.BlobInnerProof, dbTx pgx.Tx) error {
	const updateBlobInnerProofSQL = "UPDATE state.blob_inner_proof SET proof = $2, proof_id = $3, input_prover = $4, prover = $5, prover_id = $6, generating_since = $7, updated_at = $8 WHERE blob_inner_num = $1"
	e := p.getExecQuerier(dbTx)
	now := time.Now().UTC().Round(time.Microsecond)
	_, err := e.Exec(ctx, updateBlobInnerProofSQL, proof.BlobInnerNumber, proof.Data, proof.Id, proof.InputProver, proof.Prover, proof.ProverID, proof.GeneratingSince, now)
	return err
}

// DeleteBlobInnerProof deletes from the storage the blobInner proof
func (p *PostgresStorage) DeleteBlobInnerProof(ctx context.Context, blobInnerNumber uint64, dbTx pgx.Tx) error {
	const deleteBlobInnerProofSQL = "DELETE FROM state.blob_inner_proof WHERE blob_inner_num = $1"
	e := p.getExecQuerier(dbTx)
	_, err := e.Exec(ctx, deleteBlobInnerProofSQL, blobInnerNumber)
	return err
}

// GetBlobOuterToProve returns the next blobOuter that is not proved, neither in proved process.
func (p *PostgresStorage) GetBlobOuterToProve(ctx context.Context, dbTx pgx.Tx) (*state.BatchProof, *state.BlobInnerProof, error) {
	const query = `
		SELECT
			bp.batch_num, bp.batch_num_final, bp.proof_id, bp.proof, bp.prover, bp.prover_id, bp.prover_input, bp.generating_since, bp.created_at, bp.updated_at,
			bip.blob_inner_num, bip.proof_id, bip.proof, bip.prover, bip.prover_id, bip.prover_input, bip.generating_since, bip.created_at,	bip.updated_at
		FROM
			state.batch_proof bp, state.blob_inner_proof bip
		WHERE
			bip.generating_since IS NULL AND bip.proof IS NOT NULL AND -- select only blobInner proofs that are finished
			bp.generating_since IS NULL and bp.proof IS NOT NULL AND -- select only batch proofs that are finished
			NOT EXISTS ( -- select only blobInner proofs that are not already generating an outer proof (blobInner + batch proofs)
				SELECT bop.blob_outer_num FROM state.blob_outer_proof bop 
				WHERE bop.blob_outer_num = bip.blob_inner_num
			) AND ( -- select only a batch proof aggregation that includes all the batches of the blobInner
				bp.batch_num = (SELECT MIN(batch_num) from state.virtual_batch vb WHERE vb.blob_inner_num = bip.blob_inner_num) AND
				bp.batch_num_final = (SELECT MAX(batch_num) from state.virtual_batch vb WHERE vb.blob_inner_num = bip.blob_inner_num)
			)
		ORDER BY bip.blob_inner_num ASC LIMIT 1
		`

	bProof := &state.BatchProof{}
	biProof := &state.BlobInnerProof{}

	e := p.getExecQuerier(dbTx)
	row := e.QueryRow(ctx, query)
	err := row.Scan(
		&bProof.BatchNumber, &bProof.BatchNumberFinal, &bProof.Id, &bProof.Data, &bProof.Prover, &bProof.ProverID, &bProof.InputProver, &bProof.GeneratingSince, &bProof.CreatedAt, &bProof.UpdatedAt,
		&biProof.BlobInnerNumber, &biProof.Id, &biProof.Data, &biProof.Prover, &biProof.ProverID, &biProof.InputProver, &biProof.GeneratingSince, &biProof.CreatedAt, &biProof.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, state.ErrNotFound
	} else if err != nil {
		return nil, nil, err
	}

	return bProof, biProof, err
}

// AddBlobOuterProof adds a blobOuter proof to the storage
func (p *PostgresStorage) AddBlobOuterProof(ctx context.Context, proof *state.BlobOuterProof, dbTx pgx.Tx) error {
	const addBlobOuterProofSQL = "INSERT INTO state.blob_outer_proof (blob_outer_num, blob_outer_num_final, proof, proof_id, input_prover, prover, prover_id, generating_since, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	e := p.getExecQuerier(dbTx)
	now := time.Now().UTC().Round(time.Microsecond)
	_, err := e.Exec(ctx, addBlobOuterProofSQL, proof.BlobOuterNumber, proof.BlobOuterNumberFinal, proof.Data, proof.Id, proof.InputProver, proof.Prover, proof.ProverID, proof.GeneratingSince, now, now)
	return err
}

// UpdateBlobOuterProof updates a blobOuter proof in the storage
func (p *PostgresStorage) UpdateBlobOuterProof(ctx context.Context, proof *state.BlobOuterProof, dbTx pgx.Tx) error {
	const updateBlobOuterProofSQL = "UPDATE state.blob_outer_proof SET proof = $2, proof_id = $3, input_prover = $4, prover = $5, prover_id = $6, generating_since = $7, updated_at = $8 WHERE blob_outer_num = $1 AND blob_outer_num_final = $2"
	e := p.getExecQuerier(dbTx)
	now := time.Now().UTC().Round(time.Microsecond)
	_, err := e.Exec(ctx, updateBlobOuterProofSQL, proof.BlobOuterNumber, proof.BlobOuterNumberFinal, proof.Data, proof.Id, proof.InputProver, proof.Prover, proof.ProverID, proof.GeneratingSince, now)
	return err
}

// DeleteBlobOuterProof deletes from the storage the blobOuter proof
func (p *PostgresStorage) DeleteBlobOuterProof(ctx context.Context, blobOuterNumber uint64, blobOuterNumberFinal uint64, dbTx pgx.Tx) error {
	const deleteBlobInnerProofSQL = "DELETE FROM state.blob_outer_proof WHERE blob_outer_num = $1 AND blob_outer_num_final = $2"
	e := p.getExecQuerier(dbTx)
	_, err := e.Exec(ctx, deleteBlobInnerProofSQL, blobOuterNumber, blobOuterNumberFinal)
	return err
}

func toPostgresInterval(duration string) (string, error) {
	unit := duration[len(duration)-1]
	var pgUnit string

	switch unit {
	case 's':
		pgUnit = "second"
	case 'm':
		pgUnit = "minute"
	case 'h':
		pgUnit = "hour"
	default:
		return "", state.ErrUnsupportedDuration
	}

	isMoreThanOne := duration[0] != '1' || len(duration) > 2 //nolint:gomnd
	if isMoreThanOne {
		pgUnit = pgUnit + "s"
	}

	return fmt.Sprintf("%s %s", duration[:len(duration)-1], pgUnit), nil
}
