package pgstatestorage

import (
	"context"
	"errors"

	"github.com/0xPolygonHermez/zkevm-node/state"
	"github.com/jackc/pgx/v4"
)

// GetLastVerifiedBlobInner gets last verified blobInner
func (p *PostgresStorage) GetLastVerifiedBlobInner(ctx context.Context, dbTx pgx.Tx) (*state.BlobInner, error) {
	const query = `
		SELECT bi.block_inner_num, bi.data, bi.block_num, prev_l1_it_root, prev_l1_it_index 
		FROM state.blob_inner bi
		WHERE bi.blob_inner_num = (SELECT blob_inner_num FROM state.verfied_batch ORDER BY blob_inner_num DESC LIMIT 1)
		`

	blobInner := state.BlobInner{}
	e := p.getExecQuerier(dbTx)
	err := e.QueryRow(ctx, query).Scan(&blobInner.BlobInnerNum, &blobInner.Data, &blobInner.BlockNumber, &blobInner.PreviousL1InfoTreeRoot, &blobInner.PreviousL1InfoTreeIndex)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, state.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	return &blobInner, nil
}
