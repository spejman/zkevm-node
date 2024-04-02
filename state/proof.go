package state

import "time"

// Proof struct
type Proof struct {
	Id              *string
	Data            string
	Prover          *string
	ProverID        *string
	InputProver     string
	GeneratingSince *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// BatchProof struct
type BatchProof struct {
	BatchNumber      uint64
	BatchNumberFinal uint64
	Proof
}

type BlobInnerProof struct {
	BlobInnerNumber uint64
	Proof
}

type BlobOuterProof struct {
	BlobOuterNumber      uint64
	BlobOuterNumberFinal uint64
	Proof
}
