package state

import "github.com/0xPolygonHermez/zkevm-node/log"

// ForkIDInterval is a fork id interval
type ForkIDInterval struct {
	FromBatchNumber uint64
	ToBatchNumber   uint64
	ForkId          uint64
	Version         string
}

// UpdateForkIDIntervals updates the forkID intervals
func (s *State) UpdateForkIDIntervals(intervals []ForkIDInterval) {
	log.Infof("Updating forkIDs. Setting %d forkIDs", len(intervals))
	log.Infof("intervals: %#v", intervals)
	s.cfg.ForkIDIntervals = intervals
}

// GetForkIDByBatchNumber returns the fork id for a given batch number
func GetForkIDByBatchNumber(intervals []ForkIDInterval, batchNumber uint64) uint64 {
	for _, interval := range intervals {
		if batchNumber >= interval.FromBatchNumber && batchNumber <= interval.ToBatchNumber {
			return interval.ForkId
		}
	}

	// If not found return the last fork id
	return intervals[len(intervals)-1].ForkId
}

// GetForkIDByBatchNumber returns the fork id for the given batch number
func (s *State) GetForkIDByBatchNumber(batchNumber uint64) uint64 {
	return GetForkIDByBatchNumber(s.cfg.ForkIDIntervals, batchNumber)
}
