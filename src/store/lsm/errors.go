package lsm

import "fmt"

var (
	errMaxFileExceeded = fmt.Errorf("max file exceeded")

	errSnapshotsDisabled = fmt.Errorf("snapshots disabled")

	errCompactionsDisabled = fmt.Errorf("compactions disabled")

	ErrNoValues = fmt.Errorf("no values written")

	ErrTSMClosed = fmt.Errorf("tsm file closed")

	ErrKeyLengthExceeded = fmt.Errorf("key length exceeded")

	ErrMaxBlocksExceeded = fmt.Errorf("max blocks exceeded")
)

type errCompactionInProgress struct {
	err error
}

func (e errCompactionInProgress) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction in progress: %s", e.err)
	}
	return "compaction in progress"
}

type errCompactionAborted struct {
	err error
}

func (e errCompactionAborted) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction aborted: %s", e.err)
	}
	return "compaction aborted"
}
