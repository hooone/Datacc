package lsm

import "fmt"

func formatFileName(generation, sequence int) string {
	return fmt.Sprintf("%09d-%09d", generation, sequence) + "." + TSMFileExtension + "." + CompactionTempExtension
}
