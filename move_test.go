package s3tos3_test

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/tlelson/s3tos3"
)

type logger struct{}

func (logger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// Error should call Abort

// What happens for small files?!

func Test_Move(t *testing.T) {
	mS3 := newMockS3(11 << 20) // 11 MiB
	mover := s3tos3.Mover{
		MultiPartChunkSizeMB: 5,
		S3Client:             &mS3,
		Logger:               logger{},
	}

	var expectedParts int64 = 3

	mover.Start()
	defer mover.Stop()
	fmt.Println("TEST:  Preparing to move... ")

	err := mover.Move("srcBucket", "srcKey", "dstBucket", "dstKey")
	if err != nil {
		t.Error("error received from Move op: ", err)
	}

	if mS3.aborted {
		t.Errorf("unexpected abort")
	}

	// Complete Copy 1
	if !strings.HasPrefix(mS3.partRanges.parts[1], "bytes=0") {
		t.Error("incomplete copy! first part must start at byte 0", mS3.partRanges.parts[1])
	}

	// Complete Copy 2
	if !strings.HasSuffix(mS3.partRanges.parts[expectedParts], fmt.Sprintf("%d", mS3.objSize-1)) {
		t.Error("incomplete copy! last part must end at complete range", mS3.partRanges.parts[expectedParts])
	}

	if want := 1 + int(mS3.objSize)/int(mover.MultiPartChunkSizeMB<<20); len(mS3.partRanges.parts) != want {
		t.Errorf("expected %d parts, got %d", want, len(mS3.partRanges.parts))
	}

	// Each are 5MiB difference between high and low
	// First starts with 0
	// Last ends with 11MiB
	//
	// Part 1: 0 → ChunkSize-1
	// Part 2: ChunkSize → 2*ChunkSize -1
	// Part 2: 2*ChunkSize → objectSize
	//
	var expectedLow, expectedHigh int64
	for k, byteRange := range mS3.partRanges.parts {
		// Set expectations
		expectedLow = (k - 1) * int64(mover.MultiPartChunkSizeMB<<20)
		expectedHigh = (k)*int64(mover.MultiPartChunkSizeMB<<20) - 1
		if expectedHigh > mS3.objSize-1 { // If this is the final part its size should be smaller
			expectedHigh = mS3.objSize - 1
		}

		// Make assertions
		low, high := strConv(byteRange)

		if (low == nil || high == nil) || // Completely Malformed
			(expectedLow != *low) || // Bad start offset
			(expectedHigh != *high) { // Bad end offset
			t.Errorf("part %d - Expected '%d-%d'. Got: %s", k, expectedLow, expectedHigh, byteRange)
		}
	}

	if !mS3.completed {
		t.Errorf("Multipart transfer not Completed")
	}

	fmt.Println("Test DONE")
}

// strConv converts the standard HTTP ContentRange string format to its high and low byte offsets
// returning nil if unable to do so.
func strConv(byteRange string) (low, high *int64) {

	r := regexp.MustCompile(`bytes=(\d+)-(\d+)`)

	matches := r.FindStringSubmatch(byteRange)
	if len(matches) < 2 {
		return // Should always be at least 3, or zero
	}

	if l, err := strconv.ParseInt(matches[1], 10, 32); err == nil {
		low = &l
	}

	if h, err := strconv.ParseInt(matches[2], 10, 32); err == nil {
		high = &h
	}

	return
}
