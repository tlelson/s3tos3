package s3tos3

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type uploadProperties struct {
	uploadID string
	numParts int64
	objSize  int64
	duration time.Duration
}

// if errors are returned from this function no action is required.
func (m Mover) requestGenerator(
	returnChan chan response,
	sourceBucket, sourceKey, destBucket, destKey string,
) (up uploadProperties, err error) {
	// Stat the object to make sure its there and get its size
	obj, err := m.S3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: &sourceBucket,
		Prefix: &sourceKey,
	})
	if err != nil {
		return
	}
	if len(obj.Contents) != 1 {
		err = fmt.Errorf("listing object returned %v items", len(obj.Contents))
		return
	}
	objSize := *obj.Contents[0].Size
	// If smaller than MaxChunkSize its fine, it just takes one request is all

	// Register the MPU
	mpu, err := m.S3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: &destBucket,
		Key:    &destKey,
	})
	if err != nil {
		return
	}
	// N.B !! Any errors returned below this need to have the mpu aborted !

	uploadID := *mpu.UploadId
	m.Logger.Printf("%s - Move Started. s3://%s/%s → s3://%s/%s",
		uploadID[:4], sourceBucket, sourceKey, destBucket, destKey)

	chunkSize := int64(m.MultiPartChunkSizeMB << 20)
	numParts := objSize/chunkSize + 1 // ceil
	m.Logger.Printf("%s - number of parts: %d", uploadID[:4], numParts)

	var i int64 // itteration count not part number
	for ; i < numParts; i++ {
		// Set byte range
		low := i * chunkSize
		high := (i+1)*chunkSize - 1
		if high > objSize-1 {
			high = objSize - 1
		}
		byteRange := fmt.Sprintf("bytes=%d-%d", low, high)

		m.partUploadQueue <- request{
			input: &s3.UploadPartCopyInput{
				// Unchanged for item
				UploadId:   mpu.UploadId,
				CopySource: aws.String(fmt.Sprintf("/%s/%s", sourceBucket, sourceKey)),
				Bucket:     &destBucket,
				Key:        &destKey,
				// Part specifications
				PartNumber:      aws.Int64(i + 1),
				CopySourceRange: &byteRange,
			},
			done: returnChan,
		}
	}

	return uploadProperties{uploadID: uploadID, numParts: numParts, objSize: objSize}, err
}

func (m Mover) finalise(uploadID string, numParts int64, responses <-chan response) ([]*s3.CompletedPart, error) {
	var parts = make([]*s3.CompletedPart, numParts)

	var processed int64
	// TODO: Consider changing to enforce a timeout that prints the number of part aggregated every
	// 5 seconds
	for job := range responses {
		if job.err != nil {
			return parts, job.err
		}
		processed++

		// Temp logging
		if processed%10 == 0 {
			fmt.Printf("%s - items processed %d\n", uploadID[:4], processed)
		}

		parts[*job.partNumber-1] = &s3.CompletedPart{
			ETag:       job.output.CopyPartResult.ETag,
			PartNumber: job.partNumber,
		}

		if processed == numParts {
			return parts, nil
		}
	}

	return parts, fmt.Errorf("response channel closed unexpectedly")
}

// Move transfers one s3 file to another s3 location.  It blocks until complete.  It should be
// called on a Mover instatiated with your limits set.  Two Move operations on the same Mover will
// share the set resources.  This is much more efficient than moving two items in series.
func (m Mover) Move(sourceBucket, sourceKey, destBucket, destKey string) error {
	startTime := time.Now()

	// Create a done queue to return finished requests on
	// TODO, consider buffering this channel so that shedulers are unblocked. Would need to work out
	// object size first.
	partDoneQueue := make(chan response)

	// Create the multipart upload and generate all the requests
	props, err := m.requestGenerator(partDoneQueue, sourceBucket, sourceKey, destBucket, destKey)
	if err != nil {
		// MPU never started so no need to abort
		return err
	}
	m.Logger.Printf("%s - requests generated", props.uploadID[:4])

	// Check Responses
	m.Logger.Printf("%s - finaliser starting, looking for %d parts ...", props.uploadID[:4], props.numParts)
	completedParts, err := m.finalise(props.uploadID, props.numParts, partDoneQueue)
	if err != nil {
		m.Logger.Printf("%s - finaliser error! %v", props.uploadID[:4], err)
		return abortMPU(m.S3Client, destBucket, destKey, props.uploadID)
	}
	m.Logger.Printf("%s - responses verified", props.uploadID[:4])

	// CompleteMultipartUpload
	err = completeMPU(m.S3Client, destBucket, destKey, props.uploadID, completedParts)

	// Report stats
	d := time.Since(startTime)
	m.Logger.Printf("%s - move complete s3://%s/%s → s3://%s/%s (%s)",
		props.uploadID[:4],
		sourceBucket, sourceKey,
		destBucket, destKey,
		byteSize(props.objSize),
	)

	// Add the job to the stats for the mover
	props.duration = d
	m.saveStats <- props

	return err
}

// completeMPU attempts to complete the MultiPart Upload 4 times using exponential backoff after
// each failed attempt.
// This may be nessisary if the sender has gotten our HTTP session throttled or dropped by the
// server.
// If the 4 attempts fail, the final error is returned.
func completeMPU(s3Client s3iface.S3API, bucket, key, uploadID string, completedParts []*s3.CompletedPart) error {
	var err error
	for i := 1; i <= 4; i++ {
		_, err = s3Client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:          &bucket,
			Key:             &key,
			UploadId:        &uploadID,
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
		})
		if err == nil {
			return nil
		}
		time.Sleep(time.Second << i)
	}
	return fmt.Errorf("%s - could not complete MPU after 4 attempts. %v",
		uploadID[:4], err)
}

// Attempts to abort 4 times with exponential backoff after each failed attempt.  If the 4 attempts
// fail, the final error is returned.
func abortMPU(s3Client s3iface.S3API, bucket, key, uploadID string) error {
	var partsInTransit int
	var err error

	// Try and abort 4 times with increasing wait times.  After 4 attempts fail with error
	for i := 1; i <= 4; i++ {
		var res *s3.ListPartsOutput

		_, err = s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   &bucket,
			Key:      &key,
			UploadId: &uploadID,
		})
		if err != nil {
			goto Pause
		}

		// Ensure the Parts List is empty or charges will accrue
		res, err = s3Client.ListParts(&s3.ListPartsInput{
			Bucket:   &bucket,
			Key:      &key,
			UploadId: &uploadID,
		})
		if err != nil {
			goto Pause
		}
		partsInTransit = len(res.Parts)

		if partsInTransit == 0 {
			return nil // Success
		}
	Pause:
		time.Sleep(time.Second << i) // Wait to let pending part transfer complete
	}

	return fmt.Errorf("%s - could not abort MPU after 4 attempts. %d parts still in progress. %v",
		uploadID[:4], partsInTransit, err)
}
