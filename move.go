package s3tos3

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// if errors are returned from this function no action is required.
func (m *Mover) requestGenerator(
	returnChan chan response,
	sourceBucket, sourceKey, destBucket, destKey string,
) (uploadID string, numParts int64, err error) {
	// Stat the object to make sure its there and get its size
	obj, err := m.s3Client().ListObjectsV2(&s3.ListObjectsV2Input{
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
	m.Logger.Printf("object size: %d", objSize)
	// TODO: What if smaller than 5MB ?!

	// Register the MPU
	mpu, err := m.s3Client().CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: &destBucket,
		Key:    &destKey,
	})
	if err != nil {
		return
	}
	// N.B !! Any errors returned below this need to have the mpu aborted !

	uploadID = *mpu.UploadId
	m.Logger.Printf("uploadID: %s", uploadID)

	chunkSize := int64(m.MultiPartChunkSizeMB << 20)
	numParts = objSize/chunkSize + 1 // ceil
	m.Logger.Printf("number of parts: %d", numParts)

	var i int64 // itteration count not part number
	for ; i < numParts; i++ {
		// Set byte range
		low := i * chunkSize
		high := (i+1)*chunkSize - 1
		if high > objSize-1 {
			high = objSize - 1
		}
		byteRange := fmt.Sprintf("bytes=%d-%d", low, high)
		m.Logger.Printf("part %d, sends range: %s", i+1, byteRange)

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
	m.Logger.Printf("All requests sent")

	return
}

func (m Mover) finalise(uploadID string, numParts int64, responses <-chan response) ([]*s3.CompletedPart, error) {
	var parts = make([]*s3.CompletedPart, numParts)
	m.Logger.Printf("finaliser started looking for %d parts ...", numParts)

	var processed int64
	for job := range responses {
		m.Logger.Printf("received response: part %d\n", *job.partNumber)
		if job.err != nil {
			m.Logger.Printf("finaliser error! job: %s. %v", job.output.GoString(), job.err)
			return parts, job.err
		}
		processed++
		m.Logger.Printf("processed part number %d in response %d", *job.partNumber, processed)
		parts[*job.partNumber-1] = &s3.CompletedPart{
			ETag:       job.output.CopyPartResult.ETag,
			PartNumber: job.partNumber,
		}

		if processed == numParts {
			m.Logger.Printf("finaliser Complete!")
			return parts, nil
		}
	}

	return parts, fmt.Errorf("response channel closed unexpectedly")
}

// Move transfers one s3 file to another s3 location.  It blocks until complete.  It should be
// called on a Mover instatiated with your limits set.  Two Move operations on the same Mover will
// share the set resources.  This is much more efficient than moving two items in series.
func (m Mover) Move(sourceBucket, sourceKey, destBucket, destKey string) error {
	// Create a done queue to return finished requests on
	partDoneQueue := make(chan response)
	defer close(partDoneQueue)
	m.Logger.Printf("done queue made")

	// Create the multipart upload and generate all the requests
	uploadID, numParts, err := m.requestGenerator(partDoneQueue, sourceBucket, sourceKey, destBucket, destKey)
	if err != nil {
		// MPU never started so no need to abort
		return err
	}
	m.Logger.Printf("requests generated")

	// Check Responses
	completedParts, err := m.finalise(uploadID, numParts, partDoneQueue)
	if err != nil {
		return abortMPU(m.s3Client(), destBucket, destKey, uploadID)
	}
	m.Logger.Printf("responses verified")

	// CompleteMultipartUpload
	return completeMPU(m.s3Client(), destBucket, destKey, uploadID, completedParts)
}

func completeMPU(s3Client s3iface.S3API, bucket, key, uploadID string, completedParts []*s3.CompletedPart) error {
	_, err := s3Client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
	})
	return err
}

// Attempts to abort 4 times with exponential backoff after each failed attempt
func abortMPU(s3Client s3iface.S3API, bucket, key, uploadID string) error {
	var partsInTransit int

	// Try and abort 4 times with increasing wait times.  After 4 attempts fail with error
	for i := 0; i < 4; i++ {
		_, err := s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   &bucket,
			Key:      &key,
			UploadId: &uploadID,
		})
		if err != nil {
			return err
		}

		// Ensure the Parts List is empty or charges will accrue
		res, err := s3Client.ListParts(&s3.ListPartsInput{
			Bucket:   &bucket,
			Key:      &key,
			UploadId: &uploadID,
		})
		if err != nil {
			return err
		}
		partsInTransit = len(res.Parts)

		if partsInTransit == 0 {
			return nil
		}
		time.Sleep(1 << i * 5 * time.Second) // Wait to let pending part transfer complete
	}

	return fmt.Errorf("could not abort MPU '%s'. %d parts still in progress", uploadID, partsInTransit)
}
