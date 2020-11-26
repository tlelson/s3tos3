package s3tos3_test

import (
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// mockS3 needs to be created for each transfer.
type mockS3 struct {
	s3iface.S3API
	objSize    int64 // bytes
	partRanges *prs
	aborted    bool
	completed  bool
}

func newMockS3(objectSize int64) mockS3 {
	return mockS3{
		objSize: objectSize,
		partRanges: &prs{
			parts: map[int64]string{},
			lock:  sync.Mutex{},
		},
	}
}

type prs struct {
	parts map[int64]string
	lock  sync.Mutex
}

func (p *prs) Add(partNum int64, sourceRange string) {
	p.lock.Lock()
	p.parts[partNum] = sourceRange
	p.lock.Unlock()
}

func (s mockS3) ListParts(*s3.ListPartsInput) (*s3.ListPartsOutput, error) {
	return &s3.ListPartsOutput{}, nil
}

// Try without pointer
func (s mockS3) UploadPartCopy(in *s3.UploadPartCopyInput) (*s3.UploadPartCopyOutput, error) {
	// Store this for later checking
	s.partRanges.Add(*in.PartNumber, *in.CopySourceRange)

	return &s3.UploadPartCopyOutput{
		CopyPartResult: &s3.CopyPartResult{
			ETag: randomID(),
		},
	}, nil
}

func (s mockS3) ListObjectsV2(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{
		Contents: []*s3.Object{{Size: &s.objSize}},
	}, nil
}

func (s *mockS3) AbortMultipartUpload(*s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
	s.aborted = true
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (s *mockS3) CompleteMultipartUpload(*s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	s.completed = true
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (s mockS3) CreateMultipartUpload(*s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{
		UploadId: randomID(),
	}, nil
}

func randomID() *string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("/dev/urandom not available!")
	}

	tmp := fmt.Sprintf("%x", b)
	return &tmp
}
