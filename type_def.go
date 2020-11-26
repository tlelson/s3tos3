package s3tos3

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// Mover configures an instance of the s3tos3 mover.  Multiple moves may be called on the same
// instance to ensure that local resources are not used beyond those configured.
//
// For example. If you only want 100 concurrent requests but have 5 files to move, the same instance
// should be used for all move operation.  Creating 2 instances of the transferer and configuring
// both to have 100 concurrent requests will result in a total of 200 concurrent requests.
type Mover struct {
	MaxConcurrentRequests uint // default 300
	MaxQueueSize          uint // default 1000
	MultiPartChunkSizeMB  uint // default 64
	S3Client              s3iface.S3API
	Logger                interface { // If you want logs, provide a logger. We aren't going to force you.
		Printf(format string, v ...interface{})
	}

	// private
	partUploadQueue chan request
}

// unLogger fulfills the logging interface but does nothing.
type unLogger struct{}

func (unLogger) Printf(string, ...interface{}) {}

type request struct {
	input *s3.UploadPartCopyInput
	done  chan response
}

type response struct {
	output     *s3.UploadPartCopyOutput
	partNumber *int64
	err        error
}

// s3client returns and re-uses a valid client whether or not it has been provided.  Use of
// Mover.S3Client directly may result in panic since it may not be initially provided. Use this
// method internally to ensure at least the default client is provided.
func (m *Mover) s3Client() s3iface.S3API {
	// Create s3client if not provided
	if m.S3Client == nil {
		region, found := os.LookupEnv("AWS_REGION")
		if !found {
			region = "ap-southeast-2"
		}

		m.S3Client = s3.New(session.Must(session.NewSession(&aws.Config{Region: &region})))
	}
	return m.S3Client
}

// Start starts the mover with default values in preparation for one or many Move operations.
func (m *Mover) Start() {
	// Set sensible defaults.
	if m.MaxConcurrentRequests == 0 {
		m.MaxConcurrentRequests = 300
	}
	if m.MaxQueueSize == 0 {
		m.MaxQueueSize = 1e3
	}
	if m.MultiPartChunkSizeMB == 0 {
		m.MultiPartChunkSizeMB = 64
	}

	if m.Logger == nil {
		m.Logger = unLogger{}
	}

	// Create a job queue for ALL moving files.
	m.partUploadQueue = make(chan request, m.MaxQueueSize) // closed by Stop

	// Start the sender that reads from the partUploadQueue. Errors are delt with by the sender.
	go m.startWorkerPool()
}

// Stop must be called on a Started Mover to ensure resources are released.
func (m *Mover) Stop() {
	close(m.partUploadQueue)
	m.Logger.Printf("Mover Stopped")
}

// startWorkerPool schedules part copy operations to maximise but not exceed the configured
// specifications.
func (m *Mover) startWorkerPool() {
	for i := 0; i < int(m.MaxConcurrentRequests); i++ {
		go func(i int) {
			for job := range m.partUploadQueue {
				result, err := m.s3Client().UploadPartCopy(job.input)
				if err != nil {
					// TODO: Distinguish error types
					// if fatal error break
					// if throttling, backoff
					m.Logger.Printf("Sender %d: Error: %v", i, err)
				}
				job.done <- response{result, job.input.PartNumber, err}
			}
		}(i)
	}
}
