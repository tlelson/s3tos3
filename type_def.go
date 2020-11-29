package s3tos3

import (
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	MaxConcurrentRequests uint // default 600
	MaxQueueSize          uint // default 3000
	MultiPartChunkSizeMB  uint // default 64
	S3Client              s3iface.S3API
	// If you want logs, provide a logger. We aren't going to force you.
	Logger interface {
		Printf(format string, v ...interface{})
	}

	// private
	partUploadQueue chan request
	js              jobStats
	startTime       time.Time
}

type jobStats struct {
	*sync.Mutex
	jobs []uploadProperties
}

func (js *jobStats) Add(up uploadProperties) {
	js.Lock()
	js.jobs = append(js.jobs, up)
	js.Unlock()
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

// Start starts the mover with default values in preparation for one or many Move operations.
func (m *Mover) Start() {
	// Initialise the Mover
	m.startTime = time.Now()
	m.js = jobStats{
		Mutex: &sync.Mutex{},
		jobs:  []uploadProperties{},
	}

	// Set sensible defaults.
	if m.MaxConcurrentRequests == 0 {
		m.MaxConcurrentRequests = 2
	}
	if m.MaxQueueSize == 0 {
		m.MaxQueueSize = 1e3
	}
	if m.MultiPartChunkSizeMB == 0 {
		m.MultiPartChunkSizeMB = 16
	}

	if m.Logger == nil {
		m.Logger = unLogger{}
	}

	// Create s3client if not provided. S3Client is used inside concurrent routines, so needs to
	// exist before they start
	if m.S3Client == nil {
		region, found := os.LookupEnv("AWS_REGION")
		if !found {
			region = "ap-southeast-2"
		}

		m.S3Client = s3.New(session.Must(session.NewSession(&aws.Config{Region: &region})))
	}

	// Create a job queue for ALL moving files.
	m.partUploadQueue = make(chan request, m.MaxQueueSize) // closed by Stop

	// Start the sender that reads from the partUploadQueue. Errors are delt with by the sender.
	go m.startWorkerPool()
}

// Stop must be called on a Started Mover to ensure resources are released.
func (m *Mover) Stop() {
	close(m.partUploadQueue)
	d := time.Since(m.startTime)

	var totalSize byteSize
	for _, job := range m.js.jobs {
		totalSize += byteSize(job.objSize)
	}

	m.Logger.Printf("Mover Stopped. %d jobs (%s) completed in %s. Cummulative rate of %s/s",
		len(m.js.jobs), totalSize, d.Round(time.Second), totalSize/byteSize(d.Seconds()),
	)
}

// startWorkerPool schedules part copy operations to maximise but not exceed the configured
// specifications.
func (m *Mover) startWorkerPool() {
	// If the workers have to wait to get their failed job back on the queue then they will be
	// useless until they can.  Let this goroutine do the waiting for them while they continue on.
	var replayChannel = make(chan request) // Unbuffered to unblock workers
	go func() {
		for failedJob := range replayChannel {
			m.partUploadQueue <- failedJob
		}
	}()

	for i := 0; i < int(m.MaxConcurrentRequests); i++ {
		go func() {
			for job := range m.partUploadQueue {
				// TODO: Impliment a scheduler
				//m.Logger.Printf("Pulled job, Waiting for %v", cycleTime+pause)
				time.Sleep(time.Millisecond) // basic reducer for now

				result, err := m.S3Client.UploadPartCopy(job.input)
				// if throttling, backoff otherwise pass through and let the move op
				// deal with it.
				if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "RequestError" {
					// TODO: Exp. backoff but cap at 1s
					//m.Logger.Printf("RequestError: backing off request interval to %s", cycleTime)
					replayChannel <- job
					continue
				}
				job.done <- response{result, job.input.PartNumber, err}
			}
		}()
	}
}
