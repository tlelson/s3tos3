package s3tos3

import (
	"os"
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
	Tick                 time.Duration
	RequestQueueSize     uint
	MultiPartChunkSizeMB uint // default 64
	S3Client             s3iface.S3API
	// If you want logs, provide a logger. We aren't going to force you.
	Logger interface {
		Printf(format string, v ...interface{})
	}

	// internal
	partUploadQueue chan request
	uploaderDone    chan bool
	saveStats       chan uploadProperties // for individual move ops send results in
	aggregatedStats chan []uploadProperties
	startTime       time.Time
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

// Start initialises the mover and prepares for one or many Move operations.
func (m *Mover) Start() {
	// Initialise the Mover
	m.startTime = time.Now()

	// Set sensible defaults.
	if m.MultiPartChunkSizeMB == 0 {
		m.MultiPartChunkSizeMB = 64
	}
	if m.Tick == 0 {
		m.Tick = 10 * time.Millisecond
	}
	if m.RequestQueueSize == 0 {
		m.RequestQueueSize = 1e3
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

	// Start a collector to store job results
	m.saveStats = make(chan uploadProperties)
	m.aggregatedStats = make(chan []uploadProperties)
	go aggregateStats(m.saveStats, m.aggregatedStats)

	// Start the sender that reads from the partUploadQueue. Errors are delt with by the sender.
	// Create a job queue for ALL moving files.
	m.partUploadQueue = make(chan request, m.RequestQueueSize) // closed by Stop
	m.uploaderDone = make(chan bool)
	go m.startWorkerPool()
}

// Stop must be called on a Started Mover to ensure resources are released.
func (m *Mover) Stop() {
	d := time.Since(m.startTime)

	// stop running queues and the goroutines processing them
	close(m.partUploadQueue)
	<-m.uploaderDone
	close(m.saveStats)
	stats := <-m.aggregatedStats

	m.Logger.Printf("stats saver is done. Reading job stats ...")
	var totalSize byteSize
	for _, job := range stats {
		totalSize += byteSize(job.objSize)
	}

	m.Logger.Printf("Mover Stopped. %d jobs (%s) completed in %s. Cummulative rate of %s/s",
		len(stats), totalSize, d.Round(time.Second), totalSize/byteSize(d.Seconds()),
	)
}

// startWorkerPool schedules part copy operations to maximise but not exceed the configured
// specifications.
func (m Mover) startWorkerPool() {
	throttle := m.scheduler(m.Tick, 2*time.Second)
	defer throttle.Stop()

	for job := range m.partUploadQueue {
		<-throttle.C
		go func(job request) {
			result, err := m.S3Client.UploadPartCopy(job.input)
			// if error due to request rate, backoff. Otherwise pass through and let the move op
			// deal with it.
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "RequestError" {
				throttle.Inc <- true
				m.partUploadQueue <- job
				return
			}
			job.done <- response{result, job.input.PartNumber, err}
		}(job)
	}
	m.uploaderDone <- true
}

func aggregateStats(in chan uploadProperties, out chan []uploadProperties) {
	var results []uploadProperties
	for s := range in {
		results = append(results, s)
	}
	out <- results
}
