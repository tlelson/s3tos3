package s3tos3

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"

	"github.com/tlelson/s3tos3/adjustableticker"
)

type logger interface {
	Printf(format string, v ...interface{})
}

// Mover configures an instance of the s3tos3 mover.  Multiple moves may be called on the same
// instance to maximise throughput by reducing conflict over resources.
//
// None of the parameters are required.
type Mover struct {
	RequestTimeout       time.Duration // Default 10s
	Tick                 time.Duration // Default 8μs
	QueueSize            uint          // Default 0
	MultiPartChunkSizeMB uint          // Default 64
	Preload              uint          // Default 0
	S3Client             s3iface.S3API // If provided, will be reused.
	Logger               logger        // If provided, will log output.

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
	ctx   context.Context
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
		m.Tick = 8 * time.Millisecond
	}
	if m.RequestTimeout == 0 {
		m.RequestTimeout = 10 * time.Second
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

	// Start the request sender that reads from the partUploadQueue and makes throttled HTTP
	// requests.
	m.partUploadQueue = make(chan request, m.QueueSize)
	m.uploaderDone = make(chan bool)
	go m.startWorkerPool()
}

// Stop must be called on a Started Mover to ensure resources are released. Only successful move
// opperations are reported by logs.
func (m *Mover) Stop() {
	d := time.Since(m.startTime)

	// Stop running queues and the goroutines processing them
	// The following panics If Stop called before all Move ops complete.
	close(m.partUploadQueue)
	<-m.uploaderDone

	// Read and Report on Job statistics
	close(m.saveStats)
	stats := <-m.aggregatedStats

	// Only using total Size at this time
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
	throttle := adjustableticker.Ticker{
		Period: m.Tick,
		Logger: m.Logger,
	}
	throttle.Start()
	defer throttle.Stop()

	for job := range m.partUploadQueue {
		<-throttle.C
		go func(job request) {
			// If Job has been aborted. Don't send this request.
			select {
			case <-job.ctx.Done():
				m.Logger.Printf("%s - context cancelled", (*job.input.UploadId)[:4])
				return
			default:
				// otherwise proceed
			}

			// Set a timeout.
			ctx, cancel := context.WithTimeout(job.ctx, m.RequestTimeout)
			defer cancel()

			result, err := m.S3Client.UploadPartCopyWithContext(ctx, job.input)
			// Errors due to S3 throttling will be retried
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case "RequestError", "RequestCanceled":
					throttle.Inc <- time.Now() // Ticks are too close, incriment it.
					m.partUploadQueue <- job
					return
				}
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
