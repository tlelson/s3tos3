# S3 to S3

This package transfers file from one S3 location to another (in a single region) fast and
reliably.

Transferring multiple files is best done with the same Mover instance to ensure reduce
throttling/connection dropping from S3.

Typical transfer speeds range from 3GB/s to 7.5GB/s.

## Example

```go
package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/tlelson/s3tos3"
)

type logger struct{}

func (logger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func main() {

	filesToMove := [][2]string{
		{"videos2/75_GB.mxf", "videos1/75_GB-A.mxf"},
		{"videos2/8_GB.mxf", "videos1/8_GB.mxf"},
		{"videos2/75_GB.mxf", "videos1/75_GB-B.mxf"},
		{"videos2/6_GB.mxf", "videos1/6_GB.mxf"},
		{"videos2/75_GB.mxf", "videos1/75_GB-C.mxf"},
		{"videos2/2_GB.mxf", "videos1/2_GB.mxf"},
		{"videos2/38_MB.mp4", "videos1/38_MB.mp4"},
	}

	mover := s3tos3.Mover{
		Logger: logger{},
	}

	mover.Start()

	wg := sync.WaitGroup{}
	for _, filePair := range filesToMove {
		wg.Add(1)

		go func(src, dst string) {
			defer wg.Done()
			err := mover.Move(
				"bucket1", src,
				"bucket2", dst,
			)
			if err != nil {
				fmt.Printf("Error (%s → %s): %v\n", src, dst, err)
				return
			}
		}(filePair[0], filePair[1])
	}
	wg.Wait()
	mover.Stop()
	fmt.Println("ALL DONE")
}
```
