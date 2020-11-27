package s3tos3

import "fmt"

type byteSize float64

const (
	_           = iota
	kb byteSize = 1 << (10 * iota)
	mb
	gb
	tb
	pb
)

func (b byteSize) String() string {
	switch {
	case b >= pb:
		return fmt.Sprintf("%.2fPB", b/pb)
	case b >= tb:
		return fmt.Sprintf("%.2fTB", b/tb)
	case b >= gb:
		return fmt.Sprintf("%.2fGB", b/gb)
	case b >= mb:
		return fmt.Sprintf("%.2fMB", b/mb)
	case b >= kb:
		return fmt.Sprintf("%.2fKB", b/kb)
	}
	return fmt.Sprintf("%.2fB", b)
}
