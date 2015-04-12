package utils

import "io"

// NewLimitedBufferReader returns a reader that reads from the given reader
// but limits the amount of data returned to at most n bytes.
func NewLimitedBufferReader(r io.Reader, n int) io.Reader {
	return &limitedBufferReader{
		r: r,
		n: n,
	}
}

type limitedBufferReader struct {
	r io.Reader
	n int
}

func (r *limitedBufferReader) Read(p []byte) (n int, err error) {
	np := p
	if len(np) > r.n {
		np = np[:r.n]
	}
	return r.r.Read(np)
}
