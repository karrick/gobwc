package gobwc

import (
	"bytes"
	"io"
	"sync"
	"time"
)

// BucketWriteCloser is a data structure that wraps an io.WriteCloser, but groups the written
// new-line terminated lines by buckets, combining lines written to the same bucket if possible.  A
// bucketed line starts with the at-sign, @, followed by the bucket name until the hash symbol, #.
// Everything after the # symbol is the content of the line.  If another bucket with the same name
// is already found, the hash symbol and everything following it will be written to the found
// bucket.
//
// There is no lexicographical ordering of the bucket names prior to writing them to the underlying
// io.WriteCloser, other than non-bucketed lines being written first.  Lines without an @ symbol are
// non-bucketed lines.
type BucketWriteCloser struct {
	buckets [][]byte
	iowc    io.WriteCloser
	lock    sync.Mutex
	stop    chan struct{}
	stopped sync.WaitGroup
}

// NewBucketWriteCloser returns a BucketWriteCloser that groups written lines by bucket names.
//
//   bb := NewNopCloseBuffer()
//   bwc := NewBucketWriteCloser(bb, 10*time.Second)
//   defer bwc.Close()
//
//   line := "example line\n"
//   n, err := bwc.Write([]byte(line))
//   if want := len(line); n != want {
//   	t.Errorf("Actual: %#v; Expected: %#v", n, want)
//   }
//   if err != nil {
//   	t.Errorf("Actual: %#v; Expected: %#v", err, nil)
//   }
//   if want := ""; bb.String() != want {
//   	t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
//   }
//
//   if err := bwc.Flush(); err != nil {
//   	t.Errorf("Actual: %#v; Expected: %#v", err, nil)
//   }
//   if want := line; bb.String() != want {
//   	t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
//   }
func NewBucketWriteCloser(iowc io.WriteCloser, flush time.Duration) *BucketWriteCloser {
	bwc := &BucketWriteCloser{
		buckets: make([][]byte, 1),
		iowc:    iowc,
		stop:    make(chan struct{}, 1),
	}
	bwc.stopped.Add(1)
	go func() {
		ticker := time.NewTicker(flush)
		for {
			select {
			case <-bwc.stop:
				ticker.Stop()
				bwc.stopped.Done()
				return
			case <-ticker.C:
				bwc.Flush()
			}
		}
	}()
	return bwc
}

// Close flushes unwritten lines to the underlying io.WriteCloser, then invokes its Close method.
func (bw *BucketWriteCloser) Close() error {
	bw.stop <- struct{}{}
	bw.stopped.Wait()
	err1 := bw.Flush()
	err2 := bw.iowc.Close()
	if err2 != nil {
		return err2
	}
	return err1
}

// Flush flushes unwritten lines to the underlying io.WriteCloser.  Flush is automatically called by
// the BucketWriteCloser on a preset periodicity set during instantiation.
func (bw *BucketWriteCloser) Flush() error {
	bw.lock.Lock()
	defer bw.lock.Unlock()

	var buf []byte
	for _, b := range bw.buckets {
		buf = append(buf, b...)
	}
	_, err := bw.iowc.Write(buf)
	bw.buckets = make([][]byte, 1)
	return err
}

// Write saves the data to the appropriate bucket to be written when Flush is next called.
func (bw *BucketWriteCloser) Write(data []byte) (int, error) {
	bw.lock.Lock()
	defer bw.lock.Unlock()

	switch data[0] {
	case '@':
		// @bucket#everything-else
		if index := bytes.IndexByte(data, '#'); index > 0 {
			bucket := data[:index]
			for i := 1; i < len(bw.buckets); i++ {
				if bytes.HasPrefix(bw.buckets[i], bucket) {
					bw.buckets[i] = append(bw.buckets[i][:len(bw.buckets[i])-1], data[index:]...) // #everything-else
					return len(data), nil
				}
			}
			cpy := make([]byte, len(data))
			copy(cpy, data)
			bw.buckets = append(bw.buckets, cpy)
			return len(data), nil
		}
	}
	bw.buckets[0] = append(bw.buckets[0], data...)
	return len(data), nil
}
