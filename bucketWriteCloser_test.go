package gobwc

import (
	"github.com/karrick/gorill"
	"testing"
	"time"
)

func TestBucketWriteCloserFlushWritesData(t *testing.T) {
	bb := gorill.NewNopCloseBuffer()
	bwc := NewBucketWriteCloser(bb, time.Minute)
	defer bwc.Close()

	line := "example line\n"
	n, err := bwc.Write([]byte(line))
	if want := len(line); n != want {
		t.Errorf("Actual: %#v; Expected: %#v", n, want)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	if want := ""; bb.String() != want {
		t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
	}

	if err := bwc.Flush(); err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	if want := line; bb.String() != want {
		t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
	}
}

func TestBucketWriteCloserCloseFlushesData(t *testing.T) {
	bb := gorill.NewNopCloseBuffer()
	bwc := NewBucketWriteCloser(bb, time.Minute)

	line := "example line\n"
	n, err := bwc.Write([]byte(line))
	if want := len(line); n != want {
		t.Errorf("Actual: %#v; Expected: %#v", n, want)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	if want := ""; bb.String() != want {
		t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
	}

	if err := bwc.Close(); err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	if want := line; bb.String() != want {
		t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
	}
}

func TestBucketWriteCloserBucketsLinesCorrectly(t *testing.T) {
	bb := gorill.NewNopCloseBuffer()
	bwc := NewBucketWriteCloser(bb, time.Minute)

	lines := []string{
		"first line to be emitted\n",            // 0
		"@33#bunch of stuff\n",                  // 1
		"@44#nothing of too much interest\n",    // 2
		"second line to be emitted\n",           // 3
		"@33#some more stuff for this bucket\n", // 4
		"@44#again, more boring stuff\n",        // 5
	}

	for _, line := range lines {
		n, err := bwc.Write([]byte(line))
		if want := len(line); n != want {
			t.Errorf("Actual: %#v; Expected: %#v", n, want)
		}
		if err != nil {
			t.Errorf("Actual: %#v; Expected: %#v", err, nil)
		}
		if want := ""; bb.String() != want {
			t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
		}
	}

	if err := bwc.Close(); err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}

	want := lines[0] + lines[3] + lines[1][:len(lines[1])-1] + lines[4][3:] + lines[2][:len(lines[2])-1] + lines[5][3:]
	if bb.String() != want {
		t.Errorf("Actual: %#v; Expected: %#v", bb.String(), want)
	}
}
