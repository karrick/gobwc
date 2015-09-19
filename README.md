# gobwc

Go BucketWriteCloser library.

## Description

BucketWriteCloser is a data structure that wraps an io.WriteCloser, but groups the written
new-line terminated lines by buckets, combining lines written to the same bucket if possible.  A
bucketed line starts with the at-sign, @, followed by the bucket name until the hash symbol, #.
Everything after the # symbol is the content of the line.  If another bucket with the same name
is already found, the hash symbol and everything following it will be written to the found
bucket.

There is no lexicographical ordering of the bucket names prior to writing them to the underlying
io.WriteCloser, other than non-bucketed lines being written first.  Lines without an @ symbol are
non-bucketed lines.
