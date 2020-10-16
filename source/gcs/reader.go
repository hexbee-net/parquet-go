package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/hexbee-net/errors"
)

type Reader struct {
	file
	fileSize int64
	offset   int64
	whence   int
}

// NewReader creates a GCS Reader.
func NewReader(ctx context.Context, projectId, bucketName, name string) (*Reader, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.WithStack(errInstantiate)
	}

	reader := &Reader{
		file: file{
			ProjectID:      projectId,
			BucketName:     bucketName,
			FilePath:       name,
			ctx:            ctx,
			externalClient: false,
			Client:         client,
		},
	}

	if err := reader.open(ctx); err != nil {
		return nil, err
	}

	return reader, nil
}

// NewReaderWithClient is the same as NewReader but allows passing your own GCS client.
func NewReaderWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*Reader, error) {
	reader := &Reader{
		file: file{
			ProjectID:      projectID,
			BucketName:     bucketName,
			FilePath:       name,
			ctx:            ctx,
			externalClient: true,
			Client:         client,
		},
	}

	if err := reader.open(ctx); err != nil {
		return nil, err
	}

	return reader, nil
}

func (r *Reader) Read(p []byte) (cnt int, err error) {
	if r.fileSize > 0 && r.offset >= r.fileSize {
		return 0, io.EOF
	}

	numBytes := len(p)
	reader, err := r.Object.NewRangeReader(r.ctx, r.offset, int64(numBytes))
	if err != nil {
	}
	defer func() { _ = reader.Close() }()

	for cnt < numBytes {
		n, err := reader.Read(p[cnt:])
		cnt += n

		if err != nil {
			return cnt, errors.Wrap(err, "failed to read file data")
		}
	}

	r.offset += int64(cnt)
	return cnt, nil
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errors.WithFields(
			errors.WithStack(errWhence),
			errors.Fields{
				"whence": whence,
			})
	}

	switch whence {
	case io.SeekStart:
		if offset < 0 || offset > r.fileSize {
			return 0, errors.WithFields(
				errors.WithStack(errInvalidOffset),
				errors.Fields{
					"offset":   offset,
					"whence":   "SeekStart",
					"fileSize": r.fileSize,
				})
		}

		r.offset = offset

	case io.SeekCurrent:
		offset += r.offset
		if offset < 0 || offset > r.fileSize {
			return 0, errors.WithFields(
				errors.WithStack(errInvalidOffset),
				errors.Fields{
					"offset":   offset,
					"whence":   "SeekCurrent",
					"fileSize": r.fileSize,
				})
		}

		r.offset = offset

	case io.SeekEnd:
		if offset > -1 || -offset > r.fileSize {
			return 0, errors.WithFields(
				errors.WithStack(errInvalidOffset),
				errors.Fields{
					"offset":   offset,
					"whence":   "SeekEnd",
					"fileSize": r.fileSize,
				})
		}

		r.offset = r.fileSize + offset
	}

	return r.offset, nil
}

func (r *Reader) open(ctx context.Context) (err error) {
	r.Bucket = r.Client.Bucket(r.BucketName)
	r.Object = r.Bucket.Object(r.FilePath)

	objAttrs, err := r.Object.Attrs(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get object attributes")
	}

	r.fileSize = objAttrs.Size

	return err
}
