package azblob

import (
	"context"
	"io"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/hexbee-net/errors"
)

type ReaderOptions struct {
	// HTTPSender configures the sender of HTTP requests
	HTTPSender pipeline.Factory
	// Retry configures the built-in retry policy behavior.
	RetryOptions azblob.RetryOptions
	// Log configures the pipeline's logging infrastructure indicating what information is logged and where.
	Log pipeline.LogOptions
}

type Reader struct {
	blob

	fileSize int64
	offset   int64
	options  ReaderOptions
}

// NewReader creates an Azure Blob Reader.
func NewReader(ctx context.Context, URL string, credential azblob.Credential, options ReaderOptions) (r *Reader, err error) {
	r = &Reader{
		blob: blob{
			ctx:        ctx,
			credential: credential,
		},
		fileSize: int64(-1),
		options:  options,
	}

	if err := r.blob.open(URL, options.HTTPSender, options.RetryOptions, options.Log); err != nil {
		return nil, err
	}

	if props, err := r.blockBlobURL.GetProperties(r.ctx, azblob.BlobAccessConditions{}); err != nil {
		return nil, errors.Wrap(err, "failed to get blob properties")
	} else {
		r.fileSize = props.ContentLength()
	}

	return r, nil
}

func (r Reader) Read(p []byte) (n int, err error) {
	if r.blockBlobURL == nil {
		return 0, errors.WithStack(errURLNotOpened)
	}

	if r.fileSize > 0 && r.offset >= r.fileSize {
		return 0, io.EOF
	}

	count := int64(len(p))
	resp, err := r.blockBlobURL.Download(r.ctx, r.offset, count, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return 0, err
	}
	if r.fileSize < 0 {
		r.fileSize = resp.ContentLength()
	}

	toRead := r.fileSize - r.offset
	if toRead > count {
		toRead = count
	}

	body := resp.Body(azblob.RetryReaderOptions{})
	bytesRead, err := io.ReadFull(body, p[:toRead])
	if err != nil {
		return 0, errors.Wrap(err, "failed to read data")
	}

	r.offset += int64(bytesRead)

	return bytesRead, nil
}

func (r Reader) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errors.WithFields(
			errors.WithStack(errWhence),
			errors.Fields{
				"whence": whence,
			})
	}

	if r.fileSize > 0 {
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
		}
	}

	r.offset = offset

	return r.offset, nil
}

func (r Reader) Close() error {
	return nil
}
