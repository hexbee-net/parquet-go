package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hexbee-net/errors"
)

const (
	rangeHeader       = "bytes=%d-%d"
	rangeHeaderSuffix = "bytes=%d"
)

type Reader struct {
	file

	fileSize   int64
	offset     int64
	whence     int
	downloader *s3manager.Downloader
}

// NewReader creates an S3 Reader.
func NewReader(ctx context.Context, bucket, key string, configProvider client.ConfigProvider, configs ...*aws.Config) (*Reader, error) {
	return NewReaderWithClient(ctx, s3.New(configProvider, configs...), bucket, key)
}

// NewReaderWithClient is the same as NewReader but allows passing your own S3 client.
func NewReaderWithClient(ctx context.Context, s3Client s3iface.S3API, bucket, key string) (*Reader, error) {
	s3Downloader := s3manager.NewDownloaderWithClient(s3Client)

	reader := Reader{
		file: file{
			ctx:        ctx,
			client:     s3Client,
			BucketName: bucket,
			Key:        key,
		},

		downloader: s3Downloader,
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(reader.BucketName),
		Key:    aws.String(reader.Key),
	}

	headObject, err := reader.client.HeadObjectWithContext(reader.ctx, input)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch file description")
	}

	if headObject.ContentLength != nil {
		reader.fileSize = *headObject.ContentLength
	}

	return &reader, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.fileSize > 0 && r.offset >= r.fileSize {
		return 0, io.EOF
	}

	numBytes := len(p)
	bytesRange := r.getBytesRange(r.offset, r.whence, numBytes)
	getObj := &s3.GetObjectInput{
		Bucket: aws.String(r.BucketName),
		Key:    aws.String(r.Key),
	}

	if len(bytesRange) > 0 {
		getObj.Range = aws.String(bytesRange)
	}

	wab := aws.NewWriteAtBuffer(p)

	bytesDownloaded, err := r.downloader.DownloadWithContext(r.ctx, wab, getObj)
	if err != nil {
		return 0, err
	}

	r.offset += bytesDownloaded

	if buf := wab.Bytes(); len(buf) > numBytes {
		// backing buffer reassigned, copy over some of the data
		copy(p, buf)
		bytesDownloaded = int64(len(p))
	}

	return int(bytesDownloaded), err
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
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
	r.whence = whence

	return r.offset, nil
}

func (r *Reader) Close() error {
	return nil
}

func (r *Reader) getBytesRange(offset int64, whence int, numBytes int) string {
	var (
		byteRange string
		begin     int64
		end       int64
	)

	// Processing for unknown file size relies on the requester to know which ranges are valid.
	// May occur if caller is missing HEAD permissions.
	if r.fileSize < 1 {
		switch whence {
		case io.SeekStart, io.SeekCurrent:
			byteRange = fmt.Sprintf(rangeHeader, offset, offset+int64(numBytes)-1)
		case io.SeekEnd:
			byteRange = fmt.Sprintf(rangeHeaderSuffix, offset)
		}

		return byteRange
	}

	switch whence {
	case io.SeekStart, io.SeekCurrent:
		begin = offset
	case io.SeekEnd:
		begin = r.fileSize + offset
	default:
		return byteRange
	}

	endIndex := r.fileSize - 1

	if begin < 0 {
		begin = 0
	}

	end = begin + int64(numBytes) - 1
	if end > endIndex {
		end = endIndex
	}

	byteRange = fmt.Sprintf(rangeHeader, begin, end)

	return byteRange
}
