package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Writer struct {
	file

	writeDone       chan error
	pipeReader      *io.PipeReader
	pipeWriter      *io.PipeWriter
	uploader        *s3manager.Uploader
	uploaderOptions []func(*s3manager.Uploader)
}

// NewWriter creates an S3 Writer.
func NewWriter(ctx context.Context, bucket, key string, uploaderOptions []func(*s3manager.Uploader), configProvider client.ConfigProvider, configs ...*aws.Config) (*Writer, error) {
	return NewWriterWithClient(ctx, s3.New(configProvider, configs...), bucket, key, uploaderOptions)
}

// NewWriterWithClient is the same as NewWriter but allows passing your own S3 client.
func NewWriterWithClient(ctx context.Context, s3Client s3iface.S3API, bucket, key string, uploaderOptions []func(*s3manager.Uploader)) (*Writer, error) {
	writer := Writer{
		file: file{
			ctx:        ctx,
			client:     s3Client,
			BucketName: bucket,
			Key:        key,
		},

		writeDone:       make(chan error),
		uploaderOptions: uploaderOptions,
	}

	writer.pipeReader, writer.pipeWriter = io.Pipe()
	writer.uploader = s3manager.NewUploaderWithClient(writer.client, writer.uploaderOptions...)

	uploadParams := &s3manager.UploadInput{
		Bucket: aws.String(writer.BucketName),
		Key:    aws.String(writer.Key),
		Body:   writer.pipeReader,
	}

	go func(uploader *s3manager.Uploader, params *s3manager.UploadInput, done chan<- error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := uploader.UploadWithContext(writer.ctx, params)
		if err != nil {
			_ = writer.pipeWriter.CloseWithError(err)
		}

		done <- err
	}(writer.uploader, uploadParams, writer.writeDone)

	return &writer, nil
}

func (w *Writer) Write(p []byte) (n int, err error) {
	bytesWritten, err := w.pipeWriter.Write(p)
	if err != nil {
		_ = w.pipeWriter.CloseWithError(err)

		return 0, err
	}

	return bytesWritten, nil
}

func (w *Writer) Close() (err error) {
	if w.pipeWriter != nil {
		if closeErr := w.pipeWriter.Close(); closeErr != nil {
			return closeErr
		}
	}

	// wait for pending uploads
	if w.writeDone != nil {
		err = <-w.writeDone
	}

	return err
}
