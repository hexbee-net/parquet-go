package gcs

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/hexbee-net/errors"
)

type Writer struct {
	file
	fileWriter *storage.Writer
}

// NewWriter creates an GCS Writer.
func NewWriter(ctx context.Context, projectID, bucketName, name string) (*Writer, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.WithStack(errInstantiate)
	}

	writer := &Writer{
		file: file{
			ProjectID:      projectID,
			BucketName:     bucketName,
			FilePath:       name,
			ctx:            ctx,
			externalClient: false,
			Client:         client,
		},
	}

	writer.create()

	return writer, nil
}

// NewWriterWithClient is the same as NewWriter but allows passing your own GCS client.
func NewWriterWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*Writer, error) {
	writer := &Writer{
		file: file{
			ProjectID:      projectID,
			BucketName:     bucketName,
			ctx:            ctx,
			FilePath:       name,
			externalClient: true,
			Client:         client,
		},
	}

	writer.create()

	return writer, nil
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return w.fileWriter.Write(p)
}

func (w *Writer) Close() error {
	if w.fileWriter != nil {
		if err := w.fileWriter.Close(); err != nil {
			return errors.Wrap(err, "failed to close GCS writer")
		}
	}

	return w.file.Close()
}

func (w *Writer) create() {
	w.Bucket = w.Client.Bucket(w.BucketName)
	w.Object = w.Bucket.Object(w.FilePath)

	w.fileWriter = w.Object.NewWriter(w.ctx)
}
