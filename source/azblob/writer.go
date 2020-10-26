package azblob

import (
	"context"
	"io"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/hexbee-net/errors"
)

type WriterOptions struct {
	// HTTPSender configures the sender of HTTP requests
	HTTPSender pipeline.Factory
	// Retry configures the built-in retry policy behavior.
	RetryOptions azblob.RetryOptions
	// Log configures the pipeline's logging infrastructure indicating what information is logged and where.
	Log pipeline.LogOptions
	// Parallelism limits the number of go routines created to read blob content (0 = default)
	Parallelism int
}

type Writer struct {
	blob

	writeDone  chan error
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	options    WriterOptions
}

// NewAzBlobFileWriter creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriter(ctx context.Context, URL string, credential azblob.Credential, options WriterOptions) (w *Writer, err error) {
	w = &Writer{
		blob: blob{
			ctx:        ctx,
			credential: credential,
		},
		writeDone: make(chan error),
		options:   options,
	}

	if err := w.blob.open(URL, options.HTTPSender, options.RetryOptions, options.Log); err != nil {
		return nil, err
	}

	w.pipeReader, w.pipeWriter = io.Pipe()

	go func(ctx context.Context, blobURL *azblob.BlockBlobURL, opt WriterOptions, reader io.Reader, readerPipeSource *io.PipeWriter, done chan<- error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := azblob.UploadStreamToBlockBlob(ctx, reader, *blobURL, azblob.UploadStreamToBlockBlobOptions{MaxBuffers: opt.Parallelism})
		if err != nil {
			_ = readerPipeSource.CloseWithError(err)
		}

		done <- err
	}(w.ctx, w.blockBlobURL, w.options, w.pipeReader, w.pipeWriter, w.writeDone)

	return w, nil
}

func (w Writer) Write(p []byte) (n int, err error) {
	if w.blockBlobURL == nil {
		return 0, errors.WithStack(errURLNotOpened)
	}

	bytesWritten, writeError := w.pipeWriter.Write(p)
	if writeError != nil {
		_ = w.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

func (w Writer) Close() (err error) {
	if w.pipeWriter != nil {
		if closeErr := w.pipeWriter.Close(); closeErr != nil {
			return errors.Wrap(closeErr, "failed to close pipe writer")
		}

		// wait for pending uploads
		if w.writeDone != nil {
			err = <-w.writeDone
		}
	}

	return err
}
