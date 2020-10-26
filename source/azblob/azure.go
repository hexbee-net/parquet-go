package azblob

import (
	"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/hexbee-net/errors"
)

const (
	errWhence        = errors.Error("invalid whence")
	errInvalidOffset = errors.Error("invalid offset")
	errURLNotOpened  = errors.Error("url not opened")
)

type blob struct {
	ctx          context.Context
	URL          *url.URL
	credential   azblob.Credential
	blockBlobURL *azblob.BlockBlobURL
}

type BlobOptions struct { // HTTPSender configures the sender of HTTP requests
	HTTPSender pipeline.Factory
	// Retry configures the built-in retry policy behavior.
	RetryOptions azblob.RetryOptions
	// Log configures the pipeline's logging infrastructure indicating what information is logged and where.
	Log pipeline.LogOptions
}

func (b *blob) open(URL string, sender pipeline.Factory, retryOptions azblob.RetryOptions, logOptions pipeline.LogOptions) (err error) {
	if b.URL, err = url.Parse(URL); err != nil {
		return errors.Wrap(err, "failed to parse URL")
	}

	blobURL := azblob.NewBlockBlobURL(*b.URL, azblob.NewPipeline(b.credential, azblob.PipelineOptions{
		HTTPSender: sender,
		Retry:      retryOptions,
		Log:        logOptions,
	}))

	// get account properties to validate credentials
	if _, err := blobURL.GetAccountInfo(b.ctx); err != nil {
		return errors.Wrap(err, "failed to get account properties")
	}

	b.blockBlobURL = &blobURL

	return nil
}
