package gcs

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/hexbee-net/errors"
)

const (
	errInstantiate   = errors.Error("failed to instantiate GCS client")
	errWhence        = errors.Error("invalid whence")
	errInvalidOffset = errors.Error("invalid offset")
)

type file struct {
	ProjectID  string
	BucketName string
	FilePath   string

	ctx            context.Context
	externalClient bool
	Client         *storage.Client
	Bucket         *storage.BucketHandle
	Object         *storage.ObjectHandle
}

func (f *file) Close() error {
	if f.Client != nil && !f.externalClient {
		err := f.Client.Close()
		f.Client = nil

		if err != nil {
			return errors.Wrap(err, "failed to close GCS client")
		}
	}

	return nil
}
