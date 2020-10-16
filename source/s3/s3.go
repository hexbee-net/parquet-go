package s3

import (
	"context"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/hexbee-net/errors"
)

type file struct {
	ctx    context.Context
	client s3iface.S3API

	BucketName string
	Key        string
}

const (
	errWhence        = errors.Error("invalid whence")
	errInvalidOffset = errors.Error("invalid offset")
)
