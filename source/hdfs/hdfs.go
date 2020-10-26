package hdfs

import (
	"github.com/colinmarc/hdfs/v2"
	"github.com/hexbee-net/errors"
)

type file struct {
	Hosts    []string
	User     string
	FilePath string

	client         *hdfs.Client
	externalClient bool
}

func (f *file) Close() error {
	if f.client != nil && !f.externalClient {
		err := f.client.Close()
		f.client = nil

		if err != nil {
			return errors.Wrap(err, "failed to close HDFS client")
		}
	}

	return nil
}
