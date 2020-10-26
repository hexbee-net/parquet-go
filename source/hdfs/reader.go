package hdfs

import (
	"github.com/colinmarc/hdfs/v2"
	"github.com/hexbee-net/errors"
)

type Reader struct {
	file

	reader *hdfs.FileReader
}

func NewReader(hosts []string, user string, name string) (reader *Reader, err error) {
	reader = &Reader{
		file: file{
			Hosts:          hosts,
			User:           user,
			FilePath:       name,
			externalClient: false,
		},
	}

	reader.client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hosts,
		User:      user,
	})

	if err != nil {
		return nil, errors.Wrap(err, "failed to create HDFS client")
	}

	reader.reader, err = reader.client.Open(name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HDFS reader")
	}

	return reader, nil
}

func NewReaderWithClient(client *hdfs.Client, hosts []string, user string, name string) (reader *Reader, err error) {
	reader = &Reader{
		file: file{
			Hosts:          hosts,
			User:           user,
			FilePath:       name,
			client:         client,
			externalClient: true,
		},
	}

	reader.reader, err = reader.client.Open(name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HDFS reader")
	}

	return reader, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	var cnt int
	ln := len(p)

	for n < ln {
		cnt, err = r.reader.Read(p[n:])

		n += cnt

		if err != nil {
			break
		}
	}
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

func (r *Reader) Close() (err error) {
	if r.reader != nil {
		err = r.reader.Close()
		r.reader = nil

		if err != nil {
			return errors.Wrap(err, "failed to close HDFS reader")
		}
	}

	return r.file.Close()
}
