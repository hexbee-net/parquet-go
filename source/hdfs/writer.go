package hdfs

import (
	"github.com/colinmarc/hdfs/v2"
	"github.com/hexbee-net/errors"
)

type Writer struct {
	file

	writer *hdfs.FileWriter
}

func NewWriter(hosts []string, user string, name string) (writer *Writer, err error) {
	writer = &Writer{
		file: file{
			Hosts:          hosts,
			User:           user,
			FilePath:       name,
			externalClient: false,
		},
	}

	writer.client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hosts,
		User:      user,
	})

	if err != nil {
		return nil, errors.Wrap(err, "failed to create HDFS client")
	}

	writer.writer, err = writer.client.Create(name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HDFS writer")
	}

	return writer, nil
}

func NewWriterWithClient(client *hdfs.Client, hosts []string, user string, name string) (writer *Writer, err error) {
	writer = &Writer{
		file: file{
			Hosts:          hosts,
			User:           user,
			FilePath:       name,
			client:         client,
			externalClient: true,
		},
	}

	writer.writer, err = writer.client.Create(name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HDFS writer")
	}

	return writer, nil
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w *Writer) Close() (err error) {
	if w.writer != nil {
		err = w.writer.Close()
		w.writer = nil

		if err != nil {
			return errors.Wrap(err, "failed to close HDFS writer")
		}
	}

	return w.file.Close()
}
