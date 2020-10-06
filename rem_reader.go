// +build removeme

package parquet

import (
	"encoding/binary"
	"io"
	"reflect"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

const FooterSizeOffset = 8

type Reader struct {
	fileReader  FileReader
	metaData    *parquet.FileMetaData
	parallelism int
}

func NewReader() (FileReader, error) {
	panic("implement me")
}

// RowCount returns the number of rows in the parquet file.
func (r *Reader) RowCount() int64 {
	return r.metaData.NumRows
}

// RowGroupCount returns the number of row groups in the parquet file.
func (r *Reader) RowGroupCount() int {
	return len(r.metaData.RowGroups)
}

func (r *Reader) SkipRows(num int64) error {
	if num < 1 {
		return nil
	}

	//doneChan := make(chan int, r.parallelism)
	//taskChan := make(chan string, len(self.SchemaHandler.ValueColumns))
	//stopChan := make(chan int)

	panic("implement me")
}

func (r *Reader) SkipRowGroup() {
	panic("implement me")
}

func (r *Reader) MetaData() map[string]string {
	return metaDataToMap(r.metaData.KeyValueMetadata)
}

func (r *Reader) ColumnMetaData(colName string) map[string]string {
}

// readMetaData reads the meta-data of the current file.
func (r *Reader) readMetaData() error {
	size, err := r.getFooterSize()
	if err != nil {
		return errors.Wrap(err, "failed to read meta-data")
	}

	if _, err := r.fileReader.Seek(-int64(FooterSizeOffset+size), io.SeekEnd); err != nil {
		return errors.Wrap(err, "failed to set the read index to the meta-data location")
	}

	metaData := parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()

	return metaData.Read(pf.GetProtocol(thrift.NewStreamTransportR(r.fileReader)))
}

// getFooterSize reads the file of the footer in the current file.
func (r *Reader) getFooterSize() (uint32, error) {
	if _, err := r.fileReader.Seek(-FooterSizeOffset, io.SeekEnd); err != nil {
		return 0, errors.Wrap(err, "failed to set the read index to the footer size location")
	}

	buf := make([]byte, 4)

	if _, err := r.fileReader.Read(buf); err != nil {
		return 0, errors.Wrap(err, "failed to read the size of the parquet file footer")
	}

	size := binary.LittleEndian.Uint32(buf)
	return size, nil
}

func (r *Reader) read(dest interface{}, prefixPath string) error {
	ot := reflect.TypeOf(dest).Elem().Elem()
	num := reflect.ValueOf(dest).Elem().Len()

	if num < 1 {
		return nil
	}

	doneChan := make(chan int, r.parallelism)
	taskChan := make(chan string, len(self.SchemaHandler.ValueColumns))
	stopChan := make(chan int)

	for i := 0; i < r.parallelism; i++ {
		go func() {
			for {
				select {
				case <-stopChan:
					return
				case pathStr := <-taskChan:
					panic("implement me")
				}
			}
		}()
	}

	readNum := 0
}

func (r *Reader) readRowGroup(schema SchemaReader, rowGroups *parquet.RowGroup) error {
	if len(r.metaData.RowGroups) <= r.rowGroupPosition {
		return io.EOF
	}

	r.rowGroupPosition++

	return readRowGroup(r.fileReader, r.SchemaReader, r.metaData.RowGroups[r.rowGroupPosition-1])
}

func readRowGroup(reader FileReader, schema SchemaReader, rowGroups *parquet.RowGroup) error {
	dataCols := schema.Columns()
	schema.resetData()
	schema.setNumRecords(rowGroups.NumRows)

	for _, c := range dataCols {
		chunk := rowGroups.Columns[c.Index()]

		if !schema.isSelected(c.flatName) {
			if err := skipColumnChunk(reader, c, chunk); err != nil {
				return err
			}
			c.data.skipped = true
			continue
		}

		pages, err := readColumnChunk(reader, c, chunk)
		if err != nil {
			return errors.Wrap(err, "failed to read chunk")
		}

		if err := readPageData(c, pages); err != nil {
			return errors.Wrap(err, "failed to read page data")
		}
	}
}

func metaDataToMap(meta []*parquet.KeyValue) map[string]string {
	data := make(map[string]string)
	for _, kv := range meta {
		if kv.Value != nil {
			data[kv.Key] = *kv.Value
		}
	}
	return data
}
