package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/compression"
	"github.com/hexbee-net/parquet/layout"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema"
	"github.com/hexbee-net/parquet/source"
)

const (
	magic         = "PAR1"
	magicLen      = len(magic)
	footerLenSize = 4
	footerLen     = int64(footerLenSize + magicLen)
)

// FileReader is used to read data from a parquet file.
// Always use NewFileReader to create such an object.
type FileReader struct {
	schema.Reader

	meta   *parquet.FileMetaData
	reader source.Reader

	chunkReader *layout.ChunkReader

	rowGroupPosition int
	currentRecord    int64
	skipRowGroup     bool
}

// NewFileReader creates a new FileReader.
// You can limit the columns that are read by providing the names of
// the specific columns to read using dotted notation.
// If no columns are provided, then all columns are read.
func NewFileReader(r source.Reader, columns ...string) (*FileReader, error) {
	meta, err := readFileMetaData(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read file meta data")
	}

	s, err := readFileSchema(meta)
	if err != nil {
		return nil, errors.Wrap(err, "creating schema failed")
	}

	s.SetSelectedColumns(columns...)

	// Reset the reader to the beginning of the file
	if _, err := r.Seek(int64(magicLen), io.SeekStart); err != nil {
		return nil, err
	}

	return &FileReader{
		Reader:      s,
		meta:        meta,
		reader:      r,
		chunkReader: layout.NewChunkReader(defaultCompressors()),
	}, nil
}

// CurrentRowGroup returns information about the current row group.
func (f *FileReader) CurrentRowGroup() *parquet.RowGroup {
	if f == nil || f.meta == nil || f.meta.RowGroups == nil || f.rowGroupPosition-1 >= len(f.meta.RowGroups) {
		return nil
	}

	return f.meta.RowGroups[f.rowGroupPosition-1]
}

// RowGroupCount returns the number of row groups in the parquet file.
func (f *FileReader) RowGroupCount() int {
	return len(f.meta.RowGroups)
}

// NumRows returns the number of rows in the parquet file. This information is directly taken from
// the file's meta data.
func (f *FileReader) NumRows() int64 {
	return f.meta.NumRows
}

// RowGroupNumRows returns the number of rows in the current RowGroup.
func (f *FileReader) RowGroupNumRows() (int64, error) {
	if err := f.advanceIfNeeded(); err != nil {
		return 0, err
	}

	return f.Reader.RowGroupNumRecords(), nil
}

// NextRow reads the next row from the parquet file. If required, it will load the next row group.
func (f *FileReader) NextRow() (map[string]interface{}, error) {
	if err := f.advanceIfNeeded(); err != nil {
		return nil, err
	}

	f.currentRecord++

	return f.Reader.GetData()
}

// SkipRowGroup skips the currently loaded row group and advances to the next row group.
func (f *FileReader) SkipRowGroup() {
	f.skipRowGroup = true
}

// PreLoad is used to load the row group if required. It does nothing if the row group is already loaded.
func (f *FileReader) PreLoad() error {
	return f.advanceIfNeeded()
}

// MetaData returns a map of metadata key-value pairs stored in the parquet file.
func (f *FileReader) MetaData() map[string]string {
	return metaDataToMap(f.meta.KeyValueMetadata)
}

// ColumnMetaData returns a map of metadata key-value pairs for the provided column
// in the current row group.
// The column name has to be provided in its dotted notation.
func (f *FileReader) ColumnMetaData(colName string) (map[string]string, error) {
	for _, col := range f.CurrentRowGroup().Columns {
		if colName == strings.Join(col.MetaData.PathInSchema, ".") {
			return metaDataToMap(col.MetaData.KeyValueMetadata), nil
		}
	}

	return nil, errors.WithFields(
		errors.New("column not found"),
		errors.Fields{
			"name": colName,
		})
}

func (f *FileReader) advanceIfNeeded() error {
	if f.rowGroupPosition == 0 || f.currentRecord >= f.Reader.RowGroupNumRecords() || f.skipRowGroup {
		if err := f.readRowGroup(); err != nil {
			f.skipRowGroup = true
			return err
		}

		f.currentRecord = 0
		f.skipRowGroup = false
	}

	return nil
}

// readRowGroup read the next row group into memory.
func (f *FileReader) readRowGroup() error {
	if len(f.meta.RowGroups) <= f.rowGroupPosition {
		return io.EOF
	}

	f.rowGroupPosition++

	rowGroups := f.meta.RowGroups[f.rowGroupPosition-1]

	f.Reader.ResetData()
	f.Reader.SetNumRecords(rowGroups.NumRows)

	for _, c := range f.Reader.Columns() {
		chunk := rowGroups.Columns[c.Index()]

		if !f.Reader.IsSelected(c.FlatName()) {
			if err := layout.SkipChunk(f.reader, c, chunk); err != nil {
				return err
			}

			c.SetSkipped(true)

			continue
		}

		pages, err := f.chunkReader.ReadChunk(f.reader, c, chunk)
		if err != nil {
			return errors.Wrap(err, "failed to read data chunk")
		}

		if err := readPageData(c, pages); err != nil {
			return errors.Wrap(err, "failed to read page data")
		}
	}

	return nil
}

func readFileMetaData(r io.ReadSeeker) (*parquet.FileMetaData, error) {
	buf := make([]byte, magicLen)

	// read and validate magic header
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "failed to seek to file magic header")
	}

	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, errors.Wrap(err, "failed to read file magic header failed")
	}

	if !bytes.Equal(buf, []byte(magic)) {
		return nil, errors.New("invalid parquet file header")
	}

	// read and validate footer
	if _, err := r.Seek(int64(-magicLen), io.SeekEnd); err != nil {
		return nil, errors.Wrap(err, "failed to seek to file magic footer")
	}

	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, errors.Wrap(err, "failed to read file magic footer failed")
	}

	if !bytes.Equal(buf, []byte(magic)) {
		return nil, errors.Errorf("invalid parquet file footer")
	}

	// read footer length
	var fl int32

	if _, err := r.Seek(-footerLen, io.SeekEnd); err != nil {
		return nil, errors.Wrap(err, "failed to seek to footer length")
	}

	if err := binary.Read(r, binary.LittleEndian, &fl); err != nil {
		return nil, errors.Wrap(err, "failed to read footer length")
	}

	if fl <= 0 {
		return nil, errors.WithFields(
			errors.New("invalid footer length"),
			errors.Fields{
				"length": fl,
			})
	}

	// read file metadata
	meta := &parquet.FileMetaData{}

	if _, err := r.Seek(-footerLen-int64(fl), io.SeekEnd); err != nil {
		return nil, errors.Wrap(err, "failed to seek to file meta data")
	}

	if err := readThrift(meta, io.LimitReader(r, int64(fl))); err != nil {
		return nil, errors.Wrap(err, "failed to read file meta data")
	}

	return meta, nil
}

func readFileSchema(meta *parquet.FileMetaData) (schema.Reader, error) {
	if len(meta.Schema) < 1 {
		return nil, errors.New("no schema element found")
	}

	s, err := schema.LoadSchema(meta.Schema[1:])
	if err != nil {
		return nil, errors.Wrap(err, "failed to read file schema from meta data")
	}

	return s, nil
}

func readPageData(col *schema.Column, pages []layout.PageReader) error {
	s := col.ColumnStore()

	for i := range pages {
		data := make([]interface{}, pages[i].NumValues())

		n, dl, rl, err := pages[i].ReadValues(data)
		if err != nil {
			return err
		}

		if int32(n) != pages[i].NumValues() {
			return errors.WithFields(
				errors.New("unexpected number of values"),
				errors.Fields{
					"expected": pages[i].NumValues(),
					"actual":   n,
				})
		}

		// using append to make sure we handle the multiple data page correctly
		if err := s.RepetitionLevels.AppendArray(rl); err != nil {
			return err
		}

		if err := s.DefinitionLevels.AppendArray(dl); err != nil {
			return err
		}

		s.Values.Values = append(s.Values.Values, data...)
		s.Values.NoDictMode = true
	}

	return nil
}

func metaDataToMap(kvMetaData []*parquet.KeyValue) map[string]string {
	data := make(map[string]string)

	for _, kv := range kvMetaData {
		if kv.Value != nil {
			data[kv.Key] = *kv.Value
		}
	}

	return data
}

func defaultCompressors() map[parquet.CompressionCodec]compression.BlockCompressor {
	return map[parquet.CompressionCodec]compression.BlockCompressor{
		parquet.CompressionCodec_UNCOMPRESSED: compression.Uncompressed{},
		parquet.CompressionCodec_SNAPPY:       compression.Snappy{},
		parquet.CompressionCodec_GZIP:         compression.GZip{},
		parquet.CompressionCodec_BROTLI:       compression.Brotli{},
		parquet.CompressionCodec_LZ4:          compression.LZ4{},
		parquet.CompressionCodec_ZSTD:         compression.ZStd{},
	}
}
