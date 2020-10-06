package schema

// Reader is an interface with methods necessary in the FileReader.
type Reader interface {
	schemaCommon

	SetNumRecords(int64)
	GetData() (map[string]interface{}, error)
	SetSelectedColumns(selected ...string)
	IsSelected(string) bool
}
