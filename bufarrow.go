package bufarrow

import (
	"context"
	"errors"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"google.golang.org/protobuf/proto"
)

type Schema[T proto.Message] struct {
	msg *message
}

func New[T proto.Message](mem memory.Allocator) (schema *Schema[T], err error) {
	defer func() {
		e := recover()
		if e != nil {
			switch x := e.(type) {
			case error:
				err = x
			case string:
				err = errors.New(x)
			default:
				panic(x)
			}
		}
	}()
	var a T
	b := build(a.ProtoReflect())
	b.build(mem)
	schema = &Schema[T]{msg: b}
	return
}

// Append appends protobuf value to the schema builder.This method is not safe
// for concurrent use.
func (s *Schema[T]) Append(value T) {
	s.msg.append(value.ProtoReflect())
}

// NewRecord returns buffered builder value as an arrow.Record. The builder is
// reset and can be reused to build new records.
func (s *Schema[T]) NewRecord() arrow.Record {
	return s.msg.NewRecord()
}

// Parquet returns schema as parquet schema
func (s *Schema[T]) Parquet() *schema.Schema {
	return s.msg.Parquet()
}

// Schema returns schema as arrow schema
func (s *Schema[T]) Schema() *arrow.Schema {
	return s.msg.schema
}

// FieldNames returns top-level field names
func (s *Schema[T]) FieldNames() []string {
	var fieldNames []string
	fieldNames = make([]string, 0, len(s.msg.schema.Fields()))
	for _, f := range s.msg.schema.Fields() {
		fieldNames = append(fieldNames, f.Name)
	}
	return fieldNames
}

// ReadParquet specified columns from parquet source r and returns an Arrow record. The returned record must
// be released by the caller.
func (s *Schema[T]) ReadParquet(ctx context.Context, r parquet.ReaderAtSeeker, columns []int) (arrow.Record, error) {
	return s.msg.Read(ctx, r, columns)
}

// WriteParquet writes Parquet to an io.Writer
func (s *Schema[T]) WriteParquet(w io.Writer) error {
	return s.msg.WriteParquet(w)
}

// WriteParquetRecords write one or many Arrow records to parquet
func (s *Schema[T]) WriteParquetRecords(w io.Writer, records ...arrow.Record) error {
	return s.msg.WriteParquetRecords(w, records...)
}

// Proto decodes rows and returns them as proto messages.
func (s *Schema[T]) Proto(r arrow.Record, rows []int) []T {
	return unmarshal[T](s.msg.root, r, rows)
}

// Release releases the reference on the message builder
func (s *Schema[T]) Release() {
	s.msg.builder.Release()
}
