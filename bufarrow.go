package bufarrow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/proto"
)

type Schema[T proto.Message] struct {
	msg         *message
	stencil     T
	normBuilder *array.RecordBuilder
	opts        *Opt
}

type Opt struct {
	normalizerFieldStrings []string
	normalizerAliasStrings []string
	normalizerFields       []normField
	failOnRangeErr         bool
}

type normField struct {
	node                 *node
	nodePath             []*node
	normalizerListBitmap []bool
	normalizerListRanges [][]int
}

type (
	Option func(config)
	config *Opt
)

// WithNormalizer configures the scalars to add to a flat Arrow Record suitable for efficient aggregation.
// Fields should be specified by their path (field names separated by a period ie. 'field1.field2.field3').
// The Arrow field types of the selected fields will be used to build the new schema. If coaslescing
// data between multiple fields of the same type, specify only one of the paths.
// List fields should have an index to retrieve specified, otherwise defaults to all elements;
// ranges are not yet implemented.
// Current functionality is limited to valitating the fields/aliases match in `New()“, and
// `NormalizerBuilder()` returning an `*arrow.RecordBuilder` to be used externally to append data, and
// NewNormalizerRecord() to get an `arrow.Record` from the normalizer RecordBuilder.
// Future development may include Append methods that accept protopath operations to normalize protobuf
// messages in-flight internally to the package.
// failOnRangeError indicates whether to fail on a list[start:end] where end > len(list). TODO
func WithNormalizer(fields, aliases []string, failOnRangeError bool) Option {
	return func(cfg config) {
		for _, f := range fields {
			cfg.normalizerFieldStrings = append(cfg.normalizerFieldStrings, f)
		}
		for _, a := range aliases {
			cfg.normalizerAliasStrings = append(cfg.normalizerAliasStrings, a)
		}
		cfg.failOnRangeErr = failOnRangeError
	}
}

func (s *Schema[T]) validateNormalizerConfig() error {
	var err error
	if len(s.opts.normalizerFieldStrings) != len(s.opts.normalizerAliasStrings) {
		return fmt.Errorf("bufarrow: normalizer fields and aliases  have different lengths - %v vs %d", len(s.opts.normalizerFieldStrings), len(s.opts.normalizerAliasStrings))
	}
	s.opts.normalizerFields = make([]normField, len(s.opts.normalizerFieldStrings))
	for i, fullName := range s.opts.normalizerFieldStrings {
		path := strings.Split(fullName, ".")
		s.opts.normalizerFields[i].normalizerListBitmap = make([]bool, len(path))
		s.opts.normalizerFields[i].normalizerListRanges = make([][]int, len(path))
		for b := 0; b < len(s.opts.normalizerFields[i].normalizerListRanges); b++ {
			s.opts.normalizerFields[i].normalizerListRanges[b] = make([]int, 2)
		}
		var n *node = s.msg.root
		var cleanPath []string
		var nodePath []*node
		for j, nodeName := range path {
			cleanPath = append(cleanPath, strings.Split(nodeName, "[")[0])
			c, ok := n.hash[strings.Split(nodeName, "[")[0]]
			if !ok {
				return fmt.Errorf("bufarrow: normalizer fullname %s invalid at depth %d", fullName, j)
			}
			if c.field.Type.ID() == arrow.LIST {
				s.opts.normalizerFields[i].normalizerListBitmap[j] = true

				_, listRangeRaw, ok := strings.Cut(nodeName, "[")
				if ok {
					listRangeRaw, _, ok = strings.Cut(listRangeRaw, "]")
					if !ok {
						return fmt.Errorf("bufarrow: normalizer fullname %s invalid at depth %d", fullName, j)
					}
					// select all
					if listRangeRaw == "*" {
						s.opts.normalizerFields[i].normalizerListRanges[j][0] = 0
						s.opts.normalizerFields[i].normalizerListRanges[j][1] = -1
					}
					start, end, ok := strings.Cut(listRangeRaw, ":")
					if !ok {
						// only one element
						s.opts.normalizerFields[i].normalizerListRanges[j][0] = cast.ToInt(start)
						s.opts.normalizerFields[i].normalizerListRanges[j][1] = cast.ToInt(start) + 1
					} else {
						// start and end range
						if cast.ToInt(start) > cast.ToInt(end) {
							return fmt.Errorf("bufarrow: normalizer fullname %s invalid at depth %d", fullName, j)
						}
						s.opts.normalizerFields[i].normalizerListRanges[j][0] = cast.ToInt(start)
						s.opts.normalizerFields[i].normalizerListRanges[j][1] = cast.ToInt(end)
					}
				}
			}
			nodePath = append(nodePath, c)
			n = c
		}
		s.opts.normalizerFields[i].node, err = s.msg.root.getPath(cleanPath)
		if err != nil {
			return err
		}
		s.opts.normalizerFields[i].nodePath = nodePath
	}
	return nil
}

func New[T proto.Message](mem memory.Allocator, opts ...Option) (schema *Schema[T], err error) {
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
	o := new(Opt)
	for _, f := range opts {
		f(o)
	}
	schema = &Schema[T]{msg: b, stencil: a, opts: o}
	err = schema.validateNormalizerConfig()
	if err != nil {
		return nil, err
	}
	var normFields []arrow.Field
	for i, f := range schema.opts.normalizerFields {
		arrowField := f.node.field
		arrowField.Name = schema.opts.normalizerAliasStrings[i]
		normFields = append(normFields, arrowField)
	}
	if len(normFields) != 0 {
		sch := arrow.NewSchema(normFields, nil)
		schema.normBuilder = array.NewRecordBuilder(mem, sch)
	}
	return schema, err
}

func (s *Schema[T]) Clone(mem memory.Allocator) (schema *Schema[T], err error) {
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
	a := s.stencil
	b := build(a.ProtoReflect())
	b.build(mem)
	schema = &Schema[T]{msg: b, stencil: a, opts: s.opts}
	var normFields []arrow.Field
	for i, f := range schema.opts.normalizerFields {
		arrowField := f.node.field
		arrowField.Name = schema.opts.normalizerAliasStrings[i]
		normFields = append(normFields, arrowField)
	}
	if len(normFields) != 0 {
		sch := arrow.NewSchema(normFields, nil)
		schema.normBuilder = array.NewRecordBuilder(mem, sch)
	}
	return schema, err
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

func (s *Schema[T]) NormalizerBuilder() *array.RecordBuilder {
	return s.normBuilder
}

// NewNormalizerRecord returns buffered builder value as an arrow.Record. The builder is
// reset and can be reused to build new records.
func (s *Schema[T]) NewNormalizerRecord() arrow.Record {
	if s.normBuilder == nil {
		return nil
	}
	return s.normBuilder.NewRecord()
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
