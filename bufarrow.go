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
	pb "github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	dpb "google.golang.org/protobuf/types/dynamicpb"
)

type Schema[T proto.Message] struct {
	msg           *message
	stencil       T
	stencilCustom proto.Message
	normBuilder   *array.RecordBuilder
	opts          *Opt
}

type Opt struct {
	customFields           []CustomField
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

// Cardinality determines whether a field is optional, required, or repeated.
type Cardinality protoreflect.Cardinality

// Constants as defined by the google.protobuf.Cardinality enumeration.
const (
	Optional Cardinality = 1 // appears zero or one times
	Required Cardinality = 2 // appears exactly one time; invalid with Proto3
	Repeated Cardinality = 3 // appears zero or more times
)

func (c *Cardinality) Get() protoreflect.Cardinality {
	switch *c {
	case Optional:
		return protoreflect.Optional
	case Required:
		return protoreflect.Required
	case Repeated:
		return protoreflect.Repeated
	}
	return protoreflect.Optional
}

type FieldType fieldType
type fieldType string

const (
	BOOL    FieldType = "bool"
	BYTES   FieldType = "[]byte"
	STRING  FieldType = "string"
	INT64   FieldType = "int64"
	FLOAT64 FieldType = "float64"
)

type CustomField struct {
	// Name must not conflict with existing proto.Message field names.
	Name string `toml:"name"`
	// Supported types:
	// 		BOOL	bool
	// 		BYTES	[]byte
	//		STRING	string
	//		INT64	int64
	//		FLOAT64	float64
	Type FieldType `toml:"type"`
	// FieldCardinality is a type alias of protoreflect.Cardinality.
	// Cardinality determines whether a field is optional, required, or repeated.
	// const (
	// 	Optional Cardinality = 1 // appears zero or one times
	// 	Required Cardinality = 2 // appears exactly one time; invalid with Proto3
	// 	Repeated Cardinality = 3 // appears zero or more times
	// )
	// Constants as defined by the google.protobuf.Cardinality enumeration.
	FieldCardinality Cardinality `toml:"field_cardinality"`
	// IsPacked reports whether repeated primitive numeric kinds should be
	// serialized using a packed encoding.
	// If true, then it implies Cardinality is Repeated.
	IsPacked bool `toml:"is_packed"`
}

// WithCustomFields adds user-defined fields to the message schema which can be populated with AppendWithCustom().
func WithCustomFields(c []CustomField) Option {
	return func(cfg config) {
		for _, f := range c {
			cfg.customFields = append(cfg.customFields, f)
		}
	}
}

func (o *Opt) validateCustomFields() error {
	var err error
	for i, f := range o.customFields {
		if f.Name == "" {
			errors.Join(err, fmt.Errorf("custom field %d missing name", i))
		}
		if f.Type == "" {
			errors.Join(err, fmt.Errorf("custom field %d missing type", i))
		}
		if f.FieldCardinality == 0 {
			f.FieldCardinality = 1
		}
		if f.IsPacked == true && f.FieldCardinality != 3 {
			errors.Join(err, fmt.Errorf("custom field %d cannot be packed unless cardinality=repeated", i))
		}
	}
	return err
}

func (s *Schema[T]) addCustomFields() (proto.Message, error) {
	var a T
	msgBuilder, err := pb.FromMessage(a.ProtoReflect().Descriptor())
	if err != nil {
		return nil, err
	}
	for _, f := range s.opts.customFields {
		switch f.Type {
		case BOOL:
			msgBuilder.AddField(pb.NewField(protoreflect.Name(f.Name), pb.FieldTypeBool()).SetCardinality(f.FieldCardinality.Get()).SetOptions(&descriptorpb.FieldOptions{Packed: proto.Bool(f.IsPacked)}))
		case BYTES:
			msgBuilder.AddField(pb.NewField(protoreflect.Name(f.Name), pb.FieldTypeBytes()).SetCardinality(f.FieldCardinality.Get()).SetOptions(&descriptorpb.FieldOptions{Packed: proto.Bool(f.IsPacked)}))
		case STRING:
			msgBuilder.AddField(pb.NewField(protoreflect.Name(f.Name), pb.FieldTypeString()).SetCardinality(f.FieldCardinality.Get()).SetOptions(&descriptorpb.FieldOptions{Packed: proto.Bool(f.IsPacked)}))
		case INT64:
			msgBuilder.AddField(pb.NewField(protoreflect.Name(f.Name), pb.FieldTypeInt64()).SetCardinality(f.FieldCardinality.Get()).SetOptions(&descriptorpb.FieldOptions{Packed: proto.Bool(f.IsPacked)}))
		case FLOAT64:
			msgBuilder.AddField(pb.NewField(protoreflect.Name(f.Name), pb.FieldTypeFloat()).SetCardinality(f.FieldCardinality.Get()).SetOptions(&descriptorpb.FieldOptions{Packed: proto.Bool(f.IsPacked)}))
		}
	}
	md, err := msgBuilder.Build()
	if err != nil {
		return nil, err
	}
	msg := dpb.NewMessage(md).ProtoReflect()
	return msg.(proto.Message), err
}

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

// New returns a new bufarrow.Schema.
// Options include WithNormalizer and WithCustomFields.
// WithNormalizer creates a separate Arrow record whilst WithCustomFields expands the schema
// of the proto.Message used as the type parameter.
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

	o := new(Opt)
	for _, f := range opts {
		f(o)
	}
	schema = &Schema[T]{stencil: a, opts: o}
	var b *message
	if len(o.customFields) > 0 {
		err := o.validateCustomFields()
		if err != nil {
			return nil, err
		}
		schema.stencilCustom, err = schema.addCustomFields()
		if err != nil {
			return nil, err
		}
		c := schema.stencilCustom
		b = build(c.ProtoReflect())
		b.build(mem)
	} else {
		b = build(a.ProtoReflect())
		b.build(mem)
	}

	schema.msg = b

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

// Clone returns an identical bufarrow.Schema. Use in concurrency scenarios as Schema methods
// are not concurrency safe.
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
	switch len(s.opts.customFields) {
	case 0:
		a := s.stencil
		b := build(a.ProtoReflect())
		b.build(mem)
		schema = &Schema[T]{msg: b, stencil: a, opts: s.opts}
	default:
		a := s.stencilCustom
		b := build(a.ProtoReflect())
		b.build(mem)
		schema = &Schema[T]{msg: b, stencil: s.stencil, stencilCustom: a, opts: s.opts}
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

// Append appends protobuf value to the schema builder. This method is not safe
// for concurrent use.
func (s *Schema[T]) Append(value T) {
	s.msg.append(value.ProtoReflect())
}

// AppendWithCustom appends protobuf value and custom field values to the schema builder.
// This method is not safe for concurrent use. The number of custom field values must match
// the number of custom fields.
// Supported types:
//
//	bool
//	[]byte
//	string
//	int64
//	float64
func (s *Schema[T]) AppendWithCustom(value T, c ...any) error {
	if len(s.opts.customFields) != len(c) {
		return fmt.Errorf("custom fields values mismatch, got %d expected %d", len(c), len(s.opts.customFields))
	}
	m, err := s.mergeCustomFieldData(value, c)
	if err != nil {
		return err
	}
	s.msg.append(m.ProtoReflect())
	return nil
}

func (s *Schema[T]) mergeCustomFieldData(value T, c []any) (proto.Message, error) {
	// Clone proto.Message containing custom fields
	v := proto.Clone(s.stencilCustom)
	// Marshal stock message to bytes
	vb, err := proto.Marshal(value)
	if err != nil {
		return nil, err
	}
	// Unmarshal stock message into empty proto.Message containing custom fields
	err = proto.Unmarshal(vb, v)
	if err != nil {
		return nil, err
	}
	// Populate custom fields
	for i, f := range c {
		fd := v.ProtoReflect().Descriptor().Fields().ByTextName(s.opts.customFields[i].Name)
		switch s.opts.customFields[i].Type {
		case BOOL:
			switch f.(type) {
			case bool:
				fv := protoreflect.ValueOfBool(f.(bool))
				v.ProtoReflect().Set(fd, fv)
			default:
				return nil, fmt.Errorf("incorrect type %T on value %d, expected bool", f, i)
			}
		case BYTES:
			switch f.(type) {
			case []byte:
				fv := protoreflect.ValueOfBytes(f.([]byte))
				v.ProtoReflect().Set(fd, fv)
			default:
				return nil, fmt.Errorf("incorrect type %T on value %d, expected bool", f, i)
			}
		case STRING:
			switch f.(type) {
			case string:
				fv := protoreflect.ValueOfString(f.(string))
				v.ProtoReflect().Set(fd, fv)
			default:
				return nil, fmt.Errorf("incorrect type %T on value %d, expected string", f, i)
			}
		case INT64:
			switch f.(type) {
			case int64:
				fv := protoreflect.ValueOfInt64(f.(int64))
				v.ProtoReflect().Set(fd, fv)
			default:
				return nil, fmt.Errorf("incorrect type %T on value %d, expected int64", f, i)
			}
		case FLOAT64:
			switch f.(type) {
			case float64:
				fv := protoreflect.ValueOfFloat64(f.(float64))
				v.ProtoReflect().Set(fd, fv)
			default:
				return nil, fmt.Errorf("incorrect type %T on value %d, expected float64", f, i)
			}
		default:
			return nil, fmt.Errorf("unsupported type %T on value %d %v", f, i, f)
		}
	}
	return v, nil
}

// NewRecord returns buffered builder value as an arrow.Record. The builder is
// reset and can be reused to build new records.
func (s *Schema[T]) NewRecord() arrow.Record {
	return s.msg.NewRecord()
}

// NormalizerBuilder returns the Normalizer's Arrow array.RecordBuilder, to be used to append
// normalized data.
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
