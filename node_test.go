package arrow3

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/gernest/arrow3/gen/go/samples"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMessage_scalar(t *testing.T) {
	m := &samples.ScalarTypes{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar.txt", schema)
}

func TestMessage_scalarOptional(t *testing.T) {
	m := &samples.ScalarTypesOptional{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_optional.txt", schema)
}
func TestMessage_scalarRepeated(t *testing.T) {
	m := &samples.ScalarTypesRepeated{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_repeated.txt", schema)
}
func TestMessage_scalarMap(t *testing.T) {
	m := &samples.ScalarTypesMap{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_map.txt", schema)
}
func TestMessage_Nested00(t *testing.T) {
	m := &samples.Nested{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/nested00.txt", schema)
}
func TestMessage_KeyValue(t *testing.T) {
	m := &commonv1.KeyValue{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/otel_key_value.txt", schema)
}
func TestMessage_MetricsData(t *testing.T) {
	m := &metricsv1.MetricsData{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/otel_metrics_data.txt", schema)
}
func TestMessage_TraceData(t *testing.T) {
	m := &tracev1.TracesData{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/otel_trace_data.txt", schema)
}
func TestMessage_LogsData(t *testing.T) {
	m := &logsv1.LogsData{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/otel_logs_data.txt", schema)
}
func TestMessage_metricsData(t *testing.T) {
	m := &commonv1.KeyValue{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/otel_key_value.txt", schema)
}
func TestMessage_Cyclic(t *testing.T) {
	m := &samples.Cyclic{}

	err := func() (err error) {
		defer func() {
			err = recover().(error)
		}()
		build(m.ProtoReflect())
		return nil
	}()
	if !errors.Is(err, ErrMxDepth) {
		t.Errorf("expected %v got %v", ErrMxDepth, err)
	}
}

func TestMessage_OneOf(t *testing.T) {
	m := &samples.OneOfScala{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/one_of.txt", schema)
}
func TestAppendMessage_scalar(t *testing.T) {
	msg := &samples.ScalarTypes{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	msg.Uint64 = 1
	b.append(msg.ProtoReflect())

	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/scalar.json", string(data))
}
func TestAppendMessage_scalar_optional(t *testing.T) {
	msg := &samples.ScalarTypesOptional{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	u := uint64(1)
	x := []byte("hello")
	msg.Uint64 = &u
	msg.Bytes = x
	b.append(msg.ProtoReflect())
	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/scalar_optional.json", string(data))
}
func TestAppendMessage_scalar_repeated(t *testing.T) {
	msg := &samples.ScalarTypesRepeated{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	u := uint64(1)
	x := []byte("hello")
	msg.Uint64 = []uint64{u, u}
	msg.Bytes = [][]byte{x, x}
	b.append(msg.ProtoReflect())
	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/scalar_repeated.json", string(data))
}
func TestAppendMessage_scalar_map(t *testing.T) {
	msg := &samples.ScalarTypesMap{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	msg.Labels = map[string]string{
		"key": "value",
	}
	b.append(msg.ProtoReflect())
	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/scalar_map.json", string(data))
}
func TestAppendMessage_nested(t *testing.T) {
	msg := &samples.Nested{
		NestedRepeatedScalar: []*samples.ScalarTypes{{}},
		Deep:                 &samples.One{Two: &samples.Two{Three: &samples.Three{Value: 12}}},
	}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/scalar_nested.json", string(data))
}

func TestMessage_known(t *testing.T) {
	m := &samples.Known{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_known.txt", schema)
}

func TestAppendMessage_scalar_known(t *testing.T) {
	msg := &samples.Known{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	x, _ := time.Parse(time.RFC822, time.RFC822)
	now := timestamppb.New(x)
	dur := durationpb.New(time.Minute)
	msg.Ts = now
	msg.Duration = dur
	msg.TsRep = []*timestamppb.Timestamp{now}
	msg.DurationRep = []*durationpb.Duration{dur}
	b.append(msg.ProtoReflect())

	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/scalar_known.json", string(data))
}
func TestAppendMessage_scalar_oneof(t *testing.T) {
	msg := &samples.OneOfScala{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	msg.Value = &samples.OneOfScala_Uint64{
		Uint64: 10,
	}
	b.append(msg.ProtoReflect())

	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/one_of.json", string(data))
}
func TestAppendMessage_otelKeyValue(t *testing.T) {
	msg := &commonv1.KeyValue{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	msg.Key = "hello"
	msg.Value = &commonv1.AnyValue{
		Value: &commonv1.AnyValue_StringValue{
			StringValue: "world",
		},
	}
	b.append(msg.ProtoReflect())
	r := b.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/otel_key_value.json", string(data))
}

func TestSchema_MetricsData(t *testing.T) {
	m := &metricsv1.MetricsData{}
	msg := build(m.ProtoReflect())
	schema, err := msg.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/otel_metrics_data_schema.txt", schema.String())
}
func TestSchema_TracesData(t *testing.T) {
	m := &tracev1.TracesData{}
	msg := build(m.ProtoReflect())
	schema, err := msg.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/otel_traces_data_schema.txt", schema.String())
}
func TestSchema_LogssData(t *testing.T) {
	m := &logsv1.LogsData{}
	msg := build(m.ProtoReflect())
	schema, err := msg.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/otel_logs_data_schema.txt", schema.String())
}

func match(t testing.TB, path string, value string, write ...struct{}) {
	t.Helper()
	if len(write) > 0 {
		os.WriteFile(path, []byte(value), 0600)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("failed reading file %s", path)
	}
	if string(b) != value {
		t.Errorf("------> want \n%s\n------> got\n%s", string(b), value)
	}
}
