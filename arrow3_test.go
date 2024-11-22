package bufarrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrow/gen/go/samples"
)

func TestNew(t *testing.T) {
	schema, err := New[*samples.Three](memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer schema.Release()
	schema.Append(&samples.Three{
		Value: 10,
	})
	r := schema.NewRecord()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/new.json", string(data))
}
