package demo

import (
	"testing"
	"context"
)

// TestReadRow
func TestReadRow(t *testing.T) {
	t.Skip()

	// read config file
	NewConfig("conf_demo.json")
	InitHBase()

	key := "test01"
	ctx := context.Background()
	v, err := NewTableModelDaoInstance().ReadRow(ctx, key, 0)
	if err != nil {
		t.Fatalf("ReadRow error: %s", err.Error())
	}
	t.Logf("ReadRow success, column: %s, %s ...", string(v.ColumnOne), string(v.ColumnTwo))
}


// TestWriteRow
func TestWriteRow(t *testing.T) {
	t.Skip()

	// read config file
	NewConfig("conf_demo.json")
	InitHBase()

	key := "test01"
	ctx := context.Background()
	row := &TableModel{
		ColumnOne:	[]byte(`{"column_one": "one"}`),
		ColumnTwo:	[]byte(`{"column_two": 2}`),
	}
	err := NewTableModelDaoInstance().WriteRow(ctx, key, row)
	if err != nil {
		t.Fatalf("WriteRow error: %s", err.Error())
	}
	t.Logf("WriteRow success, row: %+v", row)
}
