package demo

import (
	"context"
	"sync"
	"time"
)

type TableModel struct {
	ColumnOne	[]byte	`hbase:"family:demo_family_1;column:demo_column_1"`
	ColumnTwo	[]byte	`hbase:"family:demo_family_2;column:demo_column_2"`
	ColumnThree	[]byte	`hbase:"family:demo_family_3;column:demo_column_1"`
	ColumnFour	[]byte	`hbase:"family:demo_family_3;column:demo_column_3"`
}

func (TableModel) TableName() string {
	return "demo_table_name"
}

type TableModelDao struct {
}

var dao *TableModelDao
var once sync.Once

// NewTableModelDaoInstance
func NewTableModelDaoInstance() *TableModelDao {
	once.Do(
		func() {
			dao = &TableModelDao{}
		})
	return dao
}

func (d *TableModelDao) ReadRow(ctx context.Context, key string, expire time.Duration) (*TableModel, error) {
	var m TableModel
	err := demoHB.ReadRow(ctx, key, expire, &m).Error
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (d *TableModelDao) WriteRow(ctx context.Context, key string, m *TableModel) error {
	err := demoHB.WriteRow(ctx, key, m).Error
	if err != nil {
		return err
	}
	return nil
}
