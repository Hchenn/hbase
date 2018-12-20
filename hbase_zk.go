package hbase

import (
	"github.com/tsuna/gohbase"
	"context"
	"time"
)

// HBase Zookeeper
type HBaseZK struct {
	option	*HBaseZKOption
	client	gohbase.Client
}

// NewHBaseZK  创建 HBaseZK 对象
func NewHBaseZK() *HBaseZK {
	return &HBaseZK{}
}

// Init 初始化 Hbase 对象池, 构建可读写, 以及只读的Hbase对象实例
func (h *HBaseZK) Init(option *HBaseZKOption) bool {
	h.option = option
	p := h.initHbasePool()
	return p
}

// ReadRow
func (h *HBaseZK) ReadRow(ctx context.Context, key string, expire time.Duration, out interface{}) *HBase {
	if ctx == nil {
		ctx = context.Background()
	}
	obj := &HBase{
		Key:	key,
		Value:	out,
		Ctx:	ctx,
		Expire:	expire,
	}
	obj.Find(h.client)
	return obj
}

// WriteRow
func (h *HBaseZK) WriteRow(ctx context.Context, key string, value interface{}) *HBase {
	if ctx == nil {
		ctx = context.Background()
	}
	obj := &HBase{
		Key:	key,
		Value:	value,
		Ctx:	ctx,
	}
	obj.Create(h.client)
	return obj
}

func (h *HBaseZK) initHbasePool() bool {
	if h.option.Root == "" || h.option.Hosts == "" {
		return false
	}
	option := gohbase.ZookeeperRoot(h.option.Root)
	h.client = gohbase.NewClient(h.option.Hosts, option)
	return true
}
