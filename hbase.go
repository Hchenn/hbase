package hbase

import (
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase"
	"context"
	"reflect"
	"strings"
	"errors"
	"time"
)

// HBase
type HBase struct {
	Key		string
	Value   interface{}
	Error   error

	Ctx 	context.Context
	Expire  time.Duration

	values	map[string]map[string][]byte
}

type tabler interface {
	TableName() string
}

// TableName return table name
func (b *HBase) tableName() string {
	if t, ok := b.Value.(tabler); ok {
		return t.TableName()
	}
	// TODO return Model reflect Name
	return ""
}

func (b *HBase) getKey() string {
	return b.Key
}

func (b *HBase) getValues() map[string]map[string][]byte {
	if b.values == nil {
		b.values = make(map[string]map[string][]byte)
	}
	return b.values
}

type HBaseTag struct {
	Family	string
	Cloumn	string
}

func (b *HBase) parseTag(tag string) (t *HBaseTag, err error) {
	t = &HBaseTag{}
	splits := strings.Split(tag, ";")
	for _, v := range splits {
		kv := strings.Split(v, ":")
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "family":
			t.Family = kv[1]
		case "column":
			t.Cloumn = kv[1]
		}
	}
	if t.Cloumn == "" {
		err = errors.New("missing hbase tag: column")
	}
	if t.Family == "" {
		err = errors.New("missing hbase tag: family")
	}
	return t, err
}

func (b *HBase) isExpire(timestamp uint64) bool {
	if uint64(b.Expire) == 0 {
		return false
	}
	expire := uint64(b.Expire.Nanoseconds() / (1000 * 1000))
	now := uint64(time.Now().Unix() * 1000)	// ms
	if now - timestamp > expire {
		return true
	}
	return false
}

// reflect map[string]map[string][]byte
func (b *HBase) cycleModel(obj interface{}, function func(tag *HBaseTag, value *reflect.Value)) error {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return errors.New("value is not struct")
	}

	t := reflect.TypeOf(v.Interface())
	for i := 0; i < t.NumField(); i++ {
		value := v.Field(i)
		for value.Kind() == reflect.Ptr {
			value = value.Elem()
		}
		if value.Kind() == reflect.Struct {
			b.cycleModel(value.Interface(), function)
			continue
		}
		tag, err := b.parseTag(t.Field(i).Tag.Get("hbase"))
		if err != nil {
			return err
		}
		// TODO recover
		function(tag, &value)
	}
	return nil
}

func (b *HBase) readValues(resp *hrpc.Result) error {
	if b.values == nil {
		b.values = make(map[string]map[string][]byte)
	}
	if resp != nil && len(resp.Cells) > 0 {
		for _, cell := range resp.Cells {
			if b.isExpire(*cell.Timestamp) {
				continue
			}
			if _, ok := b.values[string(cell.Family)]; !ok {
				b.values[string(cell.Family)] = make(map[string][]byte)
			}
			b.values[string(cell.Family)][string(cell.Qualifier)] = cell.Value
		}
	}
	return b.cycleModel(b.Value, b.innerReadValues)
}

func (b *HBase) innerReadValues(tag *HBaseTag, value *reflect.Value) {
	if family, ok := b.values[tag.Family]; ok {
		if bytes, ok := family[tag.Cloumn]; ok {
			value.SetBytes(bytes)
		}
	}
}

func (b *HBase) writeValues() error {
	if b.values == nil {
		b.values = make(map[string]map[string][]byte)
	}
	return b.cycleModel(b.Value, b.innerWriteValues)
}

func (b *HBase) innerWriteValues(tag *HBaseTag, value *reflect.Value) {
	bytes := value.Bytes()
	if len(bytes) == 0 {
		return
	}
	if _, ok := b.values[tag.Family]; !ok {
		b.values[tag.Family] = make(map[string][]byte)
	}
	b.values[tag.Family][tag.Cloumn] = bytes
}

func (b *HBase) Find(client gohbase.Client) {
	req, err := hrpc.NewGetStr(b.Ctx, b.tableName(), b.getKey())
	resp, err := client.Get(req)
	if err != nil {
		b.Error = err
		return
	}
	b.Error = b.readValues(resp)
}

func (b *HBase) Create(client gohbase.Client) {
	b.Error = b.writeValues()
	if b.Error != nil {
		return
	}
	// put
	req, err := hrpc.NewPutStr(b.Ctx, b.tableName(), b.getKey(), b.getValues())
	if err != nil {
		b.Error = err
		return
	}
	_, b.Error = client.Put(req)
}
