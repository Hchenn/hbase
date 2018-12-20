package hbase

import (
	"github.com/bitly/go-simplejson"
	"log"
	"os"
)

// HBaseZKOption 配置类
type HBaseZKOption struct {
	Root 	string
	Hosts	string
}

// NewHbaseZKOptionFromJSON 创建 HbaseZKOption 配置类
func NewHbaseZKOptionFromJSON(js *simplejson.Json, configName string) *HBaseZKOption {
	conf := js.Get(configName)
	configMap, err := conf.Map()
	if err != nil {
		log.Fatalf("Parse HBase Config Error:%h, configName: %h", err.Error(), configName)
		os.Exit(-1)
	}
	var r HBaseZKOption
	r.Root = configMap["root"].(string)
	r.Hosts = configMap["hosts"].(string)
	return &r
}
