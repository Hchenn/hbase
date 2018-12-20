package demo

import (
	"github.com/Hchenn/hbase"
	"log"
	"os"
)

var demoHB = hbase.NewHBaseZK()

func InitHBase() error {

	configName := "demo_hbase"
	option := hbase.NewHbaseZKOptionFromJSON(DBSettings, configName)
	if demoHB.Init(option) {
		log.Printf("HBase:%s Connection success !!!\n", configName)
	} else {
		log.Fatalf("HBase:%s Connection Error !!!", configName)
		os.Exit(-1)
	}
	return nil
}
