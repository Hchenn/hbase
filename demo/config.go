package demo

import (
	"github.com/bitly/go-simplejson"
	"io/ioutil"
)

var DBSettings *simplejson.Json


// NewConfig 从文件中加载一个配置实例
func NewConfig(file string) error {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	DBSettings, err = simplejson.NewJson(content)
	if err != nil {
		return err
	}

	return nil
}
