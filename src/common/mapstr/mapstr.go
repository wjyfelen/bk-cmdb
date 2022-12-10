/*
 * Tencent is pleased to support the open source community by making 蓝鲸 available.
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package mapstr TODO
package mapstr

import (
	"bytes"
	"configcenter/src/common/blog"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"configcenter/src/common"

	"github.com/mohae/deepcopy"
)

// MapStr the common event data definition
type MapStr map[string]interface{}

// Clone create a new MapStr by deepcopy
func (cli MapStr) Clone() MapStr {
	cpyInst := deepcopy.Copy(cli)
	return cpyInst.(MapStr)
}

// Merge merge second into self,if the key is the same then the new value replaces the old value.
func (cli MapStr) Merge(second MapStr) {
	for key, val := range second {
		if strings.Contains(key, ".") {
			root := key[:strings.Index(key, ".")]
			if _, ok := cli[root]; ok && IsNil(cli[root]) {
				delete(cli, root)
			}
		}
		cli[key] = val
	}
}

// IsNil returns whether value is nil value, including map[string]interface{}{nil}, *Struct{nil}
func IsNil(value interface{}) bool {
	rflValue := reflect.ValueOf(value)
	if rflValue.IsValid() {
		return rflValue.IsNil()
	}
	return true
}

// ToMapInterface convert to map[string]interface{}
func (cli MapStr) ToMapInterface() map[string]interface{} {
	return cli
}

// ToStructByTag convert self into a struct with 'tagName'
//
//  eg:
//  self := MapStr{"testName":"testvalue"}
//  targetStruct := struct{
//      Name string `field:"testName"`
//  }
//  After call the function self.ToStructByTag(targetStruct, "field")
//  the targetStruct.Name value will be 'testvalue'
func (cli MapStr) ToStructByTag(targetStruct interface{}, tagName string) error {
	return SetValueToStructByTagsWithTagName(targetStruct, cli, tagName)
}

// MarshalJSONInto convert to the input value
func (cli MapStr) MarshalJSONInto(target interface{}) error {

	data, err := cli.ToJSON()
	if nil != err {
		return fmt.Errorf("marshal %#v failed: %v", target, err)
	}

	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()

	err = d.Decode(target)
	if err != nil {
		return fmt.Errorf("unmarshal %s failed: %v", data, err)
	}
	return nil
}

// ToJSON convert to json string
func (cli MapStr) ToJSON() ([]byte, error) {
	js, err := json.Marshal(cli)
	if err != nil {
		return nil, err
	}
	return js, nil
}

// Get return the origin value by the key
func (cli MapStr) Get(key string) (val interface{}, exists bool) {

	val, exists = cli[key]
	return val, exists
}

// Set set a new value for the key, the old value will be replaced
func (cli MapStr) Set(key string, value interface{}) {
	cli[key] = value
}

// Reset  reset the mapstr into the init state
func (cli MapStr) Reset() {
	for key := range cli {
		delete(cli, key)
	}
}

// Bool get the value as bool
func (cli MapStr) Bool(key string) (bool, error) {
	switch t := cli[key].(type) {
	case nil:
		return false, fmt.Errorf("the key (%s) is invalid", key)
	default:
		return false, fmt.Errorf("the key (%s) is invalid", key)
	case bool:
		return t, nil
	}
}

// Int64 return the value by the key
func (cli MapStr) Int64(key string) (int64, error) {
	switch t := cli[key].(type) {
	default:
		return 0, errors.New("invalid num")
	case nil:
		return 0, errors.New("invalid key(" + key + "), not found value")
	case int:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case float32:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case uint:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case json.Number:
		num, err := t.Int64()
		return int64(num), err
	case string:
		return strconv.ParseInt(t, 10, 64)
	}
}

// Float get the value as float64
func (cli MapStr) Float(key string) (float64, error) {
	switch t := cli[key].(type) {
	default:
		return 0, errors.New("invalid num")
	case nil:
		return 0, errors.New("invalid key, not found value")
	case int:
		return float64(t), nil
	case int16:
		return float64(t), nil
	case int32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	case json.Number:
		return t.Float64()
	}
}

// String get the value as string
func (cli MapStr) String(key string) (string, error) {
	switch t := cli[key].(type) {
	case nil:
		return "", nil
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(float64(t), 'f', -1, 64), nil
	case map[string]interface{}, []interface{}:
		rest, err := json.Marshal(t)
		if nil != err {
			return "", err
		}
		return string(rest), nil
	case json.Number:
		return t.String(), nil
	case string:
		return t, nil
	default:
		return fmt.Sprintf("%v", t), nil
	}
}

// Time get the value as time.Time
func (cli MapStr) Time(key string) (*time.Time, error) {
	switch t := cli[key].(type) {
	default:
		return nil, errors.New("invalid time value")
	case nil:
		return nil, errors.New("invalid key")
	case time.Time:
		return &t, nil
	case *time.Time:
		return t, nil
	case string:
		if tm, tmErr := time.Parse(time.RFC1123, t); nil == tmErr {
			return &tm, nil
		}

		if tm, tmErr := time.Parse(time.RFC1123Z, t); nil == tmErr {
			return &tm, nil
		}

		if tm, tmErr := time.Parse(time.RFC3339, t); nil == tmErr {
			return &tm, nil
		}

		if tm, tmErr := time.Parse(time.RFC3339Nano, t); nil == tmErr {
			return &tm, nil
		}

		if tm, tmErr := time.Parse(time.RFC822, t); nil == tmErr {
			return &tm, nil
		}

		if tm, tmErr := time.Parse(time.RFC822Z, t); nil == tmErr {
			return &tm, nil
		}

		if tm, tmErr := time.Parse(time.RFC850, t); nil == tmErr {
			return &tm, nil
		}

		return nil, errors.New("can not parse the datetime")
	}
}

// MapStr get the MapStr object
func (cli MapStr) MapStr(key string) (MapStr, error) {

	switch t := cli[key].(type) {
	default:
		return nil, fmt.Errorf("the value of the key(%s) is not a map[string]interface{} type", key)
	case nil:
		if _, ok := cli[key]; ok {
			return MapStr{}, nil
		}
		return nil, errors.New("the key is invalid")
	case MapStr:
		return t, nil
	case map[string]interface{}:
		return MapStr(t), nil
	}

}

// MapStrArray get the MapStr object array
func (cli MapStr) MapStrArray(key string) ([]MapStr, error) {

	switch t := cli[key].(type) {
	default:
		val := reflect.ValueOf(cli[key])
		switch val.Kind() {
		default:
			return nil, fmt.Errorf("the value of the key(%s) is not a valid type,%s", key, val.Kind().String())
		case reflect.Slice:
			tmpval, ok := val.Interface().([]MapStr)
			if ok {
				return tmpval, nil
			}

			return nil, fmt.Errorf("the value of the key(%s) is not a valid type,%s", key, val.Kind().String())
		}

	case nil:
		return nil, fmt.Errorf("the key(%s) is invalid", key)
	case []MapStr:
		return t, nil
	case []map[string]interface{}:
		items := make([]MapStr, 0)
		for _, item := range t {
			items = append(items, item)
		}
		return items, nil
	case []interface{}:
		items := make([]MapStr, 0)
		for _, item := range t {
			switch childType := item.(type) {
			case map[string]interface{}:
				items = append(items, childType)
			case MapStr:
				items = append(items, childType)
			case nil:
				continue
			default:
				return nil, fmt.Errorf("the value of the key(%s) is not a valid type, type: %v,value:%+v", key, childType, t)
			}
		}
		return items, nil
	}

}

// ForEach for each the every item
func (cli MapStr) ForEach(callItem func(key string, val interface{}) error) error {

	for key, val := range cli {
		if err := callItem(key, val); nil != err {
			return err
		}
	}

	return nil
}

// Remove delete the item by the key and return the deleted one
func (cli MapStr) Remove(key string) interface{} {

	if val, ok := cli[key]; ok {
		delete(cli, key)
		return val
	}

	return nil
}

// Exists check the key exists
func (cli MapStr) Exists(key string) bool {
	_, ok := cli[key]
	return ok
}

// IsEmpty check the empty status
func (cli MapStr) IsEmpty() bool {
	return len(cli) == 0
}

// Different the current value is different from the content of the given data
func (cli MapStr) Different(target MapStr) (more MapStr, less MapStr, changes MapStr) {

	// init
	more = make(MapStr)
	less = make(MapStr)
	changes = make(MapStr)

	// check more
	cli.ForEach(func(key string, val interface{}) error {
		if targetVal, ok := target[key]; ok {

			if !reflect.DeepEqual(val, targetVal) {
				changes[key] = val
			}
			return nil
		}

		more.Set(key, val)
		return nil
	})

	// check less
	target.ForEach(func(key string, val interface{}) error {
		if !cli.Exists(key) {
			less[key] = val
		}
		return nil
	})

	return more, less, changes
}

type convertFunc func(string) string

type action string

const (
	replaceAll     action = "replaceAll"
	replace        action = "replace"
	noAction       action = "skip"
	recursion      action = "recursion"
	prepareReplace action = "prepareReplace"
)

// getSymbolType 根据不同的mongo操作符号返回对应的需要解析的下一步动作
func getSymbolType(symbol string) action {

	symbolTypeMap := map[string]action{
		common.BKDBIN:               replaceAll,
		common.BKDBNIN:              replaceAll,
		common.BKDBAll:              replaceAll,
		common.BKDBEach:             replaceAll,
		common.BKDBOR:               recursion,
		common.BKDBAND:              recursion,
		common.BKDBPush:             recursion,
		common.BKDBNot:              recursion,
		common.BKDBPull:             recursion,
		common.BKDBProject:          recursion,
		common.BKDBReplaceRoot:      recursion,
		common.BKDBEQ:               replace,
		common.BKDBNE:               replace,
		common.BKDBLT:               replace,
		common.BKDBLTE:              replace,
		common.BKDBGT:               replace,
		common.BKDBGTE:              replace,
		common.BKDBLIKE:             noAction,
		common.BKDBOPTIONS:          noAction,
		common.BKDBExists:           noAction,
		common.BKDBCount:            noAction,
		common.BKDBGroup:            noAction,
		common.BKDBMatch:            noAction,
		common.BKDBSum:              noAction,
		common.BKDBMULTIPLELike:     noAction,
		common.BKDBUNSET:            noAction,
		common.BKDBSize:             noAction,
		common.BKDBType:             noAction,
		common.BKDBSort:             noAction,
		common.BKDBLimit:            noAction,
		common.BKHostOuterIPv6Field: prepareReplace,
		common.BKHostInnerIPv6Field: prepareReplace,
		common.BKHostOuterIPField:   prepareReplace,
		common.BKHostInnerIPField:   prepareReplace,
	}

	if _, ok := symbolTypeMap[symbol]; !ok {
		return noAction
	}
	return symbolTypeMap[symbol]
}

func convertInterfaceIntoMapStrByReflectionForMongoIPv6(target interface{}, flag bool) error {

	value := reflect.ValueOf(target)
	switch value.Kind() {
	case reflect.Map:
		return DealMapForMongoIPv6(value, flag)
	case reflect.Struct:
		return dealStructForMongoIPv6(value.Type(), value, flag)
	}

	return fmt.Errorf("no support the kind(%s)", value.Kind())
}

func dealStructForMongoIPv6(kind reflect.Type, value reflect.Value, flag bool) error {

	fieldNum := value.NumField()
	for idx := 0; idx < fieldNum; idx++ {
		fieldType := kind.Field(idx)
		fieldValue := value.Field(idx)

		switch fieldValue.Kind() {
		default:
			blog.Errorf("5555543433333333333 type: %v", fieldValue)
			if fieldValue.IsValid() && fieldValue.CanInterface() {
				blog.Errorf("88888888666666666 value: %v,name: %v ", fieldValue.Interface(), fieldType.Name)
			}
		case reflect.Interface:
			err := convertInterfaceIntoMapStrByReflectionForMongoIPv6(fieldValue.Interface(), flag)
			if nil != err {
				return err
			}
		case reflect.Struct:
			err := dealStructForMongoIPv6(fieldValue.Type(), fieldValue, flag)
			if nil != err {
				return err
			}

		case reflect.Map:
			err := DealMapForMongoIPv6(fieldValue, flag)
			if nil != err {
				return err
			}
		}
	}

	return nil
}

func convertInterfaceIntoMap(target interface{}, flag bool) error {
	blog.Errorf("99999999999yyyyyyy target:%v ", target)
	value := reflect.ValueOf(target)
	switch value.Kind() {
	// compatible with the scenario where the value is a string.
	case reflect.String:
		if flag {
			// todo: 进行替换
		}
		blog.Errorf("88888888888888 value: %v", value)
	//case reflect.Int8, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
	//	reflect.Float32, reflect.Float64:
	//	fmt.Printf("2222222222222222222222 value: %v\n",value)

	case reflect.Map:
		blog.Errorf("aaaaaaaaaaaaaaaa")
		DealMapForMongoIPv6(target, flag)
	case reflect.Struct:
		blog.Errorf("777777777777777 target: %v", target)
		dealStructForMongoIPv6(value.Type(), value, flag)
	case reflect.Slice, reflect.Array:

		// 这个场景得range元素
		for i := 0; i < value.Len(); i++ {
			//fmt.Printf("tttttttttttttttttttttttt \n")
			convertInterfaceIntoMap(value.Index(i).Interface(), flag)
			// fmt.Printf("tttttttttttttttttttttttt value: %v, type: %v\n",value.Index(i).Interface(),reflect.ValueOf(value.Index(i).Interface()).Kind())
		}
		//fmt.Printf("ppppppppppppppppppppppp target: %v\n",value.Kind() )
	default:
	}
	return nil
}

func DealMapForMongoIPv6(data interface{}, flag bool) error {

	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Map:
		blog.Errorf("hhhhhhhhhhhhh")
		mapKeys := v.MapKeys()
		for _, key := range mapKeys {
			keyValue := v.MapIndex(key)
			switch keyValue.Kind() {
			// compatible with the scenario where the value is a string.
			case reflect.Interface:

				value := keyValue.Interface()
				switch value.(type) {
				//case float64:
				//	fmt.Printf("11111111111111hhhhhhhhhhhhh\n")
				//case float32:
				//	fmt.Printf("22222222222hhhhhhhhhhhhh\n")
				//case int64:
				//	fmt.Printf("33333333333333hhhhhhhhhhhhh\n")
				//case int32:
				//	fmt.Printf("444444444444444hhhhhhhhhhhhh\n")
				//case int:
				//	fmt.Printf("5555555555555555hhhhhhhhhhhhh\n")
				case string:
					blog.Errorf("666666666666666666hhhhhhhhhhhhh")
				case []interface{}:
					blog.Errorf("77777777777777777hhhhhhhhhhhhh")
				case []string:
					blog.Errorf("999999999999999999999hhhhhhhhhhhhh")
				case []map[string]interface{}:
					blog.Errorf("iiiiiiiiiiiiiiiiiiiiiiiiiii")

				default:
					blog.Errorf("888888888888888888888hhhhhhhhhhhhh value: %v", reflect.TypeOf(value))
				}
				if key.Kind() != reflect.String {
					continue
				}
				action := getSymbolType(key.String())
				if action == noAction {
					blog.Errorf("ppppppppppppppppppppp, v: %v, key: %v, keyValue: %", v, key.String(), keyValue)
					continue
				}
				if !flag && action == prepareReplace {
					flag = true
				}
				blog.Errorf("00000000000, v: %v, key: %v, keyValue: %v, keyType: %v, keyValue: %v",
					v, key.String(), keyValue, key.Kind(), keyValue.Kind())

				convertInterfaceIntoMap(keyValue.Interface(), flag)
			case reflect.Map:
				blog.Errorf("1111111111, v: %v, key: %v, keyValue: %v ", v, key, keyValue)

			case reflect.String:
				blog.Errorf("22222222222, v: %v,key: %v, keyValue: %v ", v, key, keyValue)

				return nil
			case reflect.Int8, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Float32, reflect.Float64:
				blog.Errorf("3333333333, v: %v, key: %v,keyValue: %v ", v, key, keyValue)

				return nil
			default:
				blog.Errorf("data type is error, type: %v", v.Kind())
			}
		}
	case reflect.Struct:
		blog.Errorf("ddddddddddddddddddd v: %v", v)
		dealStructForMongoIPv6(v.Type(), v, flag)

	case reflect.Interface:
		blog.Errorf("555555555555555555555 ")

	default:
		blog.Errorf("data type is error, type: %v", v.Kind())
	}
	return nil
}

// ReassignmentValue 替换指定field的值
//func ReassignmentValue(data interface{}, field []string, f convertFunc) error {
//	if data == nil || len(field) == 0 {
//		return nil
//	}
//
//	v := reflect.ValueOf(data)
//
//	switch v.Kind() {
//	case reflect.Map:
//		mapKeys := v.MapKeys()
//		for _, key := range mapKeys {
//			keyValue := v.MapIndex(key)
//			switch keyValue.Kind() {
//			// compatible with the scenario where the value is a string.
//			case reflect.Interface:
//				if err := convertInterfaceIntoMap(keyValue.Interface(), param, deep); err != nil {
//					return err
//				}
//			case reflect.Map:
//				if err := ValidateKVObject(keyValue.Interface(), param, deep+1); err != nil {
//					return err
//				}
//			case reflect.String:
//			case reflect.Int8, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
//				reflect.Float32, reflect.Float64:
//			default:
//				return errors.New("data type error")
//			}
//		}
//	case reflect.Struct:
//	case reflect.Interface:
//		if err := convertInterfaceIntoMap(v.Interface(), param, deep); err != nil {
//			return err
//		}
//	default:
//		return fmt.Errorf("data type is error, type: %v", v.Kind())
//	}
//	return nil
//
//	for k, v := range data {
//		if v == nil {
//			return errors.New("value is nil")
//		}
//		if getSymbolType(k) == noAction {
//			continue
//		}
//		if k == common.BKDBAddToSet {
//			// 判断v 是map还是string
//			if reflect.TypeOf(v).Kind() == reflect.String {
//				continue
//			}
//			// 这里需要测试一下除了manp类型这里，好像最可能是struct 先这么写，如果是struct应该转化一次就可以
//			if reflect.TypeOf(v).Kind() == reflect.Map {
//				ReassignmentValue(v, field, f)
//			}
//		}
//
//		// case 1：k是特殊类型，例如 $and $or 且仅支持这两个特殊字符，如果有别的特殊字符需要报错
//		// 如果是这种场景，需要判断value是否是array ，如果不是array需要报错。如果是那么还需要递归调用
//		// case 2：v是普通string 看是否命中field中，如果命中了，看对应的value是否是string 如果是直接替换。如果是mapstring 那么判断
//		// 对应的key是否是"$in", "nin","eq"等
//
//	}
//	return nil
//}
