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

package local

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/metadata"
	"configcenter/src/common/util"
	"configcenter/src/storage/dal"
	"configcenter/src/storage/dal/redis"
	"configcenter/src/storage/dal/types"
	dtype "configcenter/src/storage/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

// FieldType define the db table's field data type
type FieldType string

const (
	// Numeric means this field is Numeric data type.
	Numeric FieldType = "numeric"
	// Boolean means this field is Boolean data type.
	Boolean FieldType = "bool"
	// String means this field is String data type.
	String FieldType = "string"
	// MapStringKV means this field is string type kv type.
	MapStringKV FieldType = "mapStringKV"
	// Array means this field is Array data type.
	Array FieldType = "array"

	// MapObject means this field is Object data type.
	MapObject FieldType = "mapObject"

	// subsequent support for other types can be added here.
	// After adding a type, pay attention to adding a verification
	// function for this type synchronously. Special attention is
	// paid to whether the array elements also need to synchronize support for this type.
)

// Fields 公共的字段框架
type Fields struct {
	// descriptors 每个表结构字段的tag和对应的类型 是否可编辑 或者必填字段
	descriptors []FieldDescriptor
	// fields defines all the table's fields
	fields    []string
	fieldType map[string]FieldType
}

// FieldsDescriptors is a collection of FieldDescriptor
type FieldsDescriptors []FieldDescriptor

// ClusterFields defines all the app table's fields.
var ClusterFields = mergeFields(ClusterFieldsDescriptor)

// ClusterFieldsDescriptor is Cluster's column descriptors.
var ClusterFieldsDescriptor = mergeFieldDescriptors(
	FieldsDescriptors{
		{Field: "id", NamedC: "id", Type: Numeric, Required: true, IsEditable: false},
		{Field: "biz_id", NamedC: "biz_id", Type: Numeric, Required: true, IsEditable: false},
		{Field: "bk_supplier_account", NamedC: "bk_supplier_account", Type: String, Required: true, IsEditable: false},
		{Field: "creator", NamedC: "creator", Type: String, Required: true, IsEditable: false},
		{Field: "modifier", NamedC: "modifier", Type: String, Required: true, IsEditable: true},
		{Field: "create_time", NamedC: "create_time", Type: Numeric, Required: true, IsEditable: false},
		{Field: "last_time", NamedC: "last_time", Type: Numeric, Required: true, IsEditable: true},
	},
	mergeFieldDescriptors(ClusterSpecFieldsDescriptor),
)

// FieldTypes returns each field and it's data type
func (col Fields) FieldTypes() map[string]FieldType {
	copied := make(map[string]FieldType)
	for k, v := range col.fieldType {
		copied[k] = v
	}

	return copied
}

// ClusterSpecFieldsDescriptor is Cluster Spec's column descriptors.
var ClusterSpecFieldsDescriptor = FieldsDescriptors{
	{Field: "name", NamedC: "name", Type: String, Required: true, IsEditable: false},
	{Field: "scheduling_engine", NamedC: "scheduling_engine", Type: String, Required: false, IsEditable: false},
	{Field: "uid", NamedC: "uid", Type: String, Required: true, IsEditable: true},
	{Field: "xid", NamedC: "xid", Type: String, Required: false, IsEditable: false},
	{Field: "version", NamedC: "version", Type: String, Required: false, IsEditable: true},
	{Field: "network_type", NamedC: "network_type", Type: String, Required: false, IsEditable: false},
	{Field: "region", NamedC: "region", Type: String, Required: false, IsEditable: true},
	{Field: "vpc", NamedC: "vpc", Type: String, Required: false, IsEditable: false},
	{Field: "network", NamedC: "network", Type: String, Required: false, IsEditable: false},
	{Field: "type", NamedC: "type", Type: String, Required: false, IsEditable: true},
}

// NamespaceSpecFieldsDescriptor is Namespace Spec's column descriptors.
var NamespaceSpecFieldsDescriptor = FieldsDescriptors{
	{Field: "name", NamedC: "name", Type: String, Required: true, IsEditable: false},
	{Field: "labels", NamedC: "labels", Type: MapStringKV, Required: false, IsEditable: true},
	{Field: "cluster_uid", NamedC: "cluster_uid", Type: String, Required: true, IsEditable: false},
	{Field: "resource_quotas", NamedC: "resource_quotas", Type: Array, Required: false, IsEditable: true},
}

// NodeSpecFieldsDescriptor is Node Spec's column descriptors.
var NodeSpecFieldsDescriptor = FieldsDescriptors{
	{Field: "name", NamedC: "name", Type: String, Required: true, IsEditable: false},
	{Field: "roles", NamedC: "Roles", Type: String, Required: false, IsEditable: true},
	{Field: "labels", NamedC: "labels", Type: MapStringKV, Required: false, IsEditable: true},
	{Field: "taints", NamedC: "taints", Type: MapStringKV, Required: false, IsEditable: true},
	{Field: "unschedulable", NamedC: "unschedulable", Type: Boolean, Required: false, IsEditable: true},
	{Field: "internal_ip", NamedC: "internal_ip", Type: Array, Required: false, IsEditable: true},
	{Field: "external_ip", NamedC: "external_ip", Type: Array, Required: false, IsEditable: true},
	{Field: "hostname", NamedC: "hostname", Type: String, Required: false, IsEditable: true},
	{Field: "runtime_component", NamedC: "runtime_component", Type: String, Required: false, IsEditable: true},
	{Field: "kube_proxy_mode", NamedC: "kube_proxy_mode", Type: String, Required: false, IsEditable: true},
	{Field: "pod_cidr", NamedC: "pod_cidr", Type: String, Required: false, IsEditable: true},
}

// WorkLoadSpecFieldsDescriptor is WorkLoad Spec's column descriptors.
var WorkLoadSpecFieldsDescriptor = FieldsDescriptors{
	{Field: "name", NamedC: "name", Type: String, Required: true, IsEditable: false},
	{Field: "namespace", NamedC: "namespace", Type: String, Required: true, IsEditable: false},
	{Field: "labels", NamedC: "labels", Type: MapStringKV, Required: false, IsEditable: true},
	{Field: "selector", NamedC: "selector", Type: String, Required: false, IsEditable: true},
	{Field: "replicas", NamedC: "replicas", Type: Numeric, Required: true, IsEditable: true},
	{Field: "strategy_type", NamedC: "strategy_type", Type: String, Required: false, IsEditable: true},
	{Field: "min_ready_seconds", NamedC: "min_ready_seconds", Type: Numeric, Required: false, IsEditable: true},
	{Field: "rolling_update_strategy", NamedC: "rolling_update_strategy", Type: MapObject, Required: false, IsEditable: true},
}

// PodSpecFieldsDescriptor is Pod Spec's column descriptors.
var PodSpecFieldsDescriptor = FieldsDescriptors{
	{Field: "name", NamedC: "name", Type: String, Required: true, IsEditable: false},
	{Field: "namespace", NamedC: "namespace", Type: String, Required: true, IsEditable: false},
	{Field: "priority", NamedC: "priority", Type: Numeric, Required: false, IsEditable: true},
	{Field: "labels", NamedC: "labels", Type: MapStringKV, Required: false, IsEditable: true},
	{Field: "ip", NamedC: "ip", Type: String, Required: false, IsEditable: true},
	{Field: "ips", NamedC: "ips", Type: String, Required: false, IsEditable: true},
	{Field: "controlled_by", NamedC: "controlled_by", Type: Numeric, Required: false, IsEditable: true},
	{Field: "container_uid", NamedC: "container_uid", Type: Array, Required: false, IsEditable: true},
	{Field: "volumes", NamedC: "volumes", Type: MapObject, Required: false, IsEditable: true},
	{Field: "qos_class", NamedC: "qos_class", Type: String, Required: false, IsEditable: true},
	{Field: "node_selectors", NamedC: "node_selectors", Type: MapStringKV, Required: false, IsEditable: true},
	{Field: "tolerations", NamedC: "tolerations", Type: MapObject, Required: false, IsEditable: true},
}

// ContainerSpecFieldsDescriptor is Container Spec's column descriptors.
var ContainerSpecFieldsDescriptor = FieldsDescriptors{
	{Field: "name", NamedC: "name", Type: String, Required: true, IsEditable: false},
	{Field: "container_uid", NamedC: "container_uid", Type: String, Required: true, IsEditable: false},
	{Field: "image", NamedC: "image", Type: String, Required: true, IsEditable: false},
	{Field: "ports", NamedC: "ports", Type: String, Required: false, IsEditable: true},
	{Field: "host_ports", NamedC: "host_ports", Type: String, Required: false, IsEditable: true},
	{Field: "args", NamedC: "args", Type: String, Required: false, IsEditable: true},
	{Field: "started", NamedC: "started", Type: Numeric, Required: false, IsEditable: true},
	{Field: "requests", NamedC: "requests", Type: MapObject, Required: false, IsEditable: true},
	{Field: "limits", NamedC: "limits", Type: MapObject, Required: false, IsEditable: true},
	{Field: "liveness", NamedC: "liveness", Type: MapObject, Required: false, IsEditable: true},
	{Field: "environment", NamedC: "environment", Type: MapObject, Required: false, IsEditable: true},
	{Field: "mounts", NamedC: "mounts", Type: MapObject, Required: false, IsEditable: true},
}

// mergeFieldDescriptors merge column descriptors to one map.
func mergeFieldDescriptors(namedC ...FieldsDescriptors) FieldsDescriptors {
	if len(namedC) == 0 {
		return make([]FieldDescriptor, 0)
	}

	merged := make([]FieldDescriptor, 0)
	for _, one := range namedC {
		merged = append(merged, one...)
	}

	return merged
}

func mergeFields(all ...FieldsDescriptors) *Fields {
	tc := &Fields{
		descriptors: make([]FieldDescriptor, 0),
		fields:      make([]string, 0),
		fieldType:   make(map[string]FieldType),
	}
	if len(all) == 0 {
		return tc
	}

	for _, nc := range all {
		for _, col := range nc {
			tc.descriptors = append(tc.descriptors, col)
			tc.fieldType[col.Field] = col.Type
			tc.fields = append(tc.fields, col.Field)
		}
	}

	return tc
}

// ValidateNumeric
// 1、judgment is a number type.
// 2、the judgment is that they are all within the specified range,
func ValidateNumeric(data interface{}, param NumericSettings) error {

	if data == nil {
		return errors.New("data is nil")
	}

	v, err := util.GetIntByInterface(data)
	if err != nil {
		return err
	}

	if v > param.Max || v < param.Min {
		return fmt.Errorf("data : %d out of range [min: %d - max: %d]", v, param.Min, param.Max)
	}
	return nil
}

// ValidateString judgment of string data:
// 1、judgment type.
// 2、judgment length.
// 3、check if the regular expression is satisfied if necessary.
// 4、if length is set to 0, it means that the length of the string is not checked.
func ValidateString(data interface{}, param StringSettings) error {

	if data == nil {
		return errors.New("data is nil")
	}

	tmpType := reflect.TypeOf(data)

	if tmpType.Kind() != reflect.String {
		return errors.New("data type is not string")
	}

	v := data.(string)
	if len(v) > param.MaxLength {
		return fmt.Errorf("data length is exceeded max length %d", param.MaxLength)
	}

	if len(param.RegularCheck) < 0 {
		return nil
	}

	if !regexp.MustCompile(param.RegularCheck).MatchString(v) {
		return fmt.Errorf("invalid data %s, regular is %s", v, param.RegularCheck)
	}

	return nil
}

// ValidateBoolen Boolean type judgment
func ValidateBoolen(data interface{}) error {
	if data == nil {
		return errors.New("data is nil")
	}

	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Bool {
		return fmt.Errorf("data type is error, type: %v", v.Kind())
	}

	return nil
}

// ValidateMapString mapstring type judgment：
// 1、type must be map.
// 2、the type of key and value must be string.
// 3、check the number of key-value pairs.
func ValidateMapString(data interface{}, length int) error {
	if data == nil {
		return errors.New("data is nil")
	}

	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Map {
		return fmt.Errorf("data type is error, type: %v", v.Kind())
	}

	mapKeys := v.MapKeys()
	if len(mapKeys) > length {
		return fmt.Errorf("data length is exceeded max length %d", length)
	}

	for _, key := range mapKeys {
		if key.Kind() != reflect.String {
			return fmt.Errorf("data key type is not string")
		}
		if v.MapIndex(key).Kind() != reflect.String {
			return fmt.Errorf("data value type is not string")
		}
	}

	return nil
}

// MapObjectSettings mapObject type check parameter settings
type MapObjectSettings struct {
	// MaxDeep if the array element is the maximum level allowed by the map object
	MaxDeep int

	// MaxLength the maximum number of elements per level allowed by the array element if the map object.
	MaxLength int
}

// NumericSettings numeric type check parameter setting.
type NumericSettings struct {
	// Min Minimum value allowed for numeric types.
	Min int

	// Max maximum value allowed for numeric types.
	Max int
}

// StringSettings string type check parameter setting.
type StringSettings struct {
	// MaxLength maximum length allowed for string type.
	MaxLength int

	// RegularCheck regular expressions involved in strings
	RegularCheck string
}

// AdvancedSettingsParam parameter settings of the array type,
// including fine-grained check parameter settings for each element of the array
type AdvancedSettingsParam struct {
	// ArrayMaxLength maximum length of an array allowed.
	ArrayMaxLength int
	MapObjectParam MapObjectSettings `json:"map_object_param"`
	NumericParam   NumericSettings   `json:"numeric_param"`
	StringParam    StringSettings    `json:"string_param"`
}

// ValidateArray
// 1、the length of the check array. If maxLength is set to 0, it means that the length will not be checked.
// 2、determines the type of array elements. Currently only bool, numeric, string, and map are supported.
// The rest of the types are not supported, nor are multidimensional arrays supported.
// 3、when the elements of the array are of type map, the maximum nesting level maxDeep of the map needs to be set.
func ValidateArray(data interface{}, param *AdvancedSettingsParam) error {
	if data == nil {
		return errors.New("data is nil")
	}

	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		if v.Len() > param.ArrayMaxLength {
			return fmt.Errorf("data len exceed max length: %d", param.ArrayMaxLength)
		}
		for i := 0; i < v.Len(); i++ {
			switch v.Index(i).Kind() {
			case reflect.Int, reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint16,
				reflect.Uint32, reflect.Uint64, reflect.Uint8:
				if err := ValidateNumeric(v.Index(i).Interface(), param.NumericParam); err != nil {
					return err
				}
			case reflect.String:
				if err := ValidateString(v.Index(i).Interface(), param.StringParam); err != nil {
					return err
				}

			case reflect.Map:
				if err := ValidateKVObject(v.Index(i).Interface(), param.MapObjectParam, 1); err != nil {
					return err
				}

			case reflect.Bool:
				if err := ValidateBoolen(v.Index(i).Interface()); err != nil {
					return err
				}

			default:
				return fmt.Errorf("unsupported type: %v", v.Index(i).Kind())
			}
		}
	default:
		return errors.New("data type is not array")
	}
	return nil
}

// ValidateKVObject 对于通用的kv类型数据的校验。需要兼容嵌套，其中level指的是嵌套层级
// maxLength  每一层级允许的最大keys数量
// deep 目前解析深度的层级，初始值设定成1
// maxDeep map对象允许的最大层级
func ValidateKVObject(data interface{}, param MapObjectSettings, deep int) error {

	if data == nil {
		return errors.New("data is nil")
	}
	if param.MaxDeep == 0 {
		return errors.New("max deep must be set")
	}

	if deep > param.MaxDeep {
		return fmt.Errorf("exceed max deep: %d", param.MaxDeep)
	}

	v := reflect.ValueOf(data)

	switch v.Kind() {
	case reflect.Map:
		mapKeys := v.MapKeys()
		if len(mapKeys) > param.MaxLength {
			return fmt.Errorf("keys length exceed than %d", param.MaxLength)
		}

		for _, key := range mapKeys {
			keyValue := v.MapIndex(key)
			switch keyValue.Kind() {
			// compatible with the scenario where the value is a string.
			case reflect.Interface:
				if err := convertInterfaceIntoMap(keyValue.Interface(), param, deep); err != nil {
					return err
				}
			case reflect.Map:
				if err := ValidateKVObject(keyValue.Interface(), param, deep+1); err != nil {
					return err
				}
			case reflect.String:
			case reflect.Int8, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
			default:
				return errors.New("data type error")
			}
		}
	case reflect.Struct:
	case reflect.Interface:
		if err := convertInterfaceIntoMap(v.Interface(), param, deep); err != nil {
			return err
		}
	default:
		return fmt.Errorf("data type is error, type: %v", v.Kind())
	}
	return nil
}

func convertInterfaceIntoMap(target interface{}, param MapObjectSettings, deep int) error {

	value := reflect.ValueOf(target)
	switch value.Kind() {
	// compatible with the scenario where the value is a string.
	case reflect.String:
	case reflect.Map:
		if err := ValidateKVObject(value, param, deep); err != nil {
			return err
		}
	case reflect.Struct:
	default:
		return fmt.Errorf("no support the kind(%s)", value.Kind())
	}
	return nil
}

// FieldDescriptor defines a table's field related information.
type FieldDescriptor struct {
	// Field is field's name
	Field string
	// NamedC is named field's name
	NamedC string
	// Type is this field's data type.
	Type FieldType
	// Required is it required.
	Required bool
	// IsEditable is it editable.
	IsEditable bool
	_          struct{}
}

// ClusterOption 创建集群请求字段
type ClusterOption struct {
	Name *string `json:"name" bson:"name"`
	// SchedulingEngine scheduling engines, such as k8s, tke, etc.
	SchedulingEngine *string `json:"scheduling_engine" bson:"scheduling_engine"`
	// Uid ID of the cluster itself
	Uid *string `json:"uid" bson:"uid"`
	// Xid The underlying cluster ID it depends on
	Xid *string `json:"xid" bson:"xid"`
	// Version cluster version
	Version *string `json:"version" bson:"version"`
	// NetworkType network type, such as overlay or underlay
	NetworkType *string `json:"network_type" bson:"network_type"`
	// Region the region where the cluster is located
	Region *string `json:"region" bson:"region"`
	// Vpc vpc network
	Vpc *string `json:"vpc" bson:"vpc"`
	// NetWork global routing network address (container overlay network) For example: ["1.1.1.0/21"]
	NetWork *[]string `json:"network" bson:"network"`
	// Type cluster network type, e.g. public clusters, private clusters, etc.
	Type *string `json:"type" bson:"type"`
}

// ValidateCreate 校验
func (option *ClusterOption) ValidateCreate() error {

	if option.Name == nil || *option.Name == "" {
		return errors.New("name can not be empty")
	}

	if option.Uid == nil || *option.Uid == "" {
		return errors.New("uid can not be empty")
	}
	return nil
}

// ClusterBaseFields cluster fields
type ClusterBaseFields struct {
	// Name cluster name
	Name string `json:"name" bson:"name"`
	// SchedulingEngine scheduling engines, such as k8s, tke, etc.
	SchedulingEngine string `json:"scheduling_engine" bson:"scheduling_engine"`
	// Uid ID of the cluster itself
	Uid string `json:"uid" bson:"uid"`
	// Xid The underlying cluster ID it depends on
	Xid string `json:"xid" bson:"xid"`
	// Version cluster version
	Version string `json:"version" bson:"version"`
	// NetworkType network type, such as overlay or underlay
	NetworkType string `json:"network_type" bson:"network_type"`
	// Region the region where the cluster is located
	Region string `json:"region" bson:"region"`
	// Vpc vpc network
	Vpc string `json:"vpc" bson:"vpc"`
	// NetWork global routing network address (container overlay network) For example: ["1.1.1.0/21"]
	NetWork []string `json:"network" bson:"network"`
	// Type cluster network type, e.g. public clusters, private clusters, etc.
	Type string `json:"type" bson:"type"`
}

// Cluster container cluster table structure
type Cluster struct {
	// ID cluster auto-increment ID in cc
	ID int64 `json:"id" bson:"id"`
	// BizID the business ID to which the cluster belongs
	BizID int64 `json:"bk_biz_id" bson:"bk_biz_id"`
	// ClusterFields cluster base fields
	ClusterFields ClusterBaseFields `json:",inline"`
	// SupplierAccount the supplier account that this resource belongs to.
	SupplierAccount string `json:"bk_supplier_account" bson:"bk_supplier_account"`
	// Revision record this app's revision information
	Revision Revision `json:",inline"`
}

func (option *Cluster) Validate() {

}

// Revision is a resource's status information
type Revision struct {
	Creator    string `json:"creator" bson:"creator"`
	Modifier   string `json:"modifier" bson:"modifier"`
	CreateTime int64  `json:"create_time" bson:"create_time"`
	LastTime   int64  `json:"last_time" bson:"last_time"`
}

// IsCreateEmpty Insert data case validator and creator
func (r Revision) IsCreateEmpty() bool {
	if len(r.Creator) != 0 {
		return false
	}

	if r.CreateTime == 0 {
		return false
	}

	return true
}

const lagSeconds = 5 * 60

// ValidateCreate Insert data case validator and creator
func (r Revision) ValidateCreate() error {

	if len(r.Creator) == 0 {
		return errors.New("creator can not be empty")
	}

	now := time.Now().Unix()
	if (r.CreateTime <= (now - lagSeconds)) || (r.CreateTime >= (now + lagSeconds)) {
		return errors.New("invalid create time")
	}

	return nil
}

// IsModifyEmpty the update data scene verifies the revisioner and modification time of the updated data.
func (r Revision) IsModifyEmpty() bool {
	if len(r.Modifier) != 0 {
		return false
	}

	if r.LastTime == 0 {
		return false
	}

	return true
}

// ValidateUpdate validate revision when updated
func (r Revision) ValidateUpdate() error {
	if len(r.Modifier) == 0 {
		return errors.New("reviser can not be empty")
	}

	if len(r.Creator) != 0 {
		return errors.New("creator can not be updated")
	}

	now := time.Now().Unix()
	if (r.LastTime <= (now - lagSeconds)) || (r.LastTime >= (now + lagSeconds)) {
		return errors.New("invalid update time")
	}

	if r.LastTime < r.CreateTime-lagSeconds {
		return errors.New("update time must be later than create time")
	}
	return nil
}

// 由于是结构化数据可以写一个创建、更新数据的框架。如果没有创建和更新的框架，需要每个表都做字段的校验。

type Mongo struct {
	dbc    *mongo.Client
	dbname string
	sess   mongo.Session
	tm     *TxnManager
}

var _ dal.DB = new(Mongo)

type MongoConf struct {
	TimeoutSeconds int
	MaxOpenConns   uint64
	MaxIdleConns   uint64
	URI            string
	RsName         string
	SocketTimeout  int
}

// NewMgo returns new RDB
func NewMgo(config MongoConf, timeout time.Duration) (*Mongo, error) {
	connStr, err := connstring.Parse(config.URI)
	if nil != err {
		return nil, err
	}
	if config.RsName == "" {
		return nil, fmt.Errorf("mongodb rsName not set")
	}
	socketTimeout := time.Second * time.Duration(config.SocketTimeout)
	maxConnIdleTime := 25 * time.Minute
	appName := common.GetIdentification()
	// do not change this, our transaction plan need it to false.
	// it's related with the transaction number(eg txnNumber) in a transaction session.
	disableWriteRetry := false
	conOpt := options.ClientOptions{
		MaxPoolSize:     &config.MaxOpenConns,
		MinPoolSize:     &config.MaxIdleConns,
		ConnectTimeout:  &timeout,
		SocketTimeout:   &socketTimeout,
		ReplicaSet:      &config.RsName,
		RetryWrites:     &disableWriteRetry,
		MaxConnIdleTime: &maxConnIdleTime,
		AppName:         &appName,
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(config.URI), &conOpt)
	if nil != err {
		return nil, err
	}

	if err := client.Connect(context.TODO()); nil != err {
		return nil, err
	}

	// TODO: add this check later, this command needs authorize to get version.
	// if err := checkMongodbVersion(connStr.Database, client); err != nil {
	// 	return nil, err
	// }

	// initialize mongodb related metrics
	initMongoMetric()

	return &Mongo{
		dbc:    client,
		dbname: connStr.Database,
		tm:     &TxnManager{},
	}, nil
}

// from now on, mongodb version must >= 4.2.0
func checkMongodbVersion(db string, client *mongo.Client) error {
	serverStatus, err := client.Database(db).RunCommand(
		context.Background(),
		bsonx.Doc{{"serverStatus", bsonx.Int32(1)}},
	).DecodeBytes()
	if err != nil {
		return err
	}

	version, err := serverStatus.LookupErr("version")
	if err != nil {
		return err
	}

	fields := strings.Split(version.StringValue(), ".")
	if len(fields) != 3 {
		return fmt.Errorf("got invalid mongodb version: %v", version.StringValue())
	}
	// version must be >= v4.2.0
	major, err := strconv.Atoi(fields[0])
	if err != nil {
		return fmt.Errorf("parse mongodb version %s major failed, err: %v", version.StringValue(), err)
	}
	if major < 4 {
		return errors.New("mongodb version must be >= v4.2.0")
	}

	minor, err := strconv.Atoi(fields[1])
	if err != nil {
		return fmt.Errorf("parse mongodb version %s minor failed, err: %v", version.StringValue(), err)
	}
	if minor < 2 {
		return errors.New("mongodb version must be >= v4.2.0")
	}
	return nil
}

// InitTxnManager TxnID management of initial transaction
func (c *Mongo) InitTxnManager(r redis.Client) error {
	return c.tm.InitTxnManager(r)
}

// Close replica client
func (c *Mongo) Close() error {
	c.dbc.Disconnect(context.TODO())
	return nil
}

// Ping replica client
func (c *Mongo) Ping() error {
	return c.dbc.Ping(context.TODO(), nil)
}

// IsDuplicatedError check duplicated error
func (c *Mongo) IsDuplicatedError(err error) bool {
	if err != nil {
		if strings.Contains(err.Error(), "The existing index") {
			return true
		}
		if strings.Contains(err.Error(), "There's already an index with name") {
			return true
		}
		if strings.Contains(err.Error(), "E11000 duplicate") {
			return true
		}
		if strings.Contains(err.Error(), "IndexOptionsConflict") {
			return true
		}
		if strings.Contains(err.Error(), "all indexes already exist") {
			return true
		}
		if strings.Contains(err.Error(), "already exists with a different name") {
			return true
		}
	}
	return err == types.ErrDuplicated
}

// IsNotFoundError check the not found error
func (c *Mongo) IsNotFoundError(err error) bool {
	return err == types.ErrDocumentNotFound
}

// Table collection operation
func (c *Mongo) Table(collName string) types.Table {
	col := Collection{}
	col.collName = collName
	col.Mongo = c
	return &col
}

// get db client
func (c *Mongo) GetDBClient() *mongo.Client {
	return c.dbc
}

// get db name
func (c *Mongo) GetDBName() string {
	return c.dbname
}

// Collection implement client.Collection interface
type Collection struct {
	collName string // 集合名
	*Mongo
}

// Find 查询多个并反序列化到 Result
func (c *Collection) Find(filter types.Filter, opts ...*types.FindOpts) types.Find {
	find := &Find{
		Collection: c,
		filter:     filter,
		projection: make(map[string]int),
	}

	find.Option(opts...)

	return find
}

// Find define a find operation
type Find struct {
	*Collection

	projection map[string]int
	filter     types.Filter
	start      int64
	limit      int64
	sort       bson.D

	option types.FindOpts
}

// Fields 查询字段
func (f *Find) Fields(fields ...string) types.Find {
	for _, field := range fields {
		if len(field) <= 0 {
			continue
		}
		f.projection[field] = 1
	}
	return f
}

// Sort 查询排序
// sort支持多字段最左原则排序
// sort值为"host_id, -host_name"和sort值为"host_id:1, host_name:-1"是一样的，都代表先按host_id递增排序，再按host_name递减排序
func (f *Find) Sort(sort string) types.Find {
	if sort != "" {
		sortArr := strings.Split(sort, ",")
		f.sort = bson.D{}
		for _, sortItem := range sortArr {
			sortItemArr := strings.Split(strings.TrimSpace(sortItem), ":")
			sortKey := strings.TrimLeft(sortItemArr[0], "+-")
			if len(sortItemArr) == 2 {
				sortDescFlag := strings.TrimSpace(sortItemArr[1])
				if sortDescFlag == "-1" {
					f.sort = append(f.sort, bson.E{sortKey, -1})
				} else {
					f.sort = append(f.sort, bson.E{sortKey, 1})
				}
			} else {
				if strings.HasPrefix(sortItemArr[0], "-") {
					f.sort = append(f.sort, bson.E{sortKey, -1})
				} else {
					f.sort = append(f.sort, bson.E{sortKey, 1})
				}
			}
		}
	}

	return f
}

// Start 查询上标
func (f *Find) Start(start uint64) types.Find {
	// change to int64,后续改成int64
	dbStart := int64(start)
	f.start = dbStart
	return f
}

// Limit 查询限制
func (f *Find) Limit(limit uint64) types.Find {
	// change to int64,后续改成int64
	dbLimit := int64(limit)
	f.limit = dbLimit
	return f
}

var hostSpecialFieldMap = map[string]bool{
	common.BKHostInnerIPField:   true,
	common.BKHostOuterIPField:   true,
	common.BKOperatorField:      true,
	common.BKBakOperatorField:   true,
	common.BKHostInnerIPv6Field: true,
	common.BKHostOuterIPv6Field: true,
}

// All 查询多个
func (f *Find) All(ctx context.Context, result interface{}) error {
	mtc.collectOperCount(f.collName, findOper)

	rid := ctx.Value(common.ContextRequestIDField)
	start := time.Now()
	defer func() {
		mtc.collectOperDuration(f.collName, findOper, time.Since(start))
	}()

	err := validHostType(f.collName, f.projection, result, rid)
	if err != nil {
		return err
	}

	findOpts := f.generateMongoOption()
	// 查询条件为空时候，mongodb 不返回数据
	if f.filter == nil {
		f.filter = bson.M{}
	}

	opt := getCollectionOption(ctx)

	return f.tm.AutoRunWithTxn(ctx, f.dbc, func(ctx context.Context) error {
		cursor, err := f.dbc.Database(f.dbname).Collection(f.collName, opt).Find(ctx, f.filter, findOpts)
		if err != nil {
			mtc.collectErrorCount(f.collName, findOper)
			return err
		}
		return cursor.All(ctx, result)
	})
}

// List 查询多个数据， 当分页中start值为零的时候返回满足条件总行数
func (f *Find) List(ctx context.Context, result interface{}) (int64, error) {
	mtc.collectOperCount(f.collName, findOper)

	rid := ctx.Value(common.ContextRequestIDField)
	start := time.Now()
	defer func() {
		mtc.collectOperDuration(f.collName, findOper, time.Since(start))
	}()

	err := validHostType(f.collName, f.projection, result, rid)
	if err != nil {
		return 0, err
	}

	findOpts := f.generateMongoOption()
	// 查询条件为空时候，mongodb 不返回数据
	if f.filter == nil {
		f.filter = bson.M{}
	}

	opt := getCollectionOption(ctx)

	var total int64
	err = f.tm.AutoRunWithTxn(ctx, f.dbc, func(ctx context.Context) error {
		if f.start == 0 || (f.option.WithCount != nil && *f.option.WithCount) {
			var cntErr error
			total, cntErr = f.dbc.Database(f.dbname).Collection(f.collName, opt).CountDocuments(ctx, f.filter)
			if cntErr != nil {
				return cntErr
			}
		}
		cursor, err := f.dbc.Database(f.dbname).Collection(f.collName, opt).Find(ctx, f.filter, findOpts)
		if err != nil {
			mtc.collectErrorCount(f.collName, findOper)
			return err
		}
		return cursor.All(ctx, result)
	})

	return total, nil
}

// One 查询一个
func (f *Find) One(ctx context.Context, result interface{}) error {
	mtc.collectOperCount(f.collName, findOper)

	start := time.Now()
	rid := ctx.Value(common.ContextRequestIDField)
	defer func() {
		mtc.collectOperDuration(f.collName, findOper, time.Since(start))
	}()

	err := validHostType(f.collName, f.projection, result, rid)
	if err != nil {
		return err
	}

	findOpts := f.generateMongoOption()

	// 查询条件为空时候，mongodb panic
	if f.filter == nil {
		f.filter = bson.M{}
	}

	opt := getCollectionOption(ctx)
	return f.tm.AutoRunWithTxn(ctx, f.dbc, func(ctx context.Context) error {
		cursor, err := f.dbc.Database(f.dbname).Collection(f.collName, opt).Find(ctx, f.filter, findOpts)
		if err != nil {
			mtc.collectErrorCount(f.collName, findOper)
			return err
		}

		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			return cursor.Decode(result)
		}
		return types.ErrDocumentNotFound
	})

}

// Count 统计数量(非事务)
func (f *Find) Count(ctx context.Context) (uint64, error) {
	mtc.collectOperCount(f.collName, countOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(f.collName, countOper, time.Since(start))
	}()

	if f.filter == nil {
		f.filter = bson.M{}
	}

	opt := getCollectionOption(ctx)

	sessCtx, _, useTxn, err := f.tm.GetTxnContext(ctx, f.dbc)
	if err != nil {
		return 0, err
	}
	if !useTxn {
		// not use transaction.
		cnt, err := f.dbc.Database(f.dbname).Collection(f.collName, opt).CountDocuments(ctx, f.filter)
		if err != nil {
			mtc.collectErrorCount(f.collName, countOper)
			return 0, err
		}

		return uint64(cnt), err
	} else {
		// use transaction
		cnt, err := f.dbc.Database(f.dbname).Collection(f.collName, opt).CountDocuments(sessCtx, f.filter)
		// do not release th session, otherwise, the session will be returned to the
		// session pool and will be reused. then mongodb driver will increase the transaction number
		// automatically and do read/write retry if policy is set.
		// mongo.CmdbReleaseSession(ctx, session)
		if err != nil {
			mtc.collectErrorCount(f.collName, countOper)
			return 0, err
		}
		return uint64(cnt), nil
	}
}

// Insert 插入数据, docs 可以为 单个数据 或者 多个数据
func (c *Collection) Insert(ctx context.Context, docs interface{}) error {
	mtc.collectOperCount(c.collName, insertOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, insertOper, time.Since(start))
	}()

	rows := util.ConverToInterfaceSlice(docs)

	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).InsertMany(ctx, rows)
		if err != nil {
			mtc.collectErrorCount(c.collName, insertOper)
			return err
		}

		return nil
	})
}

// Update 更新数据
func (c *Collection) Update(ctx context.Context, filter types.Filter, doc interface{}) error {
	mtc.collectOperCount(c.collName, updateOper)
	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, updateOper, time.Since(start))
	}()

	if filter == nil {
		filter = bson.M{}
	}

	data := bson.M{"$set": doc}
	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, filter, data)
		if err != nil {
			mtc.collectErrorCount(c.collName, updateOper)
			return err
		}
		return nil
	})
}

// Update 更新数据, 返回修改成功的条数
func (c *Collection) UpdateMany(ctx context.Context, filter types.Filter, doc interface{}) (uint64, error) {
	mtc.collectOperCount(c.collName, updateOper)
	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, updateOper, time.Since(start))
	}()

	if filter == nil {
		filter = bson.M{}
	}

	data := bson.M{"$set": doc}
	var modifiedCount uint64
	err := c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		updateRet, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, filter, data)
		if err != nil {
			mtc.collectErrorCount(c.collName, updateOper)
			return err
		}
		modifiedCount = uint64(updateRet.ModifiedCount)
		return nil
	})
	return modifiedCount, err
}

// Upsert 数据存在更新数据，否则新加数据。
// 注意：该接口非原子操作，可能存在插入多条相同数据的风险。
func (c *Collection) Upsert(ctx context.Context, filter types.Filter, doc interface{}) error {
	mtc.collectOperCount(c.collName, upsertOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, upsertOper, time.Since(start))
	}()

	// set upsert option
	doUpsert := true
	replaceOpt := &options.UpdateOptions{
		Upsert: &doUpsert,
	}
	data := bson.M{"$set": doc}
	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateOne(ctx, filter, data, replaceOpt)
		if err != nil {
			mtc.collectErrorCount(c.collName, upsertOper)
			return err
		}
		return nil
	})

}

// UpdateMultiModel 根据不同的操作符去更新数据
func (c *Collection) UpdateMultiModel(ctx context.Context, filter types.Filter, updateModel ...types.ModeUpdate) error {
	mtc.collectOperCount(c.collName, updateOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, updateOper, time.Since(start))
	}()

	data := bson.M{}
	for _, item := range updateModel {
		if _, ok := data[item.Op]; ok {
			return errors.New(item.Op + " appear multiple times")
		}
		data["$"+item.Op] = item.Doc
	}

	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, filter, data)
		if err != nil {
			mtc.collectErrorCount(c.collName, updateOper)
			return err
		}
		return nil
	})

}

// Delete 删除数据
func (c *Collection) Delete(ctx context.Context, filter types.Filter) error {
	_, err := c.DeleteMany(ctx, filter)
	return err
}

// Delete 删除数据， 返回删除的行数
func (c *Collection) DeleteMany(ctx context.Context, filter types.Filter) (uint64, error) {
	mtc.collectOperCount(c.collName, deleteOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, deleteOper, time.Since(start))
	}()

	var deleteCount uint64
	err := c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		if err := c.tryArchiveDeletedDoc(ctx, filter); err != nil {
			mtc.collectErrorCount(c.collName, deleteOper)
			return err
		}
		deleteRet, err := c.dbc.Database(c.dbname).Collection(c.collName).DeleteMany(ctx, filter)
		if err != nil {
			mtc.collectErrorCount(c.collName, deleteOper)
			return err
		}

		deleteCount = uint64(deleteRet.DeletedCount)
		return nil
	})

	return deleteCount, err
}

func (c *Collection) tryArchiveDeletedDoc(ctx context.Context, filter types.Filter) error {
	switch c.collName {
	case common.BKTableNameModuleHostConfig:
	case common.BKTableNameBaseHost:
	case common.BKTableNameBaseApp:
	case common.BKTableNameBaseSet:
	case common.BKTableNameBaseModule:
	case common.BKTableNameSetTemplate:
	case common.BKTableNameBaseProcess:
	case common.BKTableNameProcessInstanceRelation:
	case common.BKTableNameBaseBizSet:

	case common.BKTableNameBaseInst:
	case common.BKTableNameInstAsst:
		// NOTE: should not use the table name for archive, the object instance and association
		// was saved in sharding tables, we still case the BKTableNameBaseInst here for the archive
		// error message in order to find the wrong table name used in logics level.

	default:
		if !common.IsObjectShardingTable(c.collName) {
			// do not archive the delete docs
			return nil
		}
	}

	docs := make([]bsonx.Doc, 0)
	cursor, err := c.dbc.Database(c.dbname).Collection(c.collName).Find(ctx, filter, nil)
	if err != nil {
		return err
	}

	if err := cursor.All(ctx, &docs); err != nil {
		return err
	}

	if len(docs) == 0 {
		return nil
	}

	archives := make([]interface{}, len(docs))
	for idx, doc := range docs {
		archives[idx] = metadata.DeleteArchive{
			Oid:    doc.Lookup("_id").ObjectID().Hex(),
			Detail: doc.Delete("_id"),
			Coll:   c.collName,
		}
	}

	_, err = c.dbc.Database(c.dbname).Collection(common.BKTableNameDelArchive).InsertMany(ctx, archives)
	return err
}

func (c *Mongo) redirectTable(tableName string) string {
	if common.IsObjectInstShardingTable(tableName) {
		tableName = common.BKTableNameBaseInst
	} else if common.IsObjectInstAsstShardingTable(tableName) {
		tableName = common.BKTableNameInstAsst
	}
	return tableName
}

// NextSequence 获取新序列号(非事务)
func (c *Mongo) NextSequence(ctx context.Context, sequenceName string) (uint64, error) {
	sequenceName = c.redirectTable(sequenceName)

	rid := ctx.Value(common.ContextRequestIDField)
	start := time.Now()
	defer func() {
		blog.V(4).InfoDepthf(2, "mongo next-sequence cost %dms, rid: %v", time.Since(start)/time.Millisecond, rid)
	}()

	// 直接使用新的context，确保不会用到事务,不会因为context含有session而使用分布式事务，防止产生相同的序列号
	ctx = context.Background()

	coll := c.dbc.Database(c.dbname).Collection("cc_idgenerator")

	Update := bson.M{
		"$inc":         bson.M{"SequenceID": int64(1)},
		"$setOnInsert": bson.M{"create_time": time.Now()},
		"$set":         bson.M{"last_time": time.Now()},
	}
	filter := bson.M{"_id": sequenceName}
	upsert := true
	returnChange := options.After
	opt := &options.FindOneAndUpdateOptions{
		Upsert:         &upsert,
		ReturnDocument: &returnChange,
	}

	doc := Idgen{}
	err := coll.FindOneAndUpdate(ctx, filter, Update, opt).Decode(&doc)
	if err != nil {
		return 0, err
	}
	return doc.SequenceID, err
}

// NextSequences 批量获取新序列号(非事务)
func (c *Mongo) NextSequences(ctx context.Context, sequenceName string, num int) ([]uint64, error) {
	if num == 0 {
		return make([]uint64, 0), nil
	}
	sequenceName = c.redirectTable(sequenceName)

	rid := ctx.Value(common.ContextRequestIDField)
	start := time.Now()
	defer func() {
		blog.V(4).InfoDepthf(2, "mongo next-sequences cost %dms, rid: %v", time.Since(start)/time.Millisecond, rid)
	}()

	// 直接使用新的context，确保不会用到事务,不会因为context含有session而使用分布式事务，防止产生相同的序列号
	ctx = context.Background()

	coll := c.dbc.Database(c.dbname).Collection("cc_idgenerator")

	Update := bson.M{
		"$inc":         bson.M{"SequenceID": num},
		"$setOnInsert": bson.M{"create_time": time.Now()},
		"$set":         bson.M{"last_time": time.Now()},
	}
	filter := bson.M{"_id": sequenceName}
	upsert := true
	returnChange := options.After
	opt := &options.FindOneAndUpdateOptions{
		Upsert:         &upsert,
		ReturnDocument: &returnChange,
	}

	doc := Idgen{}
	err := coll.FindOneAndUpdate(ctx, filter, Update, opt).Decode(&doc)
	if err != nil {
		return nil, err
	}

	sequences := make([]uint64, num)
	for i := 0; i < num; i++ {
		sequences[i] = uint64(i-num) + doc.SequenceID + 1
	}

	return sequences, err
}

type Idgen struct {
	ID         string `bson:"_id"`
	SequenceID uint64 `bson:"SequenceID"`
}

// HasTable 判断是否存在集合
func (c *Mongo) HasTable(ctx context.Context, collName string) (bool, error) {
	cursor, err := c.dbc.Database(c.dbname).ListCollections(ctx, bson.M{"name": collName, "type": "collection"})
	if err != nil {
		return false, err
	}

	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		return true, nil
	}

	return false, nil
}

// ListTables 获取所有的表名
func (c *Mongo) ListTables(ctx context.Context) ([]string, error) {
	return c.dbc.Database(c.dbname).ListCollectionNames(ctx, bson.M{"type": "collection"})

}

// DropTable 移除集合
func (c *Mongo) DropTable(ctx context.Context, collName string) error {
	return c.dbc.Database(c.dbname).Collection(collName).Drop(ctx)
}

// CreateTable 创建集合 TODO test
func (c *Mongo) CreateTable(ctx context.Context, collName string) error {
	return c.dbc.Database(c.dbname).RunCommand(ctx, map[string]interface{}{"create": collName}).Err()
}

// RenameTable 更新集合名称
func (c *Mongo) RenameTable(ctx context.Context, prevName, currName string) error {
	cmd := bson.D{
		{"renameCollection", c.dbname + "." + prevName},
		{"to", c.dbname + "." + currName},
	}
	return c.dbc.Database("admin").RunCommand(ctx, cmd).Err()
}

// CreateIndex 创建索引
func (c *Collection) CreateIndex(ctx context.Context, index types.Index) error {
	mtc.collectOperCount(c.collName, indexCreateOper)

	createIndexOpt := &options.IndexOptions{
		Background:              &index.Background,
		Unique:                  &index.Unique,
		PartialFilterExpression: index.PartialFilterExpression,
	}
	if index.Name != "" {
		createIndexOpt.Name = &index.Name
	}

	if index.ExpireAfterSeconds != 0 {
		createIndexOpt.SetExpireAfterSeconds(index.ExpireAfterSeconds)
	}

	createIndexInfo := mongo.IndexModel{
		Keys:    index.Keys,
		Options: createIndexOpt,
	}

	indexView := c.dbc.Database(c.dbname).Collection(c.collName).Indexes()
	_, err := indexView.CreateOne(ctx, createIndexInfo)
	if err != nil {
		mtc.collectErrorCount(c.collName, indexCreateOper)
		// ignore the following case
		// 1.the new index is exactly the same as the existing one
		// 2.the new index has same keys with the existing one, but its name is different
		if strings.Contains(err.Error(), "all indexes already exist") ||
			strings.Contains(err.Error(), "already exists with a different name") {
			return nil
		}
	}

	return err
}

// DropIndex remove index by name
func (c *Collection) DropIndex(ctx context.Context, indexName string) error {
	mtc.collectOperCount(c.collName, indexDropOper)
	indexView := c.dbc.Database(c.dbname).Collection(c.collName).Indexes()
	_, err := indexView.DropOne(ctx, indexName)
	if err != nil {
		if strings.Contains(err.Error(), "IndexNotFound") {
			return nil
		}
		mtc.collectErrorCount(c.collName, indexDropOper)
		return err
	}
	return nil
}

// Indexes get all indexes for the collection
func (c *Collection) Indexes(ctx context.Context) ([]types.Index, error) {
	indexView := c.dbc.Database(c.dbname).Collection(c.collName).Indexes()
	cursor, err := indexView.List(ctx)
	if nil != err {
		return nil, err
	}
	defer cursor.Close(ctx)
	var indexes []types.Index
	for cursor.Next(ctx) {
		idxResult := types.Index{}
		cursor.Decode(&idxResult)
		indexes = append(indexes, idxResult)
	}

	return indexes, nil
}

// AddColumn add a new column for the collection
func (c *Collection) AddColumn(ctx context.Context, column string, value interface{}) error {
	mtc.collectOperCount(c.collName, columnOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, columnOper, time.Since(start))
	}()

	selector := dtype.Document{column: dtype.Document{"$exists": false}}
	datac := dtype.Document{"$set": dtype.Document{column: value}}
	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, selector, datac)
		if err != nil {
			mtc.collectErrorCount(c.collName, columnOper)
			return err
		}
		return nil
	})
}

// RenameColumn rename a column for the collection
func (c *Collection) RenameColumn(ctx context.Context, filter types.Filter, oldName, newColumn string) error {
	mtc.collectOperCount(c.collName, columnOper)
	if filter == nil {
		filter = dtype.Document{}
	}

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, columnOper, time.Since(start))
	}()

	datac := dtype.Document{"$rename": dtype.Document{oldName: newColumn}}
	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, filter, datac)
		if err != nil {
			mtc.collectErrorCount(c.collName, columnOper)
			return err
		}

		return nil
	})
}

// DropColumn remove a column by the name
func (c *Collection) DropColumn(ctx context.Context, field string) error {
	mtc.collectOperCount(c.collName, columnOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, columnOper, time.Since(start))
	}()

	datac := dtype.Document{"$unset": dtype.Document{field: ""}}
	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, dtype.Document{}, datac)
		if err != nil {
			mtc.collectErrorCount(c.collName, columnOper)
			return err
		}

		return nil
	})
}

// DropColumns remove many columns by the name
func (c *Collection) DropColumns(ctx context.Context, filter types.Filter, fields []string) error {
	mtc.collectOperCount(c.collName, columnOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, columnOper, time.Since(start))
	}()

	unsetFields := make(map[string]interface{})
	for _, field := range fields {
		unsetFields[field] = ""
	}

	datac := dtype.Document{"$unset": unsetFields}
	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, filter, datac)
		if err != nil {
			mtc.collectErrorCount(c.collName, columnOper)
			return err
		}

		return nil
	})
}

// DropDocsColumn remove a column by the name for doc use filter
func (c *Collection) DropDocsColumn(ctx context.Context, field string, filter types.Filter) error {
	mtc.collectOperCount(c.collName, columnOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, columnOper, time.Since(start))
	}()

	// 查询条件为空时候，mongodb 不返回数据
	if filter == nil {
		filter = bson.M{}
	}

	datac := dtype.Document{"$unset": dtype.Document{field: ""}}
	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		_, err := c.dbc.Database(c.dbname).Collection(c.collName).UpdateMany(ctx, filter, datac)
		if err != nil {
			mtc.collectErrorCount(c.collName, columnOper)
			return err
		}

		return nil
	})
}

// AggregateAll aggregate all operation
func (c *Collection) AggregateAll(ctx context.Context, pipeline interface{}, result interface{},
	opts ...*types.AggregateOpts) error {

	mtc.collectOperCount(c.collName, aggregateOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, aggregateOper, time.Since(start))
	}()

	var aggregateOption *options.AggregateOptions
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if opt.AllowDiskUse != nil {
			aggregateOption = &options.AggregateOptions{AllowDiskUse: opt.AllowDiskUse}
		}
	}

	opt := getCollectionOption(ctx)

	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		cursor, err := c.dbc.Database(c.dbname).Collection(c.collName, opt).Aggregate(ctx, pipeline, aggregateOption)
		if err != nil {
			mtc.collectErrorCount(c.collName, aggregateOper)
			return err
		}
		defer cursor.Close(ctx)
		return decodeCursorIntoSlice(ctx, cursor, result)
	})

}

// AggregateOne aggregate one operation
func (c *Collection) AggregateOne(ctx context.Context, pipeline interface{}, result interface{}) error {
	mtc.collectOperCount(c.collName, aggregateOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, aggregateOper, time.Since(start))
	}()

	opt := getCollectionOption(ctx)

	return c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		cursor, err := c.dbc.Database(c.dbname).Collection(c.collName, opt).Aggregate(ctx, pipeline)
		if err != nil {
			mtc.collectErrorCount(c.collName, aggregateOper)
			return err
		}

		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			return cursor.Decode(result)
		}
		return types.ErrDocumentNotFound
	})

}

// Distinct Finds the distinct values for a specified field across a single collection or view and returns the results in an
// field the field for which to return distinct values.
// filter query that specifies the documents from which to retrieve the distinct values.
func (c *Collection) Distinct(ctx context.Context, field string, filter types.Filter) ([]interface{}, error) {
	mtc.collectOperCount(c.collName, distinctOper)

	start := time.Now()
	defer func() {
		mtc.collectOperDuration(c.collName, distinctOper, time.Since(start))
	}()

	if filter == nil {
		filter = bson.M{}
	}

	opt := getCollectionOption(ctx)
	var results []interface{} = nil
	err := c.tm.AutoRunWithTxn(ctx, c.dbc, func(ctx context.Context) error {
		var err error
		results, err = c.dbc.Database(c.dbname).Collection(c.collName, opt).Distinct(ctx, field, filter)
		if err != nil {
			mtc.collectErrorCount(c.collName, distinctOper)
			return err
		}

		return nil
	})
	return results, err
}

func (f *Find) Option(opts ...*types.FindOpts) {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if opt.WithObjectID != nil {
			f.option.WithObjectID = opt.WithObjectID
		}
		if opt.WithCount != nil {
			f.option.WithCount = opt.WithCount
		}
	}
}

func (f *Find) generateMongoOption() *options.FindOptions {
	findOpts := &options.FindOptions{}
	if f.projection == nil {
		f.projection = make(map[string]int, 0)
	}
	if f.option.WithObjectID != nil && *f.option.WithObjectID {
		// mongodb 要求，当有字段设置未1, 不设置都不显示
		// 没有设置projection 的时候，返回所有字段
		if len(f.projection) > 0 {
			f.projection["_id"] = 1
		}
	} else {
		if _, exists := f.projection["_id"]; !exists {
			f.projection["_id"] = 0
		}
	}
	if len(f.projection) != 0 {
		findOpts.Projection = f.projection
	}

	if f.start != 0 {
		findOpts.SetSkip(f.start)
	}
	if f.limit != 0 {
		findOpts.SetLimit(f.limit)
	}
	if len(f.sort) != 0 {
		findOpts.SetSort(f.sort)
	}

	return findOpts
}

func decodeCursorIntoSlice(ctx context.Context, cursor *mongo.Cursor, result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		return errors.New("result argument must be a slice address")
	}

	elemt := resultv.Elem().Type().Elem()
	slice := reflect.MakeSlice(resultv.Elem().Type(), 0, 10)
	for cursor.Next(ctx) {
		elemp := reflect.New(elemt)
		if err := cursor.Decode(elemp.Interface()); nil != err {
			return err
		}
		slice = reflect.Append(slice, elemp.Elem())
	}
	if err := cursor.Err(); err != nil {
		return err
	}

	resultv.Elem().Set(slice)
	return nil
}

// validHostType valid if host query uses specified type that transforms ip & operator array to string
func validHostType(collection string, projection map[string]int, result interface{}, rid interface{}) error {
	if result == nil {
		blog.Errorf("host query result is nil, rid: %s", rid)
		return fmt.Errorf("host query result type invalid")
	}

	if collection != common.BKTableNameBaseHost {
		return nil
	}

	// check if specified fields include special fields
	if len(projection) != 0 {
		needCheck := false
		for field := range projection {
			if hostSpecialFieldMap[field] {
				needCheck = true
				break
			}
		}
		if !needCheck {
			return nil
		}
	}

	resType := reflect.TypeOf(result)
	if resType.Kind() != reflect.Ptr {
		blog.Errorf("host query result type(%v) not pointer type, rid: %v", resType, rid)
		return fmt.Errorf("host query result type invalid")
	}
	// if result is *map[string]interface{} type, it must be *metadata.HostMapStr type
	if resType.ConvertibleTo(reflect.TypeOf(&map[string]interface{}{})) {
		if resType != reflect.TypeOf(&metadata.HostMapStr{}) {
			blog.Errorf("host query result type(%v) not match *metadata.HostMapStr type, rid: %v", resType, rid)
			return fmt.Errorf("host query result type invalid")
		}
		return nil
	}

	resElem := resType.Elem()
	switch resElem.Kind() {
	case reflect.Struct:
		// if result is *struct type, the special field in it must be metadata.StringArrayToString type
		numField := resElem.NumField()
		validType := reflect.TypeOf(metadata.StringArrayToString(""))
		for i := 0; i < numField; i++ {
			field := resElem.Field(i)
			bsonTag := field.Tag.Get("bson")
			if bsonTag == "" {
				blog.Errorf("host query result field(%s) has empty bson tag, rid: %v", field.Name, rid)
				return fmt.Errorf("host query result type invalid")
			}
			if hostSpecialFieldMap[bsonTag] && field.Type != validType {
				blog.Errorf("host query result field type(%v) not match *metadata.StringArrayToString type", field.Type)
				return fmt.Errorf("host query result type invalid")
			}
		}
	case reflect.Slice:
		// check if slice item is valid type, map or struct validation is similar as before
		elem := resElem.Elem()
		if elem.ConvertibleTo(reflect.TypeOf(map[string]interface{}{})) {
			if elem != reflect.TypeOf(metadata.HostMapStr{}) {
				blog.Errorf("host query result type(%v) not match *[]metadata.HostMapStr type", resType)
				return fmt.Errorf("host query result type invalid")
			}
			return nil
		}

		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}

		if elem.Kind() != reflect.Struct {
			blog.Errorf("host query result type(%v) not struct pointer type or map type", resType)
			return fmt.Errorf("host query result type invalid")
		}
		numField := elem.NumField()
		validType := reflect.TypeOf(metadata.StringArrayToString(""))
		for i := 0; i < numField; i++ {
			field := elem.Field(i)
			bsonTag := field.Tag.Get("bson")
			if bsonTag == "" {
				blog.Errorf("host query result field(%s) has empty bson tag, rid: %v", field.Name, rid)
				return fmt.Errorf("host query result type invalid")
			}
			if hostSpecialFieldMap[bsonTag] && field.Type != validType {
				blog.Errorf("host query result field type(%v) not match *metadata.StringArrayToString type", field.Type)
				return fmt.Errorf("host query result type invalid")
			}
		}
	default:
		blog.Errorf("host query result type(%v) not pointer of map, struct or slice, rid: %v", resType, rid)
		return fmt.Errorf("host query result type invalid")
	}
	return nil
}

const (
	// reference doc:
	// https://docs.mongodb.com/manual/core/read-preference-staleness/#replica-set-read-preference-max-staleness
	// this is the minimum value of maxStalenessSeconds allowed.
	// specifying a smaller maxStalenessSeconds value will raise an error. Clients estimate secondaries’ staleness
	// by periodically checking the latest write date of each replica set member. Since these checks are infrequent,
	// the staleness estimate is coarse. Thus, clients cannot enforce a maxStalenessSeconds value of less than
	// 90 seconds.
	maxStalenessSeconds = 90 * time.Second
)

func getCollectionOption(ctx context.Context) *options.CollectionOptions {
	var opt *options.CollectionOptions
	switch util.GetDBReadPreference(ctx) {

	case common.NilMode:

	case common.PrimaryMode:
		opt = &options.CollectionOptions{
			ReadPreference: readpref.Primary(),
		}
	case common.PrimaryPreferredMode:
		opt = &options.CollectionOptions{
			ReadPreference: readpref.PrimaryPreferred(readpref.WithMaxStaleness(maxStalenessSeconds)),
		}
	case common.SecondaryMode:
		opt = &options.CollectionOptions{
			ReadPreference: readpref.Secondary(readpref.WithMaxStaleness(maxStalenessSeconds)),
		}
	case common.SecondaryPreferredMode:
		opt = &options.CollectionOptions{
			ReadPreference: readpref.SecondaryPreferred(readpref.WithMaxStaleness(maxStalenessSeconds)),
		}
	case common.NearestMode:
		opt = &options.CollectionOptions{
			ReadPreference: readpref.Nearest(readpref.WithMaxStaleness(maxStalenessSeconds)),
		}
	}

	return opt
}
