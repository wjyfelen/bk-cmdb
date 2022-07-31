/*
 * Tencent is pleased to support the open source community by making
 * 蓝鲸智云 - 配置平台 (BlueKing - Configuration System) available.
 * Copyright (C) 2017 THL A29 Limited,
 * a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * We undertake not to change the open source license (MIT license) applicable
 * to the current version of the project delivered to anyone in the future.
 */

package types

import (
	meta "configcenter/src/common/metadata"
	"errors"
	"time"
)

// identification of k8s in cc
const (
	// KubeCluster k8s cluster type
	KubeCluster = "cluster"

	// KubeNode k8s node type
	KubeNode = "node"

	// KubeNamespace k8s namespace type
	KubeNamespace = "namespace"

	// KubeWorkload k8s workload type
	KubeWorkload = "workload"

	// KubePod k8s pod type
	KubePod = "pod"

	// KubeContainer k8s container type
	KubeContainer = "container"
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

// CreateClusterResult 创建集群结果
type CreateClusterResult struct {
	meta.BaseResp
	Info *Cluster `field:"id" json:"id" bson:"id"`
}

// ClusterOption 创建集群请求字段
type ClusterBaseFields struct {
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

// ValidateCreate 校验创建集群参数是否合法
func (option *ClusterBaseFields) ValidateCreate() error {

	if option.Name == nil || *option.Name == "" {
		return errors.New("name can not be empty")
	}
	if err := ValidateString(*option.Name, StringSettings{}); err != nil {
		return err
	}

	if option.Uid == nil || *option.Uid == "" {
		return errors.New("uid can not be empty")
	}
	if err := ValidateString(*option.Uid, StringSettings{}); err != nil {
		return err
	}
	return nil
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

// WorkloadType workload type enum
type WorkloadType string

const (
	// KubeDeployment k8s deployment type
	KubeDeployment WorkloadType = "deployment"

	// KubeStatefulSet k8s statefulSet type
	KubeStatefulSet WorkloadType = "statefulSet"

	// KubeDaemonSet k8s daemonSet type
	KubeDaemonSet WorkloadType = "daemonSet"

	// KubeGameStatefulSet k8s gameStatefulSet type
	KubeGameStatefulSet WorkloadType = "gameStatefulSet"

	// KubeGameDeployment k8s gameDeployment type
	KubeGameDeployment WorkloadType = "gameDeployment"

	// KubeCronJob k8s cronJob type
	KubeCronJob WorkloadType = "cronJob"

	// KubeJob k8s job type
	KubeJob WorkloadType = "job"

	// KubePodWorkload k8s pod workload type
	KubePodWorkload WorkloadType = "pods"
)

// table names
const (
	// BKTableNameBaseCluster the table name of the Cluster
	BKTableNameBaseCluster = "cc_ClusterBase"

	// BKTableNameBaseNode the table name of the Node
	BKTableNameBaseNode = "cc_NodeBase"

	// BKTableNameBaseNamespace the table name of the Namespace
	BKTableNameBaseNamespace = "cc_NamespaceBase"

	// BKTableNameBaseDeployment the table name of the Deployment
	BKTableNameBaseDeployment = "cc_DeploymentBase"

	// BKTableNameBaseStatefulSet the table name of the StatefulSet
	BKTableNameBaseStatefulSet = "cc_StatefulSetBase"

	// BKTableNameBaseDaemonSet the table name of the DaemonSet
	BKTableNameBaseDaemonSet = "cc_DaemonSetBase"

	// BKTableNameGameDeployment the table name of the GameDeployment
	BKTableNameGameDeployment = "cc_GameDeploymentBase"

	// BKTableNameGameStatefulSet the table name of the GameStatefulSet
	BKTableNameGameStatefulSet = "cc_GameStatefulSetBase"

	// BKTableNameBaseCronJob the table name of the CronJob
	BKTableNameBaseCronJob = "cc_CronJobBase"

	// BKTableNameBaseJob the table name of the Job
	BKTableNameBaseJob = "cc_JobBase"

	// BKTableNameBasePodWorkload the table name of the Pod Workload
	BKTableNameBasePodWorkload = "cc_PodWorkloadBase"

	// BKTableNameBaseCustom the table name of the Custom Workload
	BKTableNameBaseCustom = "cc_CustomBase"

	// BKTableNameBasePod the table name of the Pod
	BKTableNameBasePod = "cc_PodBase"

	// BKTableNameBaseContainer the table name of the Container
	BKTableNameBaseContainer = "cc_ContainerBase"
)

// common field names
const (
	// UidField unique id field in third party platform
	UidField = "uid"

	// LabelsField object labels field
	LabelsField = "labels"

	// KindField object kind field
	KindField = "kind"
)

// cluster field names
const (
	// BKClusterIDFiled cluster unique id field in cc
	BKClusterIDFiled = "bk_cluster_id"

	// ClusterUIDField cluster unique id field in third party platform
	ClusterUIDField = "cluster_uid"

	// XidField base cluster id field
	XidField = "xid"

	// VersionField cluster version field
	VersionField = "version"

	// NetworkTypeField cluster network type field
	NetworkTypeField = "network_type"

	// RegionField cluster region field
	RegionField = "region"

	// VpcField cluster vpc field
	VpcField = "vpc"

	// NetworkField cluster network field
	NetworkField = "network"

	// TypeField cluster type field
	TypeField = "type"
)

// node field names
const (
	// RolesField node role field
	RolesField = "roles"

	// TaintsField node taints field
	TaintsField = "taints"

	// UnschedulableField node unschedulable field
	UnschedulableField = "unschedulable"

	// InternalIPField node internal ip field
	InternalIPField = "internal_ip"

	// ExternalIPField node external ip field
	ExternalIPField = "external_ip"

	// HostnameField node hostname field
	HostnameField = "hostname"

	// RuntimeComponentField node runtime component field
	RuntimeComponentField = "runtime_component"

	// KubeProxyModeField node proxy mode field
	KubeProxyModeField = "kube_proxy_mode"

	// BKNodeIDField cluster unique id field in cc
	BKNodeIDField = "bk_node_id"

	// NodeField node name field in third party platform
	NodeField = "node"
)

// namespace field names
const (
	// ResourceQuotasField namespace resource quotas field
	ResourceQuotasField = "resource_quotas"

	// BKNamespaceIDField namespace unique id field in cc
	BKNamespaceIDField = "bk_namespace_id"

	// NamespaceField namespace name field in third party platform
	NamespaceField = "namespace"
)

// workload fields names
const (
	// SelectorField workload selector field
	SelectorField = "selector"

	// ReplicasField workload replicas field
	ReplicasField = "replicas"

	// StrategyTypeField workload strategy type field
	StrategyTypeField = "strategy_type"

	// MinReadySecondsField workload minimum ready seconds field
	MinReadySecondsField = "min_ready_seconds"

	// RollingUpdateStrategyField workload rolling update strategy field
	RollingUpdateStrategyField = "rolling_update_strategy"
)

// pod field names
const (
	// PriorityField pod priority field
	PriorityField = "priority"

	// IPField pod ip field
	IPField = "ip"

	// IPsField pod ips field
	IPsField = "ips"

	// VolumesField pod volumes field
	VolumesField = "volumes"

	// QOSClassField pod qos class field
	QOSClassField = "qos_class"

	// NodeSelectorsField pod node selectors field
	NodeSelectorsField = "node_selectors"

	// TolerationsField pod tolerations field
	TolerationsField = "tolerations"

	// BKPodIDField pod unique id field in cc
	BKPodIDField = "bk_pod_id"

	// PodUIDField pod unique id field in third party platform
	PodUIDField = "pod_uid"
)

// container field names
const (
	// ContainerUIDField container unique id field in third party platform
	ContainerUIDField = "container_uid"

	// BKContainerIDField container unique id field in cc
	BKContainerIDField = "bk_container_id"

	// ImageField container image field
	ImageField = "image"

	// PortsField container ports field
	PortsField = "ports"

	// HostPortsField container host ports field
	HostPortsField = "host_ports"

	// ArgsField container args field
	ArgsField = "args"

	// StartedField container started Field
	StartedField = "started"

	// LimitsField container limits field
	LimitsField = "limits"

	// container requests field
	RequestsField = "requests"

	// LivenessField container liveness field
	LivenessField = "liveness"

	// EnvironmentField container environment field
	EnvironmentField = "environment"

	// MountsField container mounts field
	MountsField = "mounts"
)
