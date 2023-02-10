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
	"errors"
	"time"

	"configcenter/src/common"
	"configcenter/src/common/criteria/enumor"
	ccErr "configcenter/src/common/errors"
	"configcenter/src/kube/orm"
	"configcenter/src/storage/dal/table"
)

// CronJobFields merge the fields of the CronJob and the details corresponding to the fields together.
var CronJobFields = table.MergeFields(CommonSpecFieldsDescriptor, NamespaceBaseRefDescriptor,
	ClusterBaseRefDescriptor, CronJobSpecFieldsDescriptor)

// CronJobSpecFieldsDescriptor CronJob spec's fields descriptors.
var CronJobSpecFieldsDescriptor = table.FieldsDescriptors{
	{Field: KubeNameField, Type: enumor.String, IsRequired: true, IsEditable: false},
	{Field: LabelsField, Type: enumor.MapString, IsRequired: false, IsEditable: true},
	{Field: SelectorField, Type: enumor.Object, IsRequired: false, IsEditable: true},
	{Field: ReplicasField, Type: enumor.Numeric, IsRequired: false, IsEditable: true},
	{Field: StrategyTypeField, Type: enumor.String, IsRequired: false, IsEditable: true},
	{Field: MinReadySecondsField, Type: enumor.Numeric, IsRequired: false, IsEditable: true},
	{Field: RollingUpdateStrategyField, Type: enumor.Object, IsRequired: false, IsEditable: true},
}

// CronJob define the cronJob struct.
type CronJob struct {
	WorkloadBase    `json:",inline" bson:",inline"`
	Labels          *map[string]string `json:"labels,omitempty" bson:"labels"`
	Selector        *LabelSelector     `json:"selector,omitempty" bson:"selector"`
	Replicas        *int64             `json:"replicas,omitempty" bson:"replicas"`
	MinReadySeconds *int64             `json:"min_ready_seconds,omitempty" bson:"min_ready_seconds"`
}

// GetWorkloadBase get workload base
func (c *CronJob) GetWorkloadBase() WorkloadBase {
	return c.WorkloadBase
}

// SetWorkloadBase set workload base
func (c *CronJob) SetWorkloadBase(wl WorkloadBase) {
	c.WorkloadBase = wl
}

// ValidateCreate validate create workload
func (w *CronJob) ValidateCreate() ccErr.RawErrorInfo {
	if w == nil {
		return ccErr.RawErrorInfo{
			ErrCode: common.CCErrCommParamsNeedSet,
			Args:    []interface{}{"data"},
		}
	}

	if err := ValidateCreate(*w, CronJobFields); err.ErrCode != 0 {
		return err
	}

	return ccErr.RawErrorInfo{}
}

// ValidateUpdate validate update workload
func (w *CronJob) ValidateUpdate() ccErr.RawErrorInfo {
	if w == nil {
		return ccErr.RawErrorInfo{
			ErrCode: common.CCErrCommParamsNeedSet,
			Args:    []interface{}{"data"},
		}
	}

	if err := ValidateUpdate(*w, CronJobFields); err.ErrCode != 0 {
		return err
	}

	return ccErr.RawErrorInfo{}
}

// BuildUpdateData build cronJob update data
func (w *CronJob) BuildUpdateData(user string) (map[string]interface{}, error) {
	if w == nil {
		return nil, errors.New("update param is invalid")
	}

	opts := orm.NewFieldOptions().AddIgnoredFields(CronJobFields.GetUpdateIgnoredFields()...)
	updateData, err := orm.GetUpdateFieldsWithOption(w, opts)
	if err != nil {
		return nil, err
	}
	updateData[common.LastTimeField] = time.Now().Unix()
	updateData[common.ModifierField] = user
	return updateData, err
}

// initCronJobUpdateIgnoreFields ignore non-updatable fields related to cronjob resources
func initCronJobUpdateIgnoreFields() {
	var (
		cluster   ClusterSpec
		namespace Namespace
	)
	CronJobFields.SetUpdateIgnoreFields(IgnoredUpdateBaseFields, cluster, namespace)
}
