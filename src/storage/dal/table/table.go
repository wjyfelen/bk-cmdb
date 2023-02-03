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

package table

import (
	"reflect"
	"strings"

	"configcenter/src/common/criteria/enumor"
)

// Fields table's fields details.
type Fields struct {
	// fieldType the type corresponding to the field.
	fieldType map[string]enumor.FieldType
	// isEditable the type corresponding to the field.
	isEditable map[string]bool
	// isRequired the type corresponding to the field.
	isRequired map[string]bool
	// ignoredClusterFields fields that need to be ignored in the update scenario.
	ignoredUpdateFields []string
}

// FieldsDescriptors table of field descriptor.
type FieldsDescriptors []FieldDescriptor

func getIgnoredUpdateFields(data interface{}) []string {
	typeOfOption := reflect.TypeOf(data)
	fields := make([]string, 0)
	for i := 0; i < typeOfOption.NumField(); i++ {
		tags := strings.Split(typeOfOption.Field(i).Tag.Get("json"), ",")
		if tags[0] == "" {
			continue
		}

		fields = append(fields, tags[0])
	}
	return []string{}
}

// SetUpdateIgnoreFields  set the fields that need to be ignored in the update scene
// 1、for this resource attribute itself, it is intercepted through the isEditable field
// 2、for basic fields (such as bk_biz_id, bk_supplier_account...) and redundant fields
// for convenient query, you need to use this method to set and ignore update fields.
func (f *Fields) SetUpdateIgnoreFields(baseFields []string, data []interface{}) {
	f.ignoredUpdateFields = append(f.ignoredUpdateFields, baseFields...)
	if data == nil {
		return
	}
	for _, d := range data {

		fs := getIgnoredUpdateFields(d)
		if len(fs) == 0 {
			continue
		}

		f.ignoredUpdateFields = append(f.ignoredUpdateFields, fs...)
	}
}

// MergeFields merging of table fields.
func MergeFields(all ...FieldsDescriptors) *Fields {
	result := &Fields{
		fieldType:           make(map[string]enumor.FieldType),
		isEditable:          make(map[string]bool),
		isRequired:          make(map[string]bool),
		ignoredUpdateFields: make([]string, 0),
	}

	if len(all) == 0 {
		return result
	}
	for _, col := range all {
		for _, f := range col {
			result.fieldType[f.Field] = f.Type
			result.isEditable[f.Field] = f.IsEditable
			result.isRequired[f.Field] = f.IsRequired
		}
	}
	return result
}

// FieldsType returns the corresponding type of all fields.
func (f Fields) FieldsType() map[string]enumor.FieldType {
	copied := make(map[string]enumor.FieldType)
	for k, v := range f.fieldType {
		copied[k] = v
	}
	return copied
}

// GetUpdateIgnoredFields returns the fields that need to be
// ignored for the specified resource in the update scenario.
func (f Fields) GetUpdateIgnoredFields() []string {
	return f.ignoredUpdateFields
}

// FieldsEditable returns the corresponding editable of all fields.
func (f Fields) FieldsEditable() map[string]bool {
	copied := make(map[string]bool)
	for k, v := range f.isEditable {
		copied[k] = v
	}
	return copied
}

// RequiredFields returns the corresponding required of all fields.
func (f Fields) RequiredFields() map[string]bool {
	copied := make(map[string]bool)
	for k, v := range f.isRequired {
		copied[k] = v
	}
	return copied
}

// EditableFields returns the corresponding editable of all fields.
func (f Fields) EditableFields() map[string]bool {
	copied := make(map[string]bool)
	for k, v := range f.isEditable {
		copied[k] = v
	}
	return copied
}

// IsFieldRequiredByField returns the corresponding editable of specified field.
func (f Fields) IsFieldRequiredByField(field string) bool {
	return f.isRequired[field]
}

// IsFieldEditableByField returns the corresponding editable of specified field.
func (f Fields) IsFieldEditableByField(field string) bool {
	return f.isEditable[field]
}

// FieldDescriptor defines a table's field related information.
type FieldDescriptor struct {
	// Field is field's name.
	Field string
	// Type is this field's data type.
	Type enumor.FieldType
	// IsRequired is it required.
	IsRequired bool
	// IsEditable is it editable.
	IsEditable bool
	// Option additional information for the field.
	// the content corresponding to different fields may be different.
	Option interface{}
	_      struct{}
}

// Revision resource revision information.
type Revision struct {
	Creator    string `json:"creator,omitempty" bson:"creator"`
	Modifier   string `json:"modifier,omitempty" bson:"modifier"`
	CreateTime int64  `json:"create_time,omitempty" bson:"create_time"`
	LastTime   int64  `json:"last_time,omitempty" bson:"last_time"`
}
