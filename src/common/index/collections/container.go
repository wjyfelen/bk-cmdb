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

package collections

import (
	"configcenter/src/common"
	"configcenter/src/storage/dal/types"
)

func init() {
	registerIndexes("cc_ClusterBase", commClusterIndexes)
	registerIndexes("cc_NodeBase", commNodeIndexes)
	registerIndexes("cc_NamespaceBase", commNamespaceIndexes)
	registerIndexes("cc_PodBase", commPodIndexes)
	registerIndexes("cc_ContainerBase", commContainerIndexes)

	workLoadTables := []string{"cc_DeploymentBase", "cc_DaemonSet", "cc_StatefulSet", "cc_GameStatefulSet",
		"cc_GameDeployment", "cc_CronJob", "cc_Job", "cc_Pods"}
	for _, table := range workLoadTables {
		registerIndexes(table, commWorkLoadIndexes)
	}
}

var commWorkLoadIndexes = []types.Index{
	{
		Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
		Keys:       map[string]int32{common.BKFieldID: 1},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + "bk_namespace_id" + common.BKFieldName,
		Keys: map[string]int32{
			"bk_namespace_id":  1,
			common.BKFieldName: 1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKAppIDField + common.BKOwnerIDField + "cluster_uid" +
			"namespace" + common.BKFieldName,
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			"cluster_uid":         1,
			"namespace":           1,
			common.BKFieldName:    1,
		},
		Unique:     true,
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "cluster_uid",
		Keys:       map[string]int32{"cluster_uid": 1},
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "bk_cluster_id",
		Keys:       map[string]int32{"bk_cluster_id": 1},
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + common.BKFieldName,
		Keys:       map[string]int32{common.BKFieldName: 1},
		Background: true,
	},
}
var commContainerIndexes = []types.Index{
	{
		Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
		Keys:       map[string]int32{common.BKFieldID: 1},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + "bk_pod_id" + "container_uid",
		Keys: map[string]int32{
			"bk_pod_id":     1,
			"container_uid": 1,
		},
		Background: true,
		Unique:     true,
	},
}
var commPodIndexes = []types.Index{
	{
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
		Keys: map[string]int32{
			common.BKFieldID: 1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + "bk_reference_id" + "reference_kind" + common.BKFieldName,
		Keys: map[string]int32{
			"bk_reference_id":  1,
			"reference_kind":   1,
			common.BKFieldName: 1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKAppIDField + common.BKOwnerIDField + "cluster_uid" +
			"namespace" + "reference_kind" + "reference_name" + common.BKFieldName,
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			"cluster_uid":         1,
			"namespace":           1,
			"reference_kind":      1,
			"reference_name":      1,
			common.BKFieldName:    1,
		},
		Unique:     true,
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "node_name",
		Keys:       map[string]int32{"node_name": 1},
		Background: true,
	},
	{
		// NOCC:tosa/linelength(忽略长度)
		Name: common.CCLogicIndexNamePrefix + common.BKAppIDField + common.BKOwnerIDField + "reference_name" + "reference_kind",
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			"reference_name":      1,
			"reference_kind":      1,
		},
		Background: true,
	},
	{
		Name: common.CCLogicIndexNamePrefix + "bk_reference_id" + "reference_kind",
		Keys: map[string]int32{
			"bk_reference_id": 1,
			"reference_kind":  1,
		},
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + common.BKHostIDField,
		Keys:       map[string]int32{common.BKHostIDField: 1},
		Background: true,
	},
}
var commNamespaceIndexes = []types.Index{
	{
		Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
		Keys:       map[string]int32{common.BKFieldID: 1},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + "bk_cluster_id" + common.BKFieldName,
		Keys: map[string]int32{
			"bk_cluster_id":    1,
			common.BKFieldName: 1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKAppIDField + common.BKOwnerIDField + "cluster_uid" +
			common.BKFieldName,
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			"cluster_uid":         1,
			common.BKFieldName:    1,
		},
		Unique:     true,
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "cluster_uid",
		Keys:       map[string]int32{"cluster_uid": 1},
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "bk_cluster_id",
		Keys:       map[string]int32{"bk_cluster_id": 1},
		Background: true,
	},
}
var commNodeIndexes = []types.Index{
	{
		Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
		Keys:       map[string]int32{common.BKFieldID: 1},
		Background: true,
		Unique:     true,
	},
	{
		// NOCC:tosa/linelength(忽略长度)
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKAppIDField + common.BKOwnerIDField + "cluster_uid" + common.BKFieldName,
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			"cluster_uid":         1,
			common.BKFieldID:      1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + "bk_cluster_id" + common.BKFieldID,
		Keys: map[string]int32{
			"bk_cluster_id":  1,
			common.BKFieldID: 1,
		},
		Unique:     true,
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "cluster_uid",
		Keys:       map[string]int32{"cluster_uid": 1},
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "bk_cluster_id",
		Keys:       map[string]int32{"bk_cluster_id": 1},
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + common.BKHostIDField,
		Keys:       map[string]int32{common.BKHostIDField: 1},
		Background: true,
	},
}
var commClusterIndexes = []types.Index{

	{
		Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
		Keys:       map[string]int32{common.BKFieldID: 1},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKAppIDField + common.BKOwnerIDField + "uid",
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			"uid":                 1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKAppIDField + common.BKOwnerIDField + common.BKFieldName,
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			common.BKFieldName:    1,
		},
		Unique:     true,
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + common.BKAppIDField,
		Keys:       map[string]int32{common.BKAppIDField: 1},
		Background: true,
	},
	{
		Name:       common.CCLogicIndexNamePrefix + "xid",
		Keys:       map[string]int32{"xid": 1},
		Background: true,
	},
}
