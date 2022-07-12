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

package y3_10_202207251408

import (
	"context"
	"errors"

	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/storage/dal"
	"configcenter/src/storage/dal/types"
)

func addContainerCollection(ctx context.Context, db dal.RDB) error {

	containerCollections := []string{"cc_ClusterBase", "cc_NodeBase", "cc_NamespaceBase", "cc_PodBase",
		"cc_ContainerBase", "cc_DeploymentBase", "cc_DaemonSetBase", "cc_StatefulSetBase", "cc_GameStatefulSetBase",
		"cc_GameDeploymentBase", "cc_CronJobBase", "cc_JobBase", "cc_PodsBase"}

	for _, collection := range containerCollections {
		exists, err := db.HasTable(ctx, collection)
		if err != nil {
			blog.Errorf("check if %s table exists failed, err: %v", collection, err)
			return err
		}

		if !exists {
			if err := db.CreateTable(ctx, collection); err != nil {
				blog.Errorf("create %s table failed, err: %v", collection, err)
				return err
			}
		}
	}
	return nil
}

func addContainerCollectionIndex(ctx context.Context, db dal.RDB) error {

	if err := addClusterTableIndexes(ctx, db); err != nil {
		return err
	}

	if err := addNodeTableIndexes(ctx, db); err != nil {
		return err
	}

	if err := addNamespaceTableIndexes(ctx, db); err != nil {
		return err
	}

	if err := addPodTableIndexes(ctx, db); err != nil {
		return err
	}

	if err := addContainerTableIndexes(ctx, db); err != nil {
		return err
	}

	workLoadTables := []string{"cc_DeploymentBase", "cc_DaemonSetBase", "cc_StatefulSetBase", "cc_GameStatefulSetBase",
		"cc_GameDeploymentBase", "cc_CronJobBase", "cc_JobBase", "cc_PodsBase"}

	for _, table := range workLoadTables {
		if err := addWorkLoadTableIndexes(ctx, db, table); err != nil {
			return err
		}
	}
	return nil
}

func addContainerTableIndexes(ctx context.Context, db dal.RDB) error {
	indexes := []types.Index{
		{
			Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
			Keys:       map[string]int32{common.BKFieldID: 1},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_pod_id_container_uid",
			Keys: map[string]int32{
				"bk_pod_id":     1,
				"container_uid": 1,
			},
			Background: true,
			Unique:     true,
		},
	}

	existIndexArr, err := db.Table("cc_ContainerBase").Indexes(ctx)
	if err != nil {
		blog.Errorf("get exist index for container table failed, err: %v", err)
		return err
	}

	existIdxMap := make(map[string]bool)
	for _, index := range existIndexArr {
		// skip the default "_id" index for the database
		if index.Name == "_id_" {
			continue
		}
		existIdxMap[index.Name] = true
	}

	// the number of indexes is not as expected.
	if len(existIdxMap) != 0 && (len(existIdxMap) < len(indexes)) {
		blog.Errorf("the number of indexes is not as expected, existId: %+v, indexes: %v", existIdxMap, indexes)
		return errors.New("the number of indexes is not as expected")
	}

	for _, index := range indexes {
		if _, exist := existIdxMap[index.Name]; exist {
			continue
		}
		err = db.Table("cc_ContainerBase").CreateIndex(ctx, index)
		if err != nil && !db.IsDuplicatedError(err) {
			blog.Errorf("create index for container table failed, err: %v, index: %+v", err, index)
			return err
		}
	}
	return nil
}

var podIndexes = []types.Index{
	{
		Name: common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
		Keys: map[string]int32{
			common.BKFieldID: 1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix + "bk_reference_id_reference_kind_name",
		Keys: map[string]int32{
			"bk_reference_id":  1,
			"reference_kind":   1,
			common.BKFieldName: 1,
		},
		Background: true,
		Unique:     true,
	},
	{
		Name: common.CCLogicUniqueIdxNamePrefix +
			"bk_biz_id_bk_supplier_account_cluster_uid_namespace_reference_kind_reference_name_name",
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
		Name: common.CCLogicIndexNamePrefix + "bk_biz_id_bk_supplier_account_reference_name_reference_kind",
		Keys: map[string]int32{
			common.BKAppIDField:   1,
			common.BKOwnerIDField: 1,
			"reference_name":      1,
			"reference_kind":      1,
		},
		Background: true,
	},
	{
		Name: common.CCLogicIndexNamePrefix + "bk_reference_id_reference_kind",
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

func addPodTableIndexes(ctx context.Context, db dal.RDB) error {

	existIndexArr, err := db.Table("cc_PodBase").Indexes(ctx)
	if err != nil {
		blog.Errorf("get exist index for pod table failed, err: %v", err)
		return err
	}

	existIdxMap := make(map[string]bool)
	for _, index := range existIndexArr {
		// skip the default "_id" index for the database
		if index.Name == "_id_" {
			continue
		}
		existIdxMap[index.Name] = true
	}

	// the number of indexes is not as expected.
	if len(existIdxMap) != 0 && (len(existIdxMap) < len(podIndexes)) {
		blog.Errorf("the number of indexes is not as expected, existId: %+v, indexes: %v", existIdxMap, podIndexes)
		return errors.New("the number of indexes is not as expected")
	}

	for _, index := range podIndexes {
		if _, exist := existIdxMap[index.Name]; exist {
			continue
		}
		err = db.Table("cc_PodBase").CreateIndex(ctx, index)
		if err != nil && !db.IsDuplicatedError(err) {
			blog.Errorf("create index for pod table failed, err: %v, index: %+v", err, index)
			return err
		}
	}
	return nil
}

func addWorkLoadTableIndexes(ctx context.Context, db dal.RDB, workLoadKind string) error {
	indexes := []types.Index{
		{
			Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
			Keys:       map[string]int32{common.BKFieldID: 1},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_namespace_id_name",
			Keys: map[string]int32{
				"bk_namespace_id":  1,
				common.BKFieldName: 1,
			},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_biz_id_bk_supplier_account_cluster_uid_namespace_name",
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

	existIndexArr, err := db.Table(workLoadKind).Indexes(ctx)
	if err != nil {
		blog.Errorf("get exist index for %s table failed, err: %v", workLoadKind, err)
		return err
	}

	existIdxMap := make(map[string]bool)
	for _, index := range existIndexArr {
		// skip the default "_id" index for the database
		if index.Name == "_id_" {
			continue
		}
		existIdxMap[index.Name] = true
	}

	// the number of indexes is not as expected.
	if len(existIdxMap) != 0 && (len(existIdxMap) < len(indexes)) {
		blog.Errorf("the number of indexes is not as expected, existId: %+v, indexes: %v", existIdxMap, indexes)
		return errors.New("the number of indexes is not as expected")
	}

	for _, index := range indexes {
		if _, exist := existIdxMap[index.Name]; exist {
			continue
		}
		err = db.Table(workLoadKind).CreateIndex(ctx, index)
		if err != nil && !db.IsDuplicatedError(err) {
			blog.Errorf("create index for %s table failed, err: %v, index: %+v", workLoadKind, err, index)
			return err
		}
	}
	return nil
}

func addNamespaceTableIndexes(ctx context.Context, db dal.RDB) error {
	indexes := []types.Index{
		{
			Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
			Keys:       map[string]int32{common.BKFieldID: 1},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_cluster_id_name",
			Keys: map[string]int32{
				"bk_cluster_id":    1,
				common.BKFieldName: 1,
			},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_biz_id_bk_supplier_account_cluster_uid_name",
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

	existIndexArr, err := db.Table("cc_NamespaceBase").Indexes(ctx)
	if err != nil {
		blog.Errorf("get exist index for namespace table failed, err: %v", err)
		return err
	}

	existIdxMap := make(map[string]bool)
	for _, index := range existIndexArr {
		// skip the default "_id" index for the database
		if index.Name == "_id_" {
			continue
		}
		existIdxMap[index.Name] = true
	}

	// the number of indexes is not as expected.
	if len(existIdxMap) != 0 && (len(existIdxMap) < len(indexes)) {
		blog.Errorf("the number of indexes is not as expected, existId: %+v, indexes: %v", existIdxMap, indexes)
		return errors.New("the number of indexes is not as expected")
	}

	for _, index := range indexes {
		if _, exist := existIdxMap[index.Name]; exist {
			continue
		}
		err = db.Table("cc_NamespaceBase").CreateIndex(ctx, index)
		if err != nil && !db.IsDuplicatedError(err) {
			blog.Errorf("create index for namespace table failed, err: %v, index: %+v", err, index)
			return err
		}
	}
	return nil
}

func addClusterTableIndexes(ctx context.Context, db dal.RDB) error {
	indexes := []types.Index{
		{
			Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
			Keys:       map[string]int32{common.BKFieldID: 1},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_biz_id_bk_supplier_account_uid",
			Keys: map[string]int32{
				common.BKAppIDField:   1,
				common.BKOwnerIDField: 1,
				"uid":                 1,
			},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_biz_id_bk_supplier_account_name",
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

	existIndexArr, err := db.Table("cc_ClusterBase").Indexes(ctx)
	if err != nil {
		blog.Errorf("get exist index for cluster table failed, err: %v", err)
		return err
	}

	existIdxMap := make(map[string]bool)
	for _, index := range existIndexArr {
		// skip the default "_id" index for the database
		if index.Name == "_id_" {
			continue
		}
		existIdxMap[index.Name] = true
	}

	// the number of indexes is not as expected.
	if len(existIdxMap) != 0 && (len(existIdxMap) < len(indexes)) {
		blog.Errorf("the number of indexes is not as expected, existId: %+v, indexes: %v", existIdxMap, indexes)
		return errors.New("the number of indexes is not as expected")
	}

	for _, index := range indexes {
		if _, exist := existIdxMap[index.Name]; exist {
			continue
		}
		err = db.Table("cc_ClusterBase").CreateIndex(ctx, index)
		if err != nil && !db.IsDuplicatedError(err) {
			blog.Errorf("create index for cluster table failed, err: %v, index: %+v", err, index)
			return err
		}
	}
	return nil
}

func addNodeTableIndexes(ctx context.Context, db dal.RDB) error {
	indexes := []types.Index{
		{
			Name:       common.CCLogicUniqueIdxNamePrefix + common.BKFieldID,
			Keys:       map[string]int32{common.BKFieldID: 1},
			Background: true,
			Unique:     true,
		},
		{
			// NOCC:tosa/linelength(忽略长度)
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_biz_id_bk_supplier_account_cluster_uid_name",
			Keys: map[string]int32{
				common.BKAppIDField:   1,
				common.BKOwnerIDField: 1,
				"cluster_uid":         1,
				common.BKFieldName:    1,
			},
			Background: true,
			Unique:     true,
		},
		{
			Name: common.CCLogicUniqueIdxNamePrefix + "bk_cluster_id_id",
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

	existIndexArr, err := db.Table("cc_NodeBase").Indexes(ctx)
	if err != nil {
		blog.Errorf("get exist index for node table failed, err: %v", err)
		return err
	}

	existIdxMap := make(map[string]bool)
	for _, index := range existIndexArr {
		// skip the default "_id" index for the database
		if index.Name == "_id_" {
			continue
		}
		existIdxMap[index.Name] = true
	}

	// the number of indexes is not as expected.
	if len(existIdxMap) != 0 && (len(existIdxMap) < len(indexes)) {
		blog.Errorf("the number of indexes is not as expected, existId: %+v, indexes: %v", existIdxMap, indexes)
		return errors.New("the number of indexes is not as expected")
	}

	for _, index := range indexes {
		if _, exist := existIdxMap[index.Name]; exist {
			continue
		}
		err = db.Table("cc_NodeBase").CreateIndex(ctx, index)
		if err != nil && !db.IsDuplicatedError(err) {
			blog.Errorf("create index for node table failed, err: %v, index: %+v", err, index)
			return err
		}
	}
	return nil
}
