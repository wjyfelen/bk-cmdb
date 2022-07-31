/*
 * Tencent is pleased to support the open source community by making 蓝鲸 available.,
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the ",License",); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an ",AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package container

import (
	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/errors"
	"configcenter/src/common/http/rest"
	"configcenter/src/kube/types"
	"configcenter/src/source_controller/coreservice/core"
	"configcenter/src/storage/driver/mongodb"
	"time"
)

type containerOperation struct {
}

// New create a new model manager instance
func New() core.ContainerOperation {
	container := &containerOperation{}
	return container
}

// CreateCluster 创建容器集群数据
func (p *containerOperation) CreateCluster(kit *rest.Kit, bizID int64, option *types.ClusterBaseFields) (*types.Cluster,
	errors.CCErrorCoder) {

	// 这里需要再判断一下是否有重复的的数据,防止后续有直接调用coreservice而缺少校验
	if err := option.ValidateCreate(); err != nil {
		blog.Errorf("create cluster failed, option invalid, option:%+v, err: %+v, rid: %s", option, err, kit.Rid)
		return nil, kit.CCError.CCError(common.CCErrCommParamsInvalid)
	}
	nameFilter := map[string]interface{}{
		common.BKDBOR: []map[string]interface{}{
			{
				common.BKAppIDField:     bizID,
				common.BKOwnerIDField:   kit.SupplierAccount,
				types.ContainerUIDField: option.Uid,
			},
			{
				common.BKAppIDField:     bizID,
				common.BKOwnerIDField:   kit.SupplierAccount,
				types.ContainerUIDField: option.Name,
			},
		},
	}

	count, err := mongodb.Client().Table(common.BKTableNameSetTemplate).Find(nameFilter).Count(kit.Ctx)
	if err != nil {
		blog.Errorf("query cluster failed, filter: %+v, err: %+v, rid: %s", nameFilter, err, kit.Rid)
		return nil, kit.CCError.CCError(common.CCErrCommDBSelectFailed)
	}
	if count > 0 {
		blog.Errorf("create cluster failed, name or uid duplicated, name: %s, uid: %s, rid: %s", option.Name,
			option.Uid, kit.Rid)
		return nil, kit.CCError.CCErrorf(common.CCErrCommDuplicateItem, "name or uid")
	}

	// generate id field
	id, err := mongodb.Client().NextSequence(kit.Ctx, types.BKTableNameBaseCluster)
	if nil != err {
		blog.Errorf("create cluster failed, generate id failed, err: %+v, rid: %s", err, kit.Rid)
		return nil, kit.CCError.CCErrorf(common.CCErrCommGenerateRecordIDFailed)
	}

	now := time.Now().Unix()

	cluster := &types.Cluster{
		ID:              int64(id),
		BizID:           bizID,
		SupplierAccount: kit.SupplierAccount,
		ClusterFields:   *option,
		Revision: types.Revision{
			CreateTime: now,
			LastTime:   now,
			Creator:    kit.User,
			Modifier:   kit.User,
		},
	}

	if err := mongodb.Client().Table(common.BKTableNameSetTemplate).Insert(kit.Ctx, cluster); err != nil {
		blog.Errorf("create cluster failed, db insert failed, doc: %+v, err: %+v, rid: %s", cluster, err, kit.Rid)
		return nil, kit.CCError.CCError(common.CCErrCommDBInsertFailed)
	}

	return cluster, nil
}
