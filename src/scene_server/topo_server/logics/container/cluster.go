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

package container

import (
	"configcenter/src/ac/extensions"
	"configcenter/src/apimachinery"
	"configcenter/src/common"
	"configcenter/src/common/auditlog"
	"configcenter/src/common/blog"
	"configcenter/src/common/http/rest"
	"configcenter/src/common/mapstr"
	"configcenter/src/common/mapstruct"
	"configcenter/src/common/metadata"
	"configcenter/src/kube/types"
)

// ClusterOperationInterface container cluster operation methods
type ClusterOperationInterface interface {

	// CreateCluster create container cluster
	CreateCluster(kit *rest.Kit, data *types.ClusterBaseFields, bizID int64, bkSupplierAccount string) (*types.Cluster, error)

	// SetProxy container cluster proxy
	SetProxy(inst ClusterOperationInterface)
}

// NewClusterOperation create a business instance
func NewClusterOperation(client apimachinery.ClientSetInterface,
	authManager *extensions.AuthManager) ClusterOperationInterface {
	return &cluster{
		clientSet:   client,
		authManager: authManager,
	}
}

type cluster struct {
	clientSet   apimachinery.ClientSetInterface
	authManager *extensions.AuthManager
	cluster     ClusterOperationInterface
}

// SetProxy SetProxy
func (b *cluster) SetProxy(cluster ClusterOperationInterface) {
	b.cluster = cluster
}

// CreateCluster create container  cluster
func (b *cluster) CreateCluster(kit *rest.Kit, data *types.ClusterBaseFields, bizID int64, bkSupplierAccount string) (*types.Cluster, error) {

	cond := mapstr.MapStr{common.BKDBOR: []mapstr.MapStr{
		{
			common.BKFieldName:    *data.Name,
			common.BKAppIDField:   bizID,
			common.BKOwnerIDField: bkSupplierAccount,
		},
		{
			common.BKFieldName:    *data.Uid,
			common.BKAppIDField:   bizID,
			common.BKOwnerIDField: bkSupplierAccount,
		},
	},
	}
	kit.SupplierAccount = bkSupplierAccount
	counts, err := b.clientSet.CoreService().Count().GetCountByFilter(kit.Ctx, kit.Header,
		types.BKTableNameBaseCluster, []map[string]interface{}{cond})
	if err != nil {
		blog.Errorf("count cluster failed, cond: %#v, err: %v, rid: %s", cond, err, kit.Rid)
		return nil, kit.CCError.CCErrorf(common.CCErrTopoInstCreateFailed, "cluster name or uid has been created")
	}
	if counts[0] > 0 {
		blog.Errorf("cluster name or uid has been created, num: %d, err: %v, rid: %s", counts[0], err, kit.Rid)
		return nil, kit.CCError.CCErrorf(common.CCErrTopoInstCreateFailed, "cluster name or uid has been created")
	}
	// 直接调用coreservice进行创建实例，不需要走模型那一套流程
	result, err := b.clientSet.CoreService().Container().CreateCluster(kit.Ctx, kit.Header, bizID, data)
	if err != nil {
		blog.Errorf("create business failed, data: %#v, err: %v, rid: %s", data, err, kit.Rid)
		return nil, err
	}
	// for audit log.
	generateAuditParameter := auditlog.NewGenerateAuditCommonParameter(kit, metadata.AuditCreate)
	audit := auditlog.NewContainerAudit(b.clientSet.CoreService())
	clusterData, cErr := mapstruct.Struct2Map(result.Info)
	if cErr != nil {
		blog.Errorf("convert map failed, generate audit log failed, err: %v, rid: %s", cErr, kit.Rid)
		return nil, err
	}

	auditLog, cErr := audit.GenerateAuditLog(generateAuditParameter, types.KubeCluster, []mapstr.MapStr{clusterData})
	if cErr != nil {
		blog.Errorf("create cluster, generate audit log failed, err: %v, rid: %s", err, kit.Rid)
		return nil, err
	}

	err = audit.SaveAuditLog(kit, auditLog...)
	if err != nil {
		blog.Errorf("create inst, save audit log failed, err: %v, rid: %s", err, kit.Rid)
		return nil, kit.CCError.Error(common.CCErrAuditSaveLogFailed)
	}
	return result.Info, nil
}
