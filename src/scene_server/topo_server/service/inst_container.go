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

package service

import (
	"strconv"

	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/http/rest"
	"configcenter/src/kube/types"
)

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

func isContainerObject(object string) bool {
	switch object {
	case KubeCluster, KubeNode, KubeNamespace, KubeWorkload, KubePod, KubeContainer:
		return true
	default:
		return false
	}
}

// ContainerAttrsRsp 容器资源属性回应
type ContainerAttrsRsp struct {
	Field    string `json:"field"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

// CreateContainerCluster 创建容器集群
func (s *Service) CreateContainerCluster(ctx *rest.Contexts) {
	data := new(types.ClusterBaseFields)
	if err := ctx.DecodeInto(data); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if err := data.ValidateCreate(); err != nil {
		blog.Errorf("validate create container cluster failed, err: %v, rid: %s", err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}

	bizID, err := strconv.ParseInt(ctx.Request.PathParameter("bk_biz_id"), 10, 64)
	if err != nil {
		blog.Errorf("failed to parse the biz id, err: %v, rid: %s", err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}

	var cluster *types.Cluster
	txnErr := s.Engine.CoreAPI.CoreService().Txn().AutoRunTxn(ctx.Kit.Ctx, ctx.Kit.Header, func() error {
		var err error
		cluster, err = s.Logics.ContainerOperation().CreateCluster(ctx.Kit, data, bizID, ctx.Request.PathParameter("bk_supplier_account"))
		if err != nil {
			blog.Errorf("create business set failed, err: %v, rid: %s", err, ctx.Kit.Rid)
			return err
		}
		return nil
	})

	if txnErr != nil {
		ctx.RespAutoError(txnErr)
		return
	}

	ctx.RespEntity(cluster.ID)
}

// FindContainerAttrs 获取容器对象的属性信息
func (s *Service) FindContainerAttrs(ctx *rest.Contexts) {

	object := ctx.Request.PathParameter("object")
	if !isContainerObject(object) {
		blog.Errorf("the parameter is invalid and does not belong to the container object(%s)", object)
		ctx.RespAutoError(ctx.Kit.CCError.Errorf(common.CCErrCommParamsInvalid, "object"))
		return
	}
	result := make([]ContainerAttrsRsp, 0)
	switch object {
	case KubeCluster:
		for _, descriptor := range types.ClusterSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeNamespace:
		for _, descriptor := range types.NamespaceSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeNode:
		for _, descriptor := range types.NodeSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeWorkload:
		for _, descriptor := range types.WorkLoadSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubePod:
		for _, descriptor := range types.PodSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeContainer:
		for _, descriptor := range types.ContainerSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	}
	ctx.RespEntity(result)
}
