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
	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/http/rest"
	"configcenter/src/common/metadata"
	"configcenter/src/storage/dal/mongo/local"
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
	data := new(local.ClusterOption)
	if err := ctx.DecodeInto(data); err != nil {
		ctx.RespAutoError(err)
		return
	}

	fields, errRaw := data.ValidateCreate()
	if errRaw.ErrCode != 0 {
		blog.Errorf("validate create business set failed, err: %v, rid: %s", errRaw, ctx.Kit.Rid)
		ctx.RespAutoError(errRaw.ToCCError(ctx.Kit.CCError))
		return
	}
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
		for _, descriptor := range local.ClusterSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeNamespace:
		for _, descriptor := range local.NamespaceSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeNode:
		for _, descriptor := range local.NodeSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeWorkload:
		for _, descriptor := range local.WorkLoadSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubePod:
		for _, descriptor := range local.PodSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	case KubeContainer:
		for _, descriptor := range local.ContainerSpecFieldsDescriptor {
			result = append(result, ContainerAttrsRsp{
				Field:    descriptor.Field,
				Type:     string(descriptor.Type),
				Required: descriptor.Required,
			})
		}
	}
	ctx.RespEntity(result)
}
