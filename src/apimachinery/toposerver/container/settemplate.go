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
	"configcenter/src/kube/types"
	"context"
	"net/http"

	"configcenter/src/common/blog"
	"configcenter/src/common/errors"
)

func (st *Container) CreateCluster(ctx context.Context, header http.Header, bizID int64,
	option *types.ClusterBaseFields) (*types.CreateClusterResult, errors.CCErrorCoder) {
	ret := new(types.CreateClusterResult)
	subPath := "/create/cluster/{bk_biz_id}/instance"

	err := st.client.Post().
		WithContext(ctx).
		Body(option).
		SubResourcef(subPath, bizID).
		WithHeaders(header).
		Do().
		Into(ret)

	if err != nil {
		blog.Errorf("CreateSetTemplate failed, http request failed, err: %+v", err)
		return nil, errors.CCHttpError
	}
	if ret.CCError() != nil {
		return nil, ret.CCError()
	}

	return ret, nil
}
