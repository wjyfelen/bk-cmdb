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

package service

import (
	"configcenter/src/common"
	"configcenter/src/common/auditlog"
	"configcenter/src/common/blog"
	"configcenter/src/common/http/rest"
	"configcenter/src/common/mapstr"
	"configcenter/src/common/metadata"
	"configcenter/src/common/util"
	"configcenter/src/kube/types"
)

// CreateNamespace create namespace
func (s *Service) CreateNamespace(ctx *rest.Contexts) {

	req := new(types.NsCreateOption)
	if err := ctx.DecodeInto(req); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if rawErr := req.Validate(); rawErr.ErrCode != 0 {
		ctx.RespAutoError(rawErr.ToCCError(ctx.Kit.CCError))
		return
	}

	var ids *metadata.RspIDs
	txnErr := s.Engine.CoreAPI.CoreService().Txn().AutoRunTxn(ctx.Kit.Ctx, ctx.Kit.Header, func() error {
		data, err := s.Engine.CoreAPI.CoreService().Kube().CreateNamespace(ctx.Kit.Ctx, ctx.Kit.Header, req)
		if err != nil {
			blog.Errorf("create namespace failed, data: %v, err: %v, rid: %s", req, err, ctx.Kit.Rid)
			return err
		}
		ids = data
		// audit log.
		audit := auditlog.NewKubeAudit(s.Engine.CoreAPI.CoreService())
		auditParam := auditlog.NewGenerateAuditCommonParameter(ctx.Kit, metadata.AuditCreate)
		for idx := range req.Data {
			req.Data[idx].BizID = req.BizID
			req.Data[idx].ID = data.IDs[idx]
			req.Data[idx].SupplierAccount = ctx.Kit.SupplierAccount
		}
		auditLogs, err := audit.GenerateNamespaceAuditLog(auditParam, req.Data)
		if err != nil {
			blog.Errorf("generate audit log failed, ids: %v, err: %v, rid: %s", data.IDs, err, ctx.Kit.Rid)
			return err
		}
		if err := audit.SaveAuditLog(ctx.Kit, auditLogs...); err != nil {
			blog.Errorf("save audit log failed, ids: %v, err: %v, rid: %s", data.IDs, err, ctx.Kit.Rid)
			return err
		}
		return nil
	})

	if txnErr != nil {
		ctx.RespAutoError(txnErr)
		return
	}

	ctx.RespEntity(ids)
}

// UpdateNamespace update namespace
func (s *Service) UpdateNamespace(ctx *rest.Contexts) {

	req := new(types.NsUpdateOption)
	if err := ctx.DecodeInto(req); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if rawErr := req.Validate(); rawErr.ErrCode != 0 {
		ctx.RespAutoError(rawErr.ToCCError(ctx.Kit.CCError))
		return
	}

	query := &metadata.QueryCondition{
		Condition: mapstr.MapStr{
			common.BKFieldID: mapstr.MapStr{common.BKDBIN: req.IDs},
		},
		DisableCounter: true,
	}
	resp, err := s.Engine.CoreAPI.CoreService().Kube().ListNamespace(ctx.Kit.Ctx, ctx.Kit.Header, query)
	if err != nil {
		blog.Errorf("list namespace failed, =data: %v, err: %v, rid: %s", *req, err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}

	if len(resp.Data) == 0 {
		blog.Errorf("no namespace founded, bizID: %d, query: %+v, rid: %s", req.BizID, query, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}

	for _, namespace := range resp.Data {
		ids := make([]int64, 0)
		if namespace.BizID != req.BizID {
			ids = append(ids, namespace.ID)
		}

		if len(ids) != 0 {
			blog.Errorf("namespace does not belong to this business, ids: %v, bizID: %s, rid: %s", ids, req.BizID,
				ctx.Kit.Rid)
			ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, ids))
			return
		}
	}

	txnErr := s.Engine.CoreAPI.CoreService().Txn().AutoRunTxn(ctx.Kit.Ctx, ctx.Kit.Header, func() error {
		err := s.Engine.CoreAPI.CoreService().Kube().UpdateNamespace(ctx.Kit.Ctx, ctx.Kit.Header, req)
		if err != nil {
			blog.Errorf("update namespace failed, data: %v, err: %v, rid: %s", req, err, ctx.Kit.Rid)
			return err
		}

		audit := auditlog.NewKubeAudit(s.Engine.CoreAPI.CoreService())
		auditParam := auditlog.NewGenerateAuditCommonParameter(ctx.Kit, metadata.AuditUpdate)
		updateFields, goErr := mapstr.Struct2Map(req.Data)
		if goErr != nil {
			blog.Errorf("update fields convert failed, err: %v, rid: %s", goErr, ctx.Kit.Rid)
			return goErr
		}
		auditParam.WithUpdateFields(updateFields)
		auditLogs, err := audit.GenerateNamespaceAuditLog(auditParam, resp.Data)
		if err != nil {
			blog.Errorf("generate audit log failed, data: %v, err: %v, rid: %s", resp.Data, err, ctx.Kit.Rid)
			return err
		}
		if err := audit.SaveAuditLog(ctx.Kit, auditLogs...); err != nil {
			blog.Errorf("save audit log failed, data: %v, err: %v, rid: %s", resp.Data, err, ctx.Kit.Rid)
			return err
		}
		return nil
	})

	if txnErr != nil {
		ctx.RespAutoError(txnErr)
		return
	}

	ctx.RespEntity(nil)
}

// DeleteNamespace delete namespace
func (s *Service) DeleteNamespace(ctx *rest.Contexts) {

	req := new(types.NsDeleteOption)
	if err := ctx.DecodeInto(req); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if rawErr := req.Validate(); rawErr.ErrCode != 0 {
		ctx.RespAutoError(rawErr.ToCCError(ctx.Kit.CCError))
		return
	}

	query := &metadata.QueryCondition{
		Condition: mapstr.MapStr{common.BKFieldID: mapstr.MapStr{common.BKDBIN: req.IDs}},
	}

	resp, err := s.Engine.CoreAPI.CoreService().Kube().ListNamespace(ctx.Kit.Ctx, ctx.Kit.Header, query)
	if err != nil {
		blog.Errorf("list namespace failed, data: %v, err: %v, rid: %s", *req, err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}
	if len(resp.Data) == 0 {
		ctx.RespEntity(nil)
		return
	}

	ids := make([]int64, 0)
	for _, namespace := range resp.Data {
		if namespace.BizID != req.BizID {
			ids = append(ids, namespace.ID)
		}
	}
	if len(ids) != 0 {
		blog.Errorf("namespace does not belong to this business, ids: %v, bizID: %d, rid: %s",
			ids, req.BizID, ctx.Kit.Rid)
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, ids))
		return
	}

	hasRes, e := s.hasNextLevelResource(ctx.Kit, types.KubeNamespace, req.BizID, req.IDs)
	if e != nil {
		ctx.RespAutoError(e)
		return
	}
	if hasRes {
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, common.BKFieldID))
		return
	}

	txnErr := s.Engine.CoreAPI.CoreService().Txn().AutoRunTxn(ctx.Kit.Ctx, ctx.Kit.Header, func() error {
		err := s.Engine.CoreAPI.CoreService().Kube().DeleteNamespace(ctx.Kit.Ctx, ctx.Kit.Header, req)
		if err != nil {
			blog.Errorf("delete namespace failed, data: %v, err: %v, rid: %s", req, err, ctx.Kit.Rid)
			return err
		}

		// audit log.
		audit := auditlog.NewKubeAudit(s.Engine.CoreAPI.CoreService())
		auditParam := auditlog.NewGenerateAuditCommonParameter(ctx.Kit, metadata.AuditDelete)
		auditLogs, err := audit.GenerateNamespaceAuditLog(auditParam, resp.Data)
		if err != nil {
			blog.Errorf("generate audit log failed, data: %v, err: %v, rid: %s", resp.Data, err, ctx.Kit.Rid)
			return err
		}
		if err := audit.SaveAuditLog(ctx.Kit, auditLogs...); err != nil {
			blog.Errorf("save audit log failed, data: %v, err: %v, rid: %s", resp.Data, err, ctx.Kit.Rid)
			return err
		}
		return nil
	})

	if txnErr != nil {
		ctx.RespAutoError(txnErr)
		return
	}

	ctx.RespEntity(nil)
}

// ListNamespace list namespace
func (s *Service) ListNamespace(ctx *rest.Contexts) {

	req := new(types.NsQueryOption)
	if err := ctx.DecodeInto(req); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if rawErr := req.Validate(); rawErr.ErrCode != 0 {
		ctx.RespAutoError(rawErr.ToCCError(ctx.Kit.CCError))
		return
	}

	nsBizIDs, err := s.searchNsBizIDWithBizAsstID(ctx.Kit, req.BizID)
	if err != nil {
		ctx.RespAutoError(err)
		return
	}

	bizIDs := []int64{req.BizID}
	if len(nsBizIDs) != 0 {
		bizIDs = append(bizIDs, nsBizIDs...)
	}

	cond := mapstr.MapStr{
		common.BKAppIDField: mapstr.MapStr{
			common.BKDBIN: bizIDs,
		},
	}

	if req.Filter != nil {
		filterCond, err := req.Filter.ToMgo()
		if err != nil {
			ctx.RespAutoError(err)
			return
		}
		cond = mapstr.MapStr{common.BKDBAND: []mapstr.MapStr{cond, filterCond}}
	}

	if req.Page.EnableCount {
		count, err := s.countNamespace(ctx.Kit, req.BizID, cond, req.Page, nsBizIDs)
		if err != nil {
			ctx.RespAutoError(err)
			return
		}
		ctx.RespEntityWithCount(count, make([]mapstr.MapStr, 0))
		return
	}

	result, err := s.getNamespaceDetails(ctx.Kit, req.BizID, cond, req.Page, req.Fields, nsBizIDs)
	if err != nil {
		blog.Errorf("list namespace failed, bizID: %s, data: %v, err: %v, rid: %s", req.BizID, req, err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}
	ctx.RespEntityWithCount(0, result)
}

func (s *Service) countNamespace(kit *rest.Kit, bizID int64, filter mapstr.MapStr,
	page metadata.BasePage, nsBizIDs []int64) (int64, error) {

	query := &metadata.QueryCondition{
		Condition:      filter,
		Page:           page,
		Fields:         []string{types.BKBizAsstIDField, types.ClusterTypeField},
		DisableCounter: true,
	}

	result, err := s.Engine.CoreAPI.CoreService().Kube().ListNamespace(kit.Ctx, kit.Header, query)
	if err != nil {
		blog.Errorf("find namespace failed, cond: %+v, err: %v, rid: %s", filter, err, kit.Rid)
		return 0, err
	}
	var count int64
	for _, node := range result.Data {
		if node.BizID != bizID {
			if util.InArray(node.BizAsstID, nsBizIDs) && node.ClusterType == types.ClusterShareTypeField {
				count++
			}
		}
		count++
	}
	return count, nil
}

func (s *Service) getNamespaceDetails(kit *rest.Kit, bizID int64, filter map[string]interface{}, page metadata.BasePage,
	reqFields []string, nsBizIDs []int64) ([]types.Namespace, error) {
	// 这里得加一个逻辑看是否前端传了BizAsstID ClusterType
	// 如果没有传那么需要加上，之后再返回给前端的时候需要把这两个数据删掉

	fields, fieldsMap := dealFieldsForShareCluster(reqFields)

	if page.Sort == "" {
		page.Sort = common.BKFieldID
	}

	query := &metadata.QueryCondition{
		Condition:      filter,
		Page:           page,
		Fields:         fields,
		DisableCounter: true,
	}
	result, err := s.Engine.CoreAPI.CoreService().Kube().ListNamespace(kit.Ctx, kit.Header, query)
	if err != nil {
		blog.Errorf("search node failed, filter: %+v, err: %v, rid: %s", filter, err, kit.Rid)
		return nil, err
	}

	namespaces := make([]types.Namespace, 0)
	for _, ns := range result.Data {
		if !fieldsMap[types.ClusterTypeField] {
			ns.ClusterType = ""
		}
		if !fieldsMap[types.BKBizAsstIDField] {
			ns.BizAsstID = 0
		}
		if ns.BizID != bizID {
			if util.InArray(ns.BizAsstID, nsBizIDs) && ns.ClusterType == types.ClusterShareTypeField {
				namespaces = append(namespaces, ns)
			}
		}
		namespaces = append(namespaces, ns)
	}

	return namespaces, nil
}
