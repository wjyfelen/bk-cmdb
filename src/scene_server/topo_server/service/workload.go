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
	"errors"
	"strconv"

	"configcenter/src/common"
	"configcenter/src/common/auditlog"
	"configcenter/src/common/blog"
	"configcenter/src/common/http/rest"
	"configcenter/src/common/mapstr"
	"configcenter/src/common/metadata"
	"configcenter/src/common/util"
	"configcenter/src/kube/types"
)

// CreateWorkload create workload
func (s *Service) CreateWorkload(ctx *rest.Contexts) {
	bizIDStr := ctx.Request.PathParameter(common.BKAppIDField)
	bizID, err := strconv.ParseInt(bizIDStr, 10, 64)
	if err != nil {
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, common.BKAppIDField))
		return
	}

	kind := types.WorkloadType(ctx.Request.PathParameter(types.KindField))
	if err := kind.Validate(); err != nil {
		blog.Errorf("workload kind is invalid, kind: %v, err: %v, rid: %s", kind, err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}

	req := types.WlCreateOption{Kind: kind}
	if err := ctx.DecodeInto(&req); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if rawErr := req.Validate(); rawErr.ErrCode != 0 {
		ctx.RespAutoError(rawErr.ToCCError(ctx.Kit.CCError))
		return
	}

	var data *metadata.RspIDs
	txnErr := s.Engine.CoreAPI.CoreService().Txn().AutoRunTxn(ctx.Kit.Ctx, ctx.Kit.Header, func() error {
		data, err = s.Engine.CoreAPI.CoreService().Kube().CreateWorkload(ctx.Kit.Ctx, ctx.Kit.Header, bizID, kind, &req)
		if err != nil {
			blog.Errorf("create workload failed, data: %v, err: %v, rid: %s", req, err, ctx.Kit.Rid)
			return err
		}

		// audit log.
		audit := auditlog.NewKubeAudit(s.Engine.CoreAPI.CoreService())
		auditParam := auditlog.NewGenerateAuditCommonParameter(ctx.Kit, metadata.AuditCreate)
		for idx := range req.Data {
			wlBase := req.Data[idx].GetWorkloadBase()
			wlBase.BizID = bizID
			wlBase.ID = data.IDs[idx]
			wlBase.SupplierAccount = ctx.Kit.SupplierAccount
			req.Data[idx].SetWorkloadBase(wlBase)
		}
		auditLogs, err := audit.GenerateWorkloadAuditLog(auditParam, req.Data, kind)
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

	ctx.RespEntity(data)
}

// UpdateWorkload update workload
func (s *Service) UpdateWorkload(ctx *rest.Contexts) {
	bizIDStr := ctx.Request.PathParameter(common.BKAppIDField)
	bizID, err := strconv.ParseInt(bizIDStr, 10, 64)
	if err != nil {
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, common.BKAppIDField))
		return
	}

	kind := types.WorkloadType(ctx.Request.PathParameter(types.KindField))
	if err := kind.Validate(); err != nil {
		ctx.RespAutoError(err)
		return
	}

	req := types.WlUpdateOption{Kind: kind}
	if err := ctx.DecodeInto(&req); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if rawErr := req.Validate(); rawErr.ErrCode != 0 {
		ctx.RespAutoError(rawErr.ToCCError(ctx.Kit.CCError))
		return
	}

	query := &metadata.QueryCondition{
		Condition: map[string]interface{}{common.BKFieldID: mapstr.MapStr{common.BKDBIN: req.IDs}},
	}
	resp, err := s.Engine.CoreAPI.CoreService().Kube().ListWorkload(ctx.Kit.Ctx, ctx.Kit.Header, query, kind)
	if err != nil {
		blog.Errorf("list workload failed, bizID: %s, data: %v, err: %v, rid: %s", bizID, req, err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}
	if len(resp.Info) == 0 {
		blog.Errorf("no workload founded, bizID: %s, data: %v, rid: %s", bizID, req, ctx.Kit.Rid)
		ctx.RespAutoError(errors.New("no workload founded"))
		return
	}
	for _, workload := range resp.Info {
		ids := make([]int64, 0)
		if workload.GetWorkloadBase().BizID != bizID {
			ids = append(ids, workload.GetWorkloadBase().ID)
		}

		if len(ids) != 0 {
			blog.Errorf("workload does not belong to this business, kind: %v, ids: %v, bizID: %s, rid: %s", kind, ids,
				bizID, ctx.Kit.Rid)
			ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, ids))
			return
		}
	}

	txnErr := s.Engine.CoreAPI.CoreService().Txn().AutoRunTxn(ctx.Kit.Ctx, ctx.Kit.Header, func() error {
		err := s.Engine.CoreAPI.CoreService().Kube().UpdateWorkload(ctx.Kit.Ctx, ctx.Kit.Header, bizID, kind, &req)
		if err != nil {
			blog.Errorf("update workload failed, data: %v, err: %v, rid: %s", req, err, ctx.Kit.Rid)
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
		auditLogs, err := audit.GenerateWorkloadAuditLog(auditParam, resp.Info, kind)
		if err != nil {
			blog.Errorf("generate audit log failed, data: %v, err: %v, rid: %s", resp.Info, err, ctx.Kit.Rid)
			return err
		}
		if err := audit.SaveAuditLog(ctx.Kit, auditLogs...); err != nil {
			blog.Errorf("save audit log failed, data: %v, err: %v, rid: %s", resp.Info, err, ctx.Kit.Rid)
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

// DeleteWorkload delete workload
func (s *Service) DeleteWorkload(ctx *rest.Contexts) {
	bizID, err := strconv.ParseInt(ctx.Request.PathParameter(common.BKAppIDField), 10, 64)
	if err != nil {
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, common.BKAppIDField))
		return
	}

	kind := types.WorkloadType(ctx.Request.PathParameter(types.KindField))
	if err := kind.Validate(); err != nil {
		ctx.RespAutoError(err)
		return
	}

	req := new(types.WlDeleteOption)
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
	resp, err := s.Engine.CoreAPI.CoreService().Kube().ListWorkload(ctx.Kit.Ctx, ctx.Kit.Header, query, kind)
	if err != nil {
		blog.Errorf("list workload failed, bizID: %s, req: %v, err: %v, rid: %s", bizID, req, err, ctx.Kit.Rid)
		ctx.RespAutoError(err)
		return
	}

	if len(resp.Info) == 0 {
		blog.Errorf("no workload founded, bizID: %s, data: %v, rid: %s", bizID, req, ctx.Kit.Rid)
		ctx.RespAutoError(errors.New("no workload founded"))
		return
	}

	ids := make([]int64, 0)
	for _, workload := range resp.Info {
		if workload.GetWorkloadBase().BizID != bizID {
			ids = append(ids, workload.GetWorkloadBase().ID)
		}
	}
	if len(ids) != 0 {
		blog.Errorf("workload does not belong to this business, kind: %v, ids: %v, bizID: %s, rid: %s", kind, ids,
			bizID, ctx.Kit.Rid)
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, common.BKFieldID))
		return
	}

	hasRes, err := s.hasNextLevelResource(ctx.Kit, string(kind), bizID, req.IDs)
	if err != nil {
		ctx.RespAutoError(err)
		return
	}
	if hasRes {
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, common.BKFieldID))
		return
	}

	txnErr := s.Engine.CoreAPI.CoreService().Txn().AutoRunTxn(ctx.Kit.Ctx, ctx.Kit.Header, func() error {
		err := s.Engine.CoreAPI.CoreService().Kube().DeleteWorkload(ctx.Kit.Ctx, ctx.Kit.Header, bizID, kind, req)
		if err != nil {
			blog.Errorf("delete workload failed, data: %v, err: %v, rid: %s", req, err, ctx.Kit.Rid)
			return err
		}

		audit := auditlog.NewKubeAudit(s.Engine.CoreAPI.CoreService())
		auditParam := auditlog.NewGenerateAuditCommonParameter(ctx.Kit, metadata.AuditDelete)
		auditLogs, err := audit.GenerateWorkloadAuditLog(auditParam, resp.Info, kind)
		if err != nil {
			blog.Errorf("generate audit log failed, data: %v, err: %v, rid: %s", resp.Info, err, ctx.Kit.Rid)
			return err
		}
		if err := audit.SaveAuditLog(ctx.Kit, auditLogs...); err != nil {
			blog.Errorf("save audit log failed, data: %v, err: %v, rid: %s", resp.Info, err, ctx.Kit.Rid)
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

// ListWorkload list workload
func (s *Service) ListWorkload(ctx *rest.Contexts) {
	bizIDStr := ctx.Request.PathParameter(common.BKAppIDField)
	bizID, err := strconv.ParseInt(bizIDStr, 10, 64)
	if err != nil {
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, common.BKAppIDField))
		return
	}

	kind := types.WorkloadType(ctx.Request.PathParameter(types.KindField))
	table, err := kind.Table()
	if err != nil {
		ctx.RespAutoError(ctx.Kit.CCError.CCErrorf(common.CCErrCommParamsInvalid, types.KindField))
		return
	}

	req := new(types.WlQueryOption)
	if err := ctx.DecodeInto(req); err != nil {
		ctx.RespAutoError(err)
		return
	}

	if rawErr := req.Validate(kind); rawErr.ErrCode != 0 {
		ctx.RespAutoError(rawErr.ToCCError(ctx.Kit.CCError))
		return
	}

	nsBizIDs, err := s.searchNsBizIDWithBizAsstID(ctx.Kit, bizID)
	if err != nil {
		ctx.RespAutoError(err)
		return
	}

	bizIDs := []int64{bizID}
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
		count, err := s.countWorkload(ctx.Kit, bizID, cond, req.Page, nsBizIDs, kind)
		if err != nil {
			blog.Errorf("count workload failed, table: %s, cond: %v, err: %v, rid: %s", table, cond, err, ctx.Kit.Rid)
			ctx.RespAutoError(err)
			return
		}
		ctx.RespEntityWithCount(count, make([]mapstr.MapStr, 0))
		return
	}

	info, err := s.getWorkloadDetails(ctx.Kit, bizID, cond, req.Page, req.Fields, nsBizIDs, kind)
	if err != nil {
		ctx.RespAutoError(err)
		return
	}
	ctx.RespEntityWithCount(0, info)
}

func (s *Service) countWorkload(kit *rest.Kit, bizID int64, filter mapstr.MapStr,
	page metadata.BasePage, nsBizIDs []int64, kind types.WorkloadType) (int64, error) {

	query := &metadata.QueryCondition{
		Condition:      filter,
		Page:           page,
		Fields:         []string{types.BKBizAsstIDField, types.ClusterTypeField},
		DisableCounter: true,
	}

	result, err := s.Engine.CoreAPI.CoreService().Kube().ListWorkload(kit.Ctx, kit.Header, query, kind)
	if err != nil {
		blog.Errorf("find namespace failed, cond: %+v, err: %v, rid: %s", filter, err, kit.Rid)
		return 0, err
	}
	var count int64
	for _, wl := range result.Info {
		data := wl.GetWorkloadBase()
		if data.BizID != bizID {
			if util.InArray(data.BizAsstID, nsBizIDs) && data.ClusterType == types.ClusterShareTypeField {
				count++
			}
		}
		count++
	}
	return count, nil
}

func (s *Service) getWorkloadDetails(kit *rest.Kit, bizID int64, filter map[string]interface{}, page metadata.BasePage,
	reqFields []string, nsBizIDs []int64, kind types.WorkloadType) ([]types.WorkloadInterface, error) {
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
	result, err := s.Engine.CoreAPI.CoreService().Kube().ListWorkload(kit.Ctx, kit.Header, query, kind)
	if err != nil {
		blog.Errorf("search node failed, filter: %+v, err: %v, rid: %s", filter, err, kit.Rid)
		return nil, err
	}

	wls := make([]types.WorkloadInterface, 0)
	for _, wl := range result.Info {
		base := wl.GetWorkloadBase()
		if !fieldsMap[types.ClusterTypeField] {
			base.ClusterType = ""
		}
		if !fieldsMap[types.BKBizAsstIDField] {
			base.BizAsstID = 0
		}
		if base.BizID != bizID {
			if util.InArray(base.BizAsstID, nsBizIDs) && base.ClusterType == types.ClusterShareTypeField {
				wls = append(wls, wl)

			}
		}
		wls = append(wls, wl)
	}

	return wls, nil
}
