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

package auditlog

import (
	"configcenter/src/apimachinery/coreservice"
	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/mapstr"
	"configcenter/src/common/metadata"
	"configcenter/src/common/util"
)

// instanceAuditLog provides methods to generate and save instance audit log.
type containerAuditLog struct {
	audit
}

// GenerateAuditLog generate audit log of instance.
func (i *containerAuditLog) GenerateAuditLog(parameter *generateAuditCommonParameter, objID string, data []mapstr.MapStr) (
	[]metadata.AuditLog, error) {
	return i.generateAuditLog(parameter, objID, data)
}

// 记录审计日志需要做一层转化，结构体传化成mapstr
func (i *containerAuditLog) generateAuditLog(parameter *generateAuditCommonParameter, objID string, data []mapstr.MapStr) (
	[]metadata.AuditLog, error) {
	auditLogs := make([]metadata.AuditLog, len(data))
	kit := parameter.kit

	for index, inst := range data {
		// 对于容器资源自增ID 都是 "id"
		id, err := util.GetInt64ByInterface(inst[common.BKFieldID])
		if err != nil {
			blog.Errorf("failed to get container resource id, err: %v, inst: %s, rid: %s", err, inst, kit.Rid)
			return nil, kit.CCError.CCErrorf(common.CCErrCommInstFieldConvertFail, objID, common.BKFieldID, "int", err.Error())
		}

		var bizID int64
		if _, exist := inst[common.BKAppIDField]; exist {
			bizID, err = util.GetInt64ByInterface(inst[common.BKAppIDField])
			if err != nil {
				blog.Errorf("failed to get biz id, err: %v, inst: %s, rid: %s", err, inst, kit.Rid)
				return nil, kit.CCError.CCErrorf(common.CCErrCommInstFieldConvertFail, objID, common.BKAppIDField, "int", err.Error())
			}
		}

		action := parameter.action
		updateFields := parameter.updateFields

		var details *metadata.BasicContent
		switch action {
		case metadata.AuditCreate:
			details = &metadata.BasicContent{
				CurData: inst,
			}
		case metadata.AuditDelete:
			details = &metadata.BasicContent{
				PreData: inst,
			}
		case metadata.AuditUpdate:
			if updateFields[common.BKDataStatusField] != inst[common.BKDataStatusField] {
				switch updateFields[common.BKDataStatusField] {
				case string(common.DataStatusDisabled):
					action = metadata.AuditArchive
				case string(common.DataStatusEnable):
					action = metadata.AuditRecover
				}
			}

			details = &metadata.BasicContent{
				PreData:      inst,
				UpdateFields: updateFields,
			}
		}

		auditLog := metadata.AuditLog{
			AuditType:    "container",
			ResourceType: "container",
			Action:       action,
			BusinessID:   bizID,
			ResourceID:   id,
			OperateFrom:  parameter.operateFrom,
			ResourceName: objID,
			OperationDetail: &metadata.ContainerOpDetail{
				BasicOpDetail: metadata.BasicOpDetail{
					Details: details,
				},
				Object: objID,
			},
		}
		auditLogs[index] = auditLog
	}

	return auditLogs, nil
}

func NewContainerAudit(clientSet coreservice.CoreServiceClientInterface) *containerAuditLog {
	return &containerAuditLog{
		audit: audit{
			clientSet: clientSet,
		},
	}
}
