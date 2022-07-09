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

package cmd

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/mapstr"
	"configcenter/src/common/metadata"
	"configcenter/src/common/util"
	"configcenter/src/storage/dal"
	"configcenter/src/storage/dal/mongo"
	"configcenter/src/storage/dal/mongo/local"
	"configcenter/src/tools/cmdb_ctl/app/config"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	clientMaxOpenConns     = 10
	clientMaxIdleOpenConns = 5

	batchNum        = 200
	ccV1mongoURI    = "mongodb://11.177.154.8:27017/cmdb"
	ccV1mongoRsName = "rs0"

	ccV3mongoURI = "mongodb://cmdbP.cloud.bk.db:27000,cmdbS.cloud.bk.db:27000/cmdb"
	//ccV3mongoRsName = "rs0"
	ccV3mongoRsName = "bk-cloud-cmdb"

	ccV1ProcessCollection = "CC_V1Process"
	ccV1ModuleHost        = "CC_V1ModuleHost"

	procBindInfo = "bind_info"
	user         = "cc_admin"
)

var hostIdFailList map[int64]struct{}

func init() {
	rootCmd.AddCommand(NewTransferPortOperationCommand())
	hostIdFailList = make(map[int64]struct{})
}

type transferPortConf struct {
	service *config.Service
}

func NewTransferPortOperationCommand() *cobra.Command {

	conf := new(transferPortConf)
	cmd := &cobra.Command{
		Use:   "proc",
		Short: "db",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	transferCmds := make([]*cobra.Command, 0)
	transferCmd := &cobra.Command{
		Use:   "transfer",
		Short: "transfer port from v1 to v3",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTransferPortCmd(conf)
		},
	}

	transferCmds = append(transferCmds, transferCmd)
	for _, fCmd := range transferCmds {
		cmd.AddCommand(fCmd)
	}

	return cmd
}

type ccV1HostModule struct {
	HostID   int64 `bson:"bk_host_id"`
	ModuleID int64 `bson:"bk_module_id"`
}

type ccV3Relation struct {
	BizID    int64 `json:"bk_biz_id" bson:"bk_biz_id"`
	HostID   int64 `json:"bk_host_id" bson:"bk_host_id"`
	ModuleID int64 `json:"bk_module_id" bson:"bk_module_id"`
}

// hostID 和moduleID的关系必须是合法。可能出现的情况:
// v1存在此hostID v3不存在，迁移的时候需要把这个删除。
// v1的这个hostID 的模块与v3对应的模块ID 不一致。需要把这个hostID删除。
// v1这个hostID对应的业务ID 与v3的业务不一致。需要把这个删除。
// 这个函数返回的是无效的hostIDs。 每个host对应的模块找一个即可，然后将所有的processname放到一个模块下面
func getInvalidHostIDs(hostModuleV1 []ccV1HostModule, relationsV3 []ccV3Relation, hostBiz1 map[int64]int64) ([]int64,
	map[int64]int64) {

	v1Map := make(map[int64][]int64)
	for _, moduleV1result := range hostModuleV1 {
		v1Map[moduleV1result.HostID] = append(v1Map[moduleV1result.HostID], moduleV1result.ModuleID)
	}

	// 校验业务是否一致
	v3MapBiz := make(map[int64]int64)
	// 校验moduleID是否有差异
	v3Map := make(map[int64][]int64)
	for _, moduleV3result := range relationsV3 {
		v3Map[moduleV3result.HostID] = append(v3Map[moduleV3result.HostID], moduleV3result.ModuleID)
		v3MapBiz[moduleV3result.HostID] = moduleV3result.BizID
	}

	hostIDs := make([]int64, 0)

	for hostID, bizv1ID := range hostBiz1 {
		if bizv1ID == 0 || v3MapBiz[hostID] != bizv1ID {
			hostIDs = append(hostIDs, hostID)
			blog.Errorf("*********************  biz invalid bizID1: %d, bizID3: %d, hostID: %d", bizv1ID, v3MapBiz[hostID], hostID)
			delete(v1Map, hostID)
		}
	}

	// 每个hostID的维度，对应的模块需要完全一样
	v1ModuleMap := make(map[int64]int64)
	for h1, m1list := range v1Map {
		for _, m1 := range m1list {
			if exist := util.InArray(m1, v3Map[h1]); !exist {
				// 这里过滤出来的是不合法的hostID
				hostIDs = append(hostIDs, h1)
				blog.Errorf("********************* biz invalid m1: %d, moduleID3: %d, hostID: %d", m1, v3Map[h1], h1)
				delete(v1Map, h1)
			}
			// 只要找出一个模块ID即可，后续可以将进程信息挂到这个模块
			if _, ok := v1ModuleMap[h1]; !ok {
				v1ModuleMap[h1] = m1
			}
		}

	}
	return hostIDs, v1ModuleMap
}

// adjustProcData 对数据进行校验，将不能处理的数据剔除。一个主机对应的process可能是多个,获取全部的主机对应的进程
func adjustProcData(v1Procs []map[string]interface{}) map[int64][]map[string]interface{} {
	// procData: hostID=>v1ProcData
	procData := make(map[int64][]map[string]interface{})
	// 从1.0进程看全量hostID
	for _, v1Proc := range v1Procs {
		// hostID必须得有
		hostID, err := util.GetInt64ByInterface(v1Proc[common.BKHostIDField])
		if err != nil {
			blog.Errorf(" cc1.0 data invalid procData: %v, err: %v", v1Proc, err)
			continue
		}
		// bizID必须得有
		_, err = util.GetInt64ByInterface(v1Proc[common.BKAppIDField])
		if err != nil {
			blog.Errorf(" cc1.0 data invalid procData: %+v, err: %v", v1Proc, err)
			continue
		}

		if v1Proc[common.BKFuncName] == nil {
			blog.Errorf(" cc1.0 data invalid BKProcessNameField hostID: %v", hostID)
			continue
		}

		// 进程名字必须得有，类型得是string
		if _, ok := v1Proc[common.BKFuncName].(string); !ok {
			blog.Errorf(" cc1.0 data invalid BKProcessNameField hostID: %v", hostID)
			continue
		}

		if v1Proc[common.BKProcessNameField] == nil {
			blog.Errorf(" cc1.0 data invalid BKProcessNameField hostID: %v", hostID)
			continue
		}

		// 进程别名必须得有，类型得是string
		if _, ok := v1Proc[common.BKProcessNameField].(string); !ok {
			blog.Errorf(" cc1.0 data invalid BKProcessNameField hostID: %v", hostID)
			continue
		}

		if v1Proc[procBindInfo] == nil {
			continue
		}

		// 去掉bk_host_id字段
		delete(v1Proc, common.BKHostIDField)
		procData[hostID] = append(procData[hostID], v1Proc)
	}

	return procData
}

// removeInvalidData 去掉非法的进程信息数据
func removeInvalidData(hostIDs []int64, procs map[int64][]map[string]interface{}) map[int64][]map[string]interface{} {
	if len(hostIDs) == 0 {
		return procs
	}

	blog.Errorf("********************* the module information corresponding to the host is inconsistent hostID list :%v", hostIDs)
	for _, hostID := range hostIDs {
		delete(procs, hostID)
	}
	return procs
}

// generateServiceInstanceName 生成服务实例名字
func generateServiceInstanceName(s3 dal.RDB, hostID int64, processData *metadata.Process) (string, error) {

	//  1、获取ip 只要找到内网第一ip即可
	filter := mapstr.MapStr{
		common.BKHostIDField: hostID,
	}
	host := make(metadata.HostMapStr)
	fields := []string{common.BKHostInnerIPField}
	err := s3.Table(common.BKTableNameBaseHost).Find(filter).Fields(fields...).One(context.Background(), &host)
	if err != nil {
		blog.Errorf("get host data from mongodb for cache failed, err: %v", err)
		return "", err
	}

	var ok bool
	ips, ok := host[common.BKHostInnerIPField].(string)
	if !ok {
		blog.Errorf("get host: %d data from mongodb for cache, but got invalid ip, host: %v", hostID, host)
		return "", fmt.Errorf("invalid host: %d innerip", hostID)
	}
	ip := strings.Split(ips, ",")

	serviceInstanceName := ip[0]

	// 这里第二个参数用进程名字填充
	if processData.ProcessName != nil && len(*processData.ProcessName) > 0 {
		serviceInstanceName += fmt.Sprintf("_%s", *processData.ProcessName)
	}
	// 第三个参数用bindInfo中的第一个端口填充
	for _, bindInfo := range processData.BindInfo {
		if bindInfo.Std != nil && bindInfo.Std.Port != nil {
			serviceInstanceName += fmt.Sprintf("_%s", *bindInfo.Std.Port)
			break
		}
	}
	return serviceInstanceName, nil
}

// generateServiceInstance 生成服务实例
func generateServiceInstance(s3 dal.RDB, hostID int64, hostModuleMap map[int64]int64, procV1 *metadata.Process) (int64,
	error) {

	// 生成服务实例名字
	name, err := generateServiceInstanceName(s3, hostID, procV1)
	if err != nil {
		return 0, err
	}

	id, err := s3.NextSequence(context.Background(), common.BKTableNameServiceInstance)
	if err != nil {
		return 0, err
	}

	instance := &metadata.ServiceInstance{
		BizID:             procV1.BusinessID,
		ID:                int64(id),
		Name:              name,
		ServiceTemplateID: 0,
		HostID:            hostID,
		ModuleID:          hostModuleMap[hostID],
		CreateTime:        time.Now(),
		LastTime:          time.Now(),
		Creator:           user,
		Modifier:          user,
		SupplierAccount:   "tencent",
	}
	if err := s3.Table(common.BKTableNameServiceInstance).Insert(context.Background(), instance); err != nil {
		blog.Errorf("insert service instance failed hostID: %d, instance: %+v, err: %v", hostID, instance, err)
		return 0, err
	}
	return int64(id), nil
}

// 判断端口port 是否在v1PortList的中
func isPortInV1PortsList(port string, v1PortList string) bool {
	if strings.Contains(v1PortList, "-") {
		ports := strings.Split(v1PortList, "-")
		if len(ports) != 2 {
			return false
		}
		if port <= ports[1] && port >= ports[0] {
			return true
		}
	} else {
		if port == v1PortList {
			return true
		}
	}
	return false
}

// createNewProcess 创建新的进程
func createNewProcess(ctx context.Context, s3 dal.RDB, serviceInstID, hostID int64, proc map[string]interface{}) error {

	if proc[common.BKProcBindInfo] != nil {
		atrr, ok := proc[common.BKProcBindInfo].([]map[string]interface{})
		if !ok {
			blog.Errorf("generate process fail, hostID: %d, type: %+v", hostID, reflect.TypeOf(proc[common.BKProcBindInfo]))
			return fmt.Errorf("bindInfo type error")
		}
		proc[common.BKProcBindInfo] = atrr
	}

	id, err := s3.NextSequence(context.Background(), common.BKTableNameBaseProcess)
	if err != nil {
		blog.Errorf("get sequence failed, serviceInstanceID : %v, hostID: %d, err: %v", serviceInstID, hostID, err)
		return err
	}
	proc[common.BKProcessIDField] = int64(id)
	proc[common.BKServiceInstanceIDField] = serviceInstID
	proc[common.CreateTimeField] = time.Now()
	proc[common.LastTimeField] = time.Now()

	user := user
	proc[common.BKUser] = &user
	if err = s3.Table(common.BKTableNameBaseProcess).Insert(ctx, proc); err != nil {
		blog.Errorf("insert process failed, serviceInstanceID: %d, hostID: %d, err: %v", serviceInstID, hostID, err)
		return err
	}
	bizID, _ := util.GetInt64ByInterface(proc[common.BKAppIDField])
	// build service instance relation
	relation := &metadata.ProcessInstanceRelation{
		ProcessID:         int64(id),
		BizID:             bizID,
		ServiceInstanceID: serviceInstID,
		ProcessTemplateID: 0,
		HostID:            hostID,
		SupplierAccount:   "tencent",
	}
	if err = s3.Table(common.BKTableNameProcessInstanceRelation).Insert(ctx, relation); err != nil {
		blog.Errorf("insert process relation failed, serviceInstanceID: %d, hostID: %s, processID: %d, err: %v",
			serviceInstID, hostID, id, err)
		return err
	}
	blog.Errorf("create process success, hostID: %d, serviceInstID: %d, processID: %d", hostID, serviceInstID, id)
	return nil
}

// updatePortProcess 在原有进程基础上进行更新端口，增加方式是追加,方便起见直接追加即可，不会对业务产生影响
func updatePortProcess(ctx context.Context, s3 dal.RDB, hostID int64, prcV3 map[string]interface{},
	bindInfos []map[string]interface{}) error {

	if prcV3[common.BKProcBindInfo] != nil {
		procTmp, ok := prcV3[common.BKProcBindInfo].(primitive.A)
		if !ok {
			blog.Errorf("generate process fail, hostID: %d, type: %+v", hostID, reflect.TypeOf(procTmp))
			return fmt.Errorf("bindInfo type error")
		}
		atrrs := []interface{}(procTmp)
		for _, attr := range atrrs {
			tmpAttr, ok := attr.(map[string]interface{})
			if !ok {
				continue
			}
			bindInfos = append(bindInfos, tmpAttr)
		}
		prcV3[common.BKProcBindInfo] = bindInfos
	} else {
		// 可能存在的情况是v3中有进程信息但是没有配置端口号
		prcV3[common.BKProcBindInfo] = bindInfos
	}

	filter := map[string]interface{}{
		common.BKProcIDField: prcV3[common.BKProcessIDField],
	}
	prcV3[common.LastTimeField] = time.Now()

	if err := s3.Table(common.BKTableNameBaseProcess).Update(ctx, filter, prcV3); err != nil {
		blog.Errorf("update process failed, procName: %v, procID: %d, serviceInstance ID: %v, err: %v",
			prcV3[common.BKProcessNameField], prcV3[common.BKProcessIDField], prcV3[common.BKServiceInstanceIDField], err)
		return err
	}
	blog.Errorf("========================== update success, hostID: %v, processID: %v", hostID, prcV3[common.BKProcessIDField])
	return nil
}

// 生成进程和进程实例关系表
func generateProcess(ctx context.Context, s3 dal.RDB, serviceInstID, hostID int64, procV1Struct *metadata.Process,
	procV1 map[string]interface{}) error {

	// 判断是否存在同名进程
	filter := map[string]interface{}{
		common.BKDBOR: []map[string]interface{}{{
			common.BKProcessNameField:       *procV1Struct.ProcessName,
			common.BKServiceInstanceIDField: serviceInstID,
		}, {
			common.BKServiceInstanceIDField: serviceInstID,
			common.BKFuncName:               *procV1Struct.FuncName,
			common.BKStartParamRegex:        *procV1Struct.StartParamRegex,
		},
		},
	}
	procV3 := make(map[string]interface{})
	err := s3.Table(common.BKTableNameBaseProcess).Find(filter).One(context.Background(), &procV3)
	if err != nil && !s3.IsNotFoundError(err) {
		blog.Errorf("generate process failed, serviceInstanceID: %d, hostID: %d, err: %v", serviceInstID, hostID, err)
		return err
	}

	if s3.IsNotFoundError(err) {
		// 没有同名的进程那么就创建新的Process
		if err := createNewProcess(ctx, s3, serviceInstID, hostID, procV1); err != nil {
			blog.Errorf("create process failed, hostID: %v, serviceInstID: %v, procV1: %+v, err: %v",
				hostID, serviceInstID, procV1, err)
			return err
		}
		// 成功创建了进程
		blog.Info("create process success, hostID: %d, serviceInstID: %d", hostID, serviceInstID)
		return nil
	}

	// 这里说明有同名进程，那么processID 必须得有
	processID, err := util.GetInt64ByInterface(procV3[common.BKProcessIDField])
	if err != nil {
		blog.Errorf(" cc1.0 data invalid procData, hostID: %d, serviceInstID: %d, name: %s, err: %v", hostID,
			serviceInstID, *procV1Struct.ProcessName, err)
		return err
	}

	if processID <= 0 {
		blog.Errorf("processID is nil, serviceInstID: %d, hostID: %d,process name: %s", serviceInstID, hostID,
			*procV1Struct.ProcessName)
		return fmt.Errorf("processID is nil, serviceInstID: %d, hostID: %d,process name: %s", serviceInstID, hostID,
			*procV1Struct.ProcessName)
	}

	// 存在同名的进程，直接再原进程实例上添加端口即可
	bindInfo, ok := procV1[common.BKProcBindInfo].([]map[string]interface{})
	if !ok {
		blog.Errorf("update process failed, hostID: %d, serviceInstanceID: %d, err: %v", hostID, serviceInstID, err)
		return fmt.Errorf("type error: hostID: %d,", hostID)
	}
	if err := updatePortProcess(ctx, s3, hostID, procV3, bindInfo); err != nil {
		blog.Errorf("update process failed, hostID: %d, serviceInstanceID: %d, err: %v", hostID, serviceInstID, err)
		return err
	}

	blog.Infof("update process success, hostID: %d, serviceInstanceID: %d, name: %s", hostID, serviceInstID,
		*procV1Struct.ProcessName)

	return nil
}

// isInvalidHostIdAndIp 判断一下v3中的ip和v1中是否存在
func isInvalidHostIdAndIp(ip string, procs []map[string]interface{}) bool {

	ips := make([]string, 0)
	for _, proc := range procs {
		ipStr, ok := proc[common.BKHostInnerIPField].(string)
		if ok && ipStr != "" {
			innerIps := strings.Split(ipStr, ",")
			ips = append(ips, innerIps...)
		}

		outerIpStr, ok := proc[common.BKHostOuterIPField].(string)
		if ok && outerIpStr != "" {
			outerIps := strings.Split(outerIpStr, ",")
			ips = append(ips, outerIps...)
		}
		// 这个ip的作用就是用来判断一致性的，获取到之后就可以删掉了
		delete(proc, common.BKHostOuterIPField)
		delete(proc, common.BKHostInnerIPField)
	}

	if len(ips) == 0 {
		return false
	}
	if util.InArray(ip, ips) {
		return true
	}
	return false
}

// removeRedundancyProcBindInfo 去掉不需要迁移的进程 hostIdPortsMap: hostID: ports列表 最终得到结果是hostID对应的一组需要迁移的进程
func removeRedundancyProcBindInfo(hostID int64, hostIdPortsMap map[int64][]string, procs []map[string]interface{},
	hostBizMap map[int64]int64) (
	[]map[string]interface{}, error) {

	result := make([]map[string]interface{}, 0)
	for _, proc := range procs {

		if proc[common.BKProcBindInfo] == nil {
			continue
		}

		procTmp, ok := proc[common.BKProcBindInfo].(primitive.A)
		if !ok {
			blog.Errorf("generate process fail, hostID: %d, type: %+v", hostID, reflect.TypeOf(procTmp))
			continue
		}

		bindInfos := []interface{}(procTmp)
		bindInfoResults := make([]map[string]interface{}, 0)
		// indexMap 这个map是为了确定是否需要将进程信息同步给3.0，防止重复加入。
		indexMap := make(map[int]struct{})

		for _, port := range hostIdPortsMap[hostID] {
			for index, a := range bindInfos {
				bindInfo, ok := a.(map[string]interface{})
				if !ok {
					blog.Errorf("BKProtocol: %v, BKPort: %v,type: %v", bindInfo[common.BKProtocol],
						bindInfo[common.BKPort], reflect.TypeOf(a))
					continue
				}
				if bindInfo[common.BKProtocol] == nil {
					blog.Errorf("********************* protocol field is null, hostID: %v", hostID)
					continue
				}
				protocol, ok := bindInfo[common.BKProtocol].(string)
				if !ok {
					blog.Errorf("********************* protocol field type is invalid, hostID: %v, type: %v", hostID,
						reflect.TypeOf(bindInfo[common.BKProtocol]))
					continue
				}
				if protocol != "1" {
					blog.Errorf("protocol field is not tcp, hostID: %v", hostID)
					continue
				}
				if bindInfo[common.BKPort] == nil {
					blog.Errorf("********************* port field is null, hostID: %v", hostID)
					continue
				}
				info, ok := bindInfo[common.BKPort].(string)
				if !ok {
					blog.Errorf("********************* port field type is invalid, hostID: %v, type: %v", hostID,
						reflect.TypeOf(bindInfo[common.BKPort]))
					continue
				}
				_, exist := indexMap[index]
				// 符合端口范围并且之前没有加入到最终结果的进程那么需要把这个进程信息放到最终结果中
				if isPortInV1PortsList(port, info) && !exist {
					bindInfoResults = append(bindInfoResults, bindInfo)
					indexMap[index] = struct{}{}
				}
			}
		}

		if len(bindInfoResults) == 0 {
			continue
		}
		// 更新到符合要求的bindInfo
		proc[common.BKProcBindInfo] = bindInfoResults
		proc[common.BKAppIDField] = hostBizMap[hostID]
		delete(proc, common.BKHostIDField)

		result = append(result, proc)
	}
	return result, nil
}

func diffServiceTemplateAndModule(s3 dal.RDB) error {

	fields := []string{common.BKServiceCategoryIDField, common.BKFieldID}
	cond := make(map[string]interface{})

	// 2、模板id与分类的对应 map
	serviceTemplateIDCategory := make(map[int64]int64)
	serviceTemplateIDModule := make(map[int64][]metadata.ModuleInst)
	serviceTemplates := make([]metadata.ServiceTemplate, 0)

	if err := s3.Table(common.BKTableNameServiceTemplate).Find(cond).Fields(fields...).All(context.Background(), &serviceTemplates); nil != err {
		blog.Errorf("ListServiceCategories failed, err: %+v", err)
		return err
	}
	for _, serviceTemplate := range serviceTemplates {
		serviceTemplateIDCategory[serviceTemplate.ID] = serviceTemplate.ServiceCategoryID
	}
	//3、每个 serviceTemplatID serviceCategory 不同于 模板的分类ID 获取到对应模块的name和ID并且打印出来
	for serviceTemplateID := range serviceTemplateIDCategory {
		moduleFilter := map[string]interface{}{
			common.BKServiceTemplateIDField: serviceTemplateID,
		}
		modules := make([]metadata.ModuleInst, 0)
		modulefields := []string{common.BKServiceTemplateIDField, common.BKServiceCategoryIDField, common.BKModuleNameField, common.BKModuleIDField}

		err := s3.Table(common.BKTableNameBaseModule).Find(moduleFilter).Fields(modulefields...).All(context.Background(), &modules)
		if err != nil {
			blog.Errorf("filter: %s, err: %s", moduleFilter, err)
			return err
		}
		serviceTemplateIDModule[serviceTemplateID] = modules
	}

	for id, value := range serviceTemplateIDCategory {
		for _, module := range serviceTemplateIDModule[id] {
			if !reflect.DeepEqual(value, module.ServiceCategoryID) {
				fmt.Printf("************ modulename: %s, moduleID: %d, moduleCategoryID: %d, serviceTemplateCategoryID: %d \n",
					module.ModuleName, module.ModuleID, module.ServiceCategoryID, value)
			}
		}

	}
	return nil
}

// transProcDataV1ToV3Process 将主机的v1接口迁移到v3版本，目的是hostPortsMap 中的端口号在1.0存在的,整体进程信息都要迁移到3.0
func transProcDataV1ToV3Process(s3 dal.RDB, procsV1 map[int64][]map[string]interface{}, hostModuleMap map[int64]int64,
	hostIdIp map[int64]string, hostIdPortsMap map[int64][]string, hostBizMap map[int64]int64) error {

	// 以1.0的进程为维度一个一个处理
	for hostID, procsA := range procsV1 {

		//首先得判断一下hostID 和ip在cc1.0中是否能找到，如果找不到那么直接跳过去
		if !isInvalidHostIdAndIp(hostIdIp[hostID], procsA) {
			blog.Errorf("********************* hostID and ip information of the v1 and v3 version hosts are different,"+
				"hostID: %v, v3Ip: %v", hostID, hostIdIp[hostID])
			hostIdFailList[hostID] = struct{}{}
			continue
		}
		// 这里需要判断一下这个ip对应的进程有具体有哪些需要迁移。如果存在的话那么就重组一下v1版本的process,这里的结果就是需要处理的进程，
		// 后续流程复用之前的逻辑即可
		procs, err := removeRedundancyProcBindInfo(hostID, hostIdPortsMap, procsA, hostBizMap)
		if len(procs) == 0 {
			blog.Errorf("********************* no process added host: %v", hostID)
			hostIdFailList[hostID] = struct{}{}
			continue
		}

		// 判断是否已经存在了服务实例
		filter := map[string]interface{}{
			common.BKHostIDField: hostID,
		}

		fields := []string{"id"}
		instance := new(metadata.ServiceInstance)

		err = s3.Table(common.BKTableNameServiceInstance).Find(filter).Fields(fields...).One(context.Background(), instance)
		if err != nil && !s3.IsNotFoundError(err) {
			blog.Errorf("get service instance failed, hostID: %d, err: %v", hostID, err)
			hostIdFailList[hostID] = struct{}{}
			continue
		}

		if s3.IsNotFoundError(err) {
			processV1 := new(metadata.Process)
			if err := mapstr.DecodeFromMapStr(processV1, procs[0]); err != nil {
				blog.Errorf("decode2Struct failed, hostID: %d, process: %s, err: %v", hostID, procs[0], err)
				hostIdFailList[hostID] = struct{}{}
				continue
			}
			// 不存在，要创建新的服务实例
			serviceInstID, err := generateServiceInstance(s3, hostID, hostModuleMap, processV1)
			if err != nil || serviceInstID == 0 {
				hostIdFailList[hostID] = struct{}{}
				blog.Errorf("********************* create service instance failed, host: %v", hostID)
				continue
			}
			instance.ID = serviceInstID
		}

		for _, procV1 := range procs {
			processV1 := new(metadata.Process)
			if err := mapstr.DecodeFromMapStr(processV1, procV1); err != nil {
				blog.Errorf("decode2Struct failed, hostID: %d, serviceInstanceID: %d, process: %s, err: %v", hostID, instance.ID, procV1, err)
				hostIdFailList[hostID] = struct{}{}
				continue
			}

			// 存在的话不需要创建服务实例，直接在原有服务实例上分情况绑定即可
			if err := generateProcess(context.Background(), s3, instance.ID, hostID, processV1, procV1); err != nil {
				blog.Errorf("generate process failed hostID: %v, serviceInstanceID: %d, process: %+v", hostID, instance.ID, processV1)
				hostIdFailList[hostID] = struct{}{}
				continue
			}
		}
	}
	return nil
}

// 格式是按照ip:id
type hostBase struct {
	// ip可能是内网也可能是外网
	ips []string
	id  int64
}

// 查一下v3中的ip与hostID的对应关系,这里面安全扫的ip可能是内网 也可能是外网ip所以需要全量看下这个对应关系。
func getHostIpIdMap(s3 dal.RDB, hostPortsMap map[string][]string) (map[string]int64, map[int64]string,
	map[int64][]string, error) {

	ips := make([]string, 0)
	for ip := range hostPortsMap {
		ips = append(ips, ip)
	}
	//给的ip不知道是内网ip还是外网ip所以粗暴一点查吧
	cond1 := mapstr.MapStr{
		common.BKDBOR: []map[string]interface{}{
			{
				common.BKHostInnerIPField: map[string]interface{}{common.BKDBIN: ips},
			},
			{
				common.BKHostOuterIPField: map[string]interface{}{common.BKDBIN: ips},
			},
		},
	}

	hosts := make([]metadata.HostMapStr, 0)

	// 这里有一个问题，就是多ip场景可能需要测试一下，先按照这种方式写
	if err := s3.Table(common.BKTableNameBaseHost).Find(cond1).Fields(common.BKHostInnerIPField, common.BKHostOuterIPField,
		common.BKHostIDField).All(context.Background(), &hosts); err != nil {
		return nil, nil, nil, fmt.Errorf("find the result from db failed, %+v", err)
	}

	if len(hosts) == 0 {
		return nil, nil, nil, fmt.Errorf("no hosts founed in v3 host")
	}
	hostBaseInfo := make([]hostBase, 0)
	hostIdIpMap := make(map[int64]string)

	for _, host := range hosts {
		hostID, err := util.GetInt64ByInterface(host[common.BKHostIDField])
		if err != nil {
			blog.Errorf("********************* get hostID failed, host: %+v, err: %v", host, err)
			continue
		}
		ipInners := make([]string, 0)
		ipOuters := make([]string, 0)
		if host[common.BKHostInnerIPField] != nil {
			hostInnerIpStr, _ := host[common.BKHostInnerIPField].(string)
			ipInners = strings.Split(hostInnerIpStr, ",")
		}
		if host[common.BKHostOuterIPField] != nil {
			hostOuterIpStr := host[common.BKHostOuterIPField].(string)
			ipOuters = strings.Split(hostOuterIpStr, ",")
		}

		ipInners = append(ipInners, ipOuters...)
		hostBaseInfo = append(hostBaseInfo, hostBase{
			ips: ipInners,
			id:  hostID,
		})
	}

	hostIdPortsMap := make(map[int64][]string)
	hostIpIDMap := make(map[string]int64)
	for _, ip := range ips {
		for _, hostInfo := range hostBaseInfo {
			if util.InArray(ip, hostInfo.ips) {
				hostIpIDMap[ip] = hostInfo.id
				hostIdIpMap[hostInfo.id] = ip
				hostIdPortsMap[hostInfo.id] = append(hostIdPortsMap[hostInfo.id], hostPortsMap[ip]...)
			}
		}
	}

	return hostIpIDMap, hostIdIpMap, hostIdPortsMap, nil
}

//// 查一下v3中的ip与hostID的对应关系,这里面安全扫的ip可能是内网 也可能是外网ip所以需要全量看下这个对应关系。
//func getHostIpIdMap(s3 dal.RDB, hostPortsMap map[string][]string) (map[string]int64, map[int64]string,
//	map[int64][]string, error) {
//
//	ips := make([]string, 0)
//	for ip := range hostPortsMap {
//		ips = append(ips, ip)
//	}
//	//给的ip不知道是内网ip还是外网ip所以粗暴一点查吧
//	cond1 := mapstr.MapStr{
//		common.BKHostInnerIPField: map[string]interface{}{common.BKDBIN: ips},
//	}
//	cond2 := mapstr.MapStr{
//		common.BKHostOuterIPField: map[string]interface{}{common.BKDBIN: ips},
//	}
//	hosts1 := make([]metadata.HostMapStr, 0)
//
//	// 这里有一个问题，就是多ip场景可能需要测试一下，先按照这种方式写
//	if err := s3.Table(common.BKTableNameBaseHost).Find(cond1).Fields(common.BKHostInnerIPField,
//		common.BKHostIDField).All(context.Background(), &hosts1); err != nil {
//		return nil, nil, nil, fmt.Errorf("find the result from db failed, %+v", err)
//	}
//	hosts2 := make([]metadata.HostMapStr, 0)
//	if err := s3.Table(common.BKTableNameBaseHost).Find(cond2).Fields(common.BKHostOuterIPField,
//		common.BKHostIDField).All(context.Background(), &hosts2); err != nil {
//		return nil, nil, nil, fmt.Errorf("find the result from db failed, %+v", err)
//	}
//
//	hosts := make([]metadata.HostMapStr, 0)
//	hosts = append(hosts, hosts1...)
//	hosts = append(hosts, hosts2...)
//
//	if len(hosts) == 0 {
//		return nil, nil, nil, fmt.Errorf("no hosts founed in v3 host")
//	}
//	hostBaseInfo := make([]hostBase, 0)
//	hostIdIpMap := make(map[int64]string)
//
//	for _, host := range hosts {
//		hostID, err := util.GetInt64ByInterface(host[common.BKHostIDField])
//		if err != nil {
//			blog.Errorf("********************* get hostID failed, host: %+v, err: %v", host, err)
//			continue
//		}
//		ipInners := make([]string, 0)
//		ipOuters := make([]string, 0)
//		if host[common.BKHostInnerIPField] != nil {
//			hostInnerIpStr, _ := host[common.BKHostInnerIPField].(string)
//			ipInners = strings.Split(hostInnerIpStr, ",")
//		}
//		if host[common.BKHostOuterIPField] != nil {
//			hostOuterIpStr := host[common.BKHostOuterIPField].(string)
//			ipOuters = strings.Split(hostOuterIpStr, ",")
//		}
//
//		ipInners = append(ipInners, ipOuters...)
//		hostBaseInfo = append(hostBaseInfo, hostBase{
//			ips: ipInners,
//			id:  hostID,
//		})
//	}
//
//	hostIdPortsMap := make(map[int64][]string)
//	hostIpIDMap := make(map[string]int64)
//	for _, ip := range ips {
//		for _, hostInfo := range hostBaseInfo {
//			if util.InArray(ip, hostInfo.ips) {
//				hostIpIDMap[ip] = hostInfo.id
//				hostIdIpMap[hostInfo.id] = ip
//				hostIdPortsMap[hostInfo.id] = append(hostIdPortsMap[hostInfo.id], hostPortsMap[ip]...)
//			}
//		}
//	}
//
//	return hostIpIDMap, hostIdIpMap, hostIdPortsMap, nil
//}
type ModuleID struct {
	ModuleID int64 `bson:"bk_module_id"`
}

// 最终需要获取的是hostID: moduleID之间的对应关系，其中moduleID不能是空闲机池，hostID:BizID
func getV3HostModuleRelation(s3 dal.RDB, hostIDs []int64) (map[int64]int64, map[int64]int64, error) {

	hostIdCond := mapstr.MapStr{
		common.BKHostIDField: mapstr.MapStr{
			common.BKDBIN: hostIDs,
		},
	}

	hostModuleV3 := make([]ccV3Relation, 0)
	fields := []string{common.BKHostIDField, common.BKModuleIDField, common.BKAppIDField}
	if err := s3.Table(common.BKTableNameModuleHostConfig).Find(hostIdCond).Fields(fields...).All(context.Background(),
		&hostModuleV3); err != nil {
		return nil, nil, fmt.Errorf("find the result from db failed, %+v", err)
	}

	hostModuleMap := make(map[int64][]int64)
	hostIdBizMap := make(map[int64]int64)

	for _, info := range hostModuleV3 {
		hostModuleMap[info.HostID] = append(hostModuleMap[info.HostID], info.ModuleID)
		hostIdBizMap[info.HostID] = info.BizID
	}
	result := make(map[int64]int64)
	for host, moduleIDs := range hostModuleMap {
		if len(moduleIDs) == 0 {
			blog.Errorf("********************* host no modules host: %v", host)
			continue
		}

		// 得找一个非空闲机池的模块
		modules := make([]ModuleID, 0)
		moduleFilter := map[string]interface{}{
			common.BKDefaultField: 0,
			common.BKModuleIDField: mapstr.MapStr{
				common.BKDBIN: moduleIDs,
			},
		}
		if err := s3.Table(common.BKTableNameBaseModule).Find(moduleFilter).All(context.Background(), &modules); err != nil {
			return nil, nil, err
		}
		if len(modules) == 0 {
			blog.Errorf("********************* host no normal modules host: %v", host)
			continue
		}
		// 找到一个模块即可
		result[host] = modules[0].ModuleID
	}
	return result, hostIdBizMap, nil
}

// transferProcToV3  hostPortsMap: ip:[port1,port2...]

func runTransferPortCmd(conf *transferPortConf) error {
	s3, err := newMongoV3()
	if err != nil {
		fmt.Printf("connect v3 mongo db fail ,err: %v\n", err)
		return err
	}

	defer func() {
		defer s3.Close()
	}()
	if err := diffServiceTemplateAndModule(s3); err != nil {
		fmt.Printf("fail ,err: %v\n", err)
		return err
	}
	return nil
}

func newMongoV3() (dal.RDB, error) {

	mgoConfig := mongo.Config{
		Address:       "cmdbP.cloud.bk.db:27000,cmdbS.cloud.bk.db:27000",
		User:          "cmdb",
		Password:      "Du4E549KPQbz3pF5",
		Port:          "27000",
		Database:      "cmdb",
		Mechanism:     "SCRAM-SHA-1",
		MaxOpenConns:  10,
		MaxIdleConns:  5,
		RsName:        "bk-cloud-cmdb",
		SocketTimeout: 10,
	}

	db, dbErr := local.NewMgo(mgoConfig.GetMongoConf(), 1*time.Minute)
	if dbErr != nil {
		blog.Errorf("failed to connect the mongo server, error info is %s", dbErr.Error())
		return nil, dbErr
	}

	//db := mongodb.Client()
	return db, nil
}
