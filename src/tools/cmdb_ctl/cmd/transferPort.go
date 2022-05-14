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
	//hostIDs string
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
	//transferCmd.Flags().StringVar(&conf.hostIDs, "hostIDs", "", "hostIDs like 1,2,3,4")

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
func adjustProcData(v1Procs []map[string]interface{}) (map[int64][]map[string]interface{}, map[int64]int64) {
	// procData: hostID=>v1ProcData
	procData := make(map[int64][]map[string]interface{})
	hostBizV1 := make(map[int64]map[int64]struct{})

	// 从1.0进程看全量hostID
	initHostID := make([]int64, 0)
	for _, v1Proc := range v1Procs {

		// hostID必须得有
		hostID, err := util.GetInt64ByInterface(v1Proc[common.BKHostIDField])
		if err != nil {
			blog.Errorf(" cc1.0 data invalid procData: %v, err: %v", v1Proc, err)
			continue
		}
		initHostID = append(initHostID, hostID)
		// bizID必须得有
		bizID, err := util.GetInt64ByInterface(v1Proc[common.BKAppIDField])
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
		if hostBizV1[hostID] == nil {
			hostBizV1[hostID] = make(map[int64]struct{})
		}
		hostBizV1[hostID][bizID] = struct{}{}
	}

	for id, data := range hostBizV1 {
		if len(data) > 1 {
			delete(hostBizV1, id)
			delete(procData, id)
			blog.Errorf("********************* there are multiple business IDs in the host process. hostID: %d", id)
		}
	}

	// 目前看合法的hostID
	resultHostId := make([]int64, 0)

	result := make(map[int64]int64)
	for id, bizID := range hostBizV1 {
		for bId := range bizID {
			result[id] = bId
			resultHostId = append(resultHostId, id)
		}
	}

	// 这里记录下总体的不符合要求的hostID
	for _, iid := range initHostID {
		if !util.InArray(iid, resultHostId) {
			hostIdFailList[iid] = struct{}{}
		}
	}

	return procData, result
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

// createNewProcess 创建新的进程
func createNewProcess(ctx context.Context, s3 dal.RDB, serviceInstID, hostID int64, proc map[string]interface{}) error {

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

	return nil
}

// updatePortProcess 在原有进程基础上进行更新端口，增加方式是追加,方便起见直接追加即可，不会对业务产生影响
func updatePortProcess(ctx context.Context, s3 dal.RDB, hostID int64, prcV3 map[string]interface{},
	bindInfos []interface{}) error {

	if prcV3[common.BKProcBindInfo] != nil {
		procTmp, ok := prcV3[common.BKProcBindInfo].(primitive.A)
		if !ok {
			blog.Errorf("generate process fail, hostID: %d, type: %+v", hostID, reflect.TypeOf(procTmp))
			return fmt.Errorf("bindInfo type error")
		}
		atrr := []interface{}(procTmp)

		atrr = append(atrr, bindInfos...)
		prcV3[common.BKProcBindInfo] = atrr
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
	procTmp, ok := procV1[common.BKProcBindInfo].(primitive.A)
	if !ok {
		blog.Errorf("generate process fail hostID: %d, serviceInstID: %d, type: %+v", hostID, serviceInstID, reflect.TypeOf(procTmp))
		return fmt.Errorf("bindInfo type error")
	}
	bindInfo := []interface{}(procTmp)

	if err := updatePortProcess(ctx, s3, hostID, procV3, bindInfo); err != nil {
		blog.Errorf("update process failed, hostID: %d, serviceInstanceID: %d, err: %v", hostID, serviceInstID, err)
		return err
	}

	blog.Infof("update process success, hostID: %d, serviceInstanceID: %d, name: %s", hostID, serviceInstID,
		*procV1Struct.ProcessName)

	return nil
}

// transProcDataV1ToV3Process 将主机的v1接口迁移到v3版本
func transProcDataV1ToV3Process(s3 dal.RDB, procsV1Map map[int64][]map[string]interface{},
	hostModuleMap map[int64]int64) error {

	for hostID, procs := range procsV1Map {

		instance := new(metadata.ServiceInstance)

		// 判断是否已经存在了服务实例
		filter := map[string]interface{}{
			common.BKHostIDField: hostID,
		}

		fields := []string{"id"}
		err := s3.Table(common.BKTableNameServiceInstance).Find(filter).Fields(fields...).One(context.Background(), instance)
		if err != nil && !s3.IsNotFoundError(err) {
			blog.Errorf("get service instance failed, hostID: %d, err: %v", hostID, err)
			continue
		}

		if s3.IsNotFoundError(err) {
			processV1 := new(metadata.Process)
			if err := mapstr.DecodeFromMapStr(processV1, procs[0]); err != nil {
				blog.Errorf("decode2Struct failed, hostID: %d, process: %s, err: %v", hostID, procs[0], err)
				hostIdFailList[hostID] = struct{}{}
				return err
			}
			// 不存在，要创建新的服务实例
			serviceInstID, err := generateServiceInstance(s3, hostID, hostModuleMap, processV1)
			if err != nil || serviceInstID == 0 {
				hostIdFailList[hostID] = struct{}{}
				blog.Errorf("")
				continue
			}
			instance.ID = serviceInstID
		}

		for _, procV1 := range procs {
			processV1 := new(metadata.Process)
			if err := mapstr.DecodeFromMapStr(processV1, procV1); err != nil {
				blog.Errorf("decode2Struct failed, hostID: %d, serviceInstanceID: %d, process: %s, err: %v", hostID, instance.ID, procV1, err)
				hostIdFailList[hostID] = struct{}{}
				return err
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

func transferProcToV3(s1, s3 dal.RDB, inputHostIDs []int64) error {

	// 1、获取cc1.0的进程数据
	cond := mapstr.MapStr{
		common.BKHostIDField: mapstr.MapStr{
			common.BKDBIN: inputHostIDs,
		},
	}

	v1Procs := make([]map[string]interface{}, 0)
	if err := s1.Table(ccV1ProcessCollection).Find(cond).All(context.Background(), &v1Procs); err != nil {
		blog.Errorf(" get v1 procs failed, inputHostIDs: %v, err: %v", inputHostIDs, err)
		return fmt.Errorf("find the result from db failed, %+v", err)
	}

	if len(v1Procs) == 0 {
		blog.Errorf("failed to get v1 process, inputHostIDs: %v", inputHostIDs)
		return nil
	}

	// 对数据进行简单校验。获取主机和业务的关系和主机维度的进程关系
	procData, hostBizV1 := adjustProcData(v1Procs)
	if len(procData) == 0 {
		blog.Errorf("no valid proc data")
		return fmt.Errorf("no valid data")
	}
	if len(hostIdFailList) > 0 {
		failed := make([]int64, 0)
		for id := range hostIdFailList {
			failed = append(failed, id)
		}
		blog.Errorf("********************* bizID illegal hostIdFailList: %v", failed)
	}
	filterHostID := make([]int64, 0)
	for id := range hostBizV1 {
		filterHostID = append(filterHostID, id)
	}

	hostIdCond := mapstr.MapStr{
		common.BKHostIDField: mapstr.MapStr{
			common.BKDBIN: filterHostID,
		},
	}

	// 2、获取cc1.0的主机和模块的关系
	hostModuleV1 := make([]ccV1HostModule, 0)
	if err := s1.Table(ccV1ModuleHost).Find(hostIdCond).All(context.Background(), &hostModuleV1); err != nil {
		return fmt.Errorf("find the result from db failed, %+v", err)
	}

	// 3、获取cc3.0的主机和模块关系
	hostModuleV3 := make([]ccV3Relation, 0)
	fields := []string{common.BKAppIDField, common.BKHostIDField, common.BKModuleIDField}
	if err := s3.Table(common.BKTableNameModuleHostConfig).Find(hostIdCond).Fields(fields...).All(context.Background(), &hostModuleV3); err != nil {
		return fmt.Errorf("find the result from db failed, %+v", err)
	}

	// 获取无效的hostID列表
	hostIDs, hostModuleMap := getInvalidHostIDs(hostModuleV1, hostModuleV3, hostBizV1)

	for _, hID := range hostIDs {
		hostIdFailList[hID] = struct{}{}
	}
	// procs: 去掉无效数据之后真正需要调整的数据
	procs := removeInvalidData(hostIDs, procData)

	if len(procs) == 0 {
		blog.Errorf("no valid proc data with invalid hostID")
		return fmt.Errorf("no valid proc data with invalid hostID")
	}

	if err := transProcDataV1ToV3Process(s3, procs, hostModuleMap); err != nil {
		blog.Errorf("trans failed err: %v", err)
		return err
	}
	if len(hostIdFailList) > 0 {
		tmpFailed := make([]int64, 0)
		for id := range hostIdFailList {
			tmpFailed = append(tmpFailed, id)
		}
		blog.Errorf("********************* failed to deal hostIDs: %v", tmpFailed)
	}

	if len(hostIdFailList) == 0 {
		blog.Errorf("========================== success to deal hostIDs")
	}
	return nil
}

func runTransferPortCmd(conf *transferPortConf) error {

	s1, err := newMongoV1()
	if err != nil {
		fmt.Printf("connect v1 mongo db fail ,err: %v\n", err)
		return err
	}

	s3, err := newMongoV3()
	if err != nil {
		fmt.Printf("connect v3 mongo db fail ,err: %v\n", err)
		return err
	}
	//
	defer func() {
		defer s1.Close()
		defer s3.Close()
	}()

	hostIDs := make([]int64, 0)

	cond := mapstr.MapStr{}
	hostIDDistinct, err := s1.Table(ccV1ModuleHost).Distinct(context.Background(), "bk_host_id", cond)
	if err != nil {
		return fmt.Errorf("find the result from db failed, %+v", err)
	}

	for _, id := range hostIDDistinct {
		hostID, err := util.GetInt64ByInterface(id)
		if err != nil {
			blog.Errorf(" cc1.0 data invalid, err: %v", err)
			continue
		}
		hostIDs = append(hostIDs, hostID)
	}

	blog.Errorf("hostIDLen: %v", len(hostIDs))

	start := 0

	for {

		if start >= len(hostIDs) {
			break
		}
		//if start >= batchNum {
		//	break
		//}

		hostIDTmps := make([]int64, 0)

		if start+batchNum < len(hostIDs) {
			hostIDTmps = hostIDs[start : start+batchNum]
		} else {
			hostIDTmps = hostIDs[start:]
		}
		blog.Errorf("hostID list: %v", hostIDTmps)
		if err := transferProcToV3(s1, s3, hostIDTmps); err != nil {
			blog.Errorf("transfer proc to v3 failed, err: %v", err)
			return err
		}

		start = start + batchNum

		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

func newMongoV1() (dal.RDB, error) {

	mongoConfig := mongo.Config{
		MaxOpenConns: clientMaxOpenConns,
		MaxIdleConns: clientMaxIdleOpenConns,
		Connect:      ccV1mongoURI,
		RsName:       ccV1mongoRsName,
	}

	db, dbErr := local.NewMgo(mongoConfig.GetMongoConf(), time.Minute)
	if dbErr != nil {
		blog.Errorf("failed to connect the mongo server, error info is %s", dbErr.Error())
		return nil, dbErr
	}
	//err := mongodb.InitClient("", &mongoConfig)
	//if err != nil {
	//	blog.Errorf("init v1 client failed, err: %v", err)
	//	return nil, fmt.Errorf("connect mongo server failed %s", err.Error())
	//}
	//
	//db := mongodb.Client()
	return db, nil
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
	//err := mongodb.InitClient("", &mongoConfig)
	//if err != nil {
	//	blog.Errorf("init v3 client failed, err: %v", err)
	//	return nil, fmt.Errorf("connect mongo server failed %s", err.Error())
	//}

	//db := mongodb.Client()
	return db, nil
}
