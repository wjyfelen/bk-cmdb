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
	"configcenter/src/common/mapstr"
	"configcenter/src/common/util"
	"context"
	"errors"
	"fmt"
	"time"

	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/storage/dal"
	"configcenter/src/storage/dal/mongo"
	"configcenter/src/storage/dal/mongo/local"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(FindNoIegBizHostNumCommand())
}

func FindNoIegBizHostNumCommand() *cobra.Command {

	cmd := &cobra.Command{
		Use:   "findNum",
		Short: "查询非IEG业务的主机数量",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFindHostNumCmd()
		},
	}

	return cmd
}

func findBizHostNum(s3 dal.RDB) error {

	// 1、获取非IEG的 业务ID，业务名称 和运维人员

	bizs := make([]map[string]interface{}, 0)
	filter := mapstr.MapStr{
		"bk_operate_dept_id": mapstr.MapStr{
			common.BKDBNE: 3,
		},
		"bk_data_status": mapstr.MapStr{
			common.BKDBNE: "disabled",
		},
	}
	fields := []string{"bk_biz_id", "bk_biz_name", "bk_biz_maintainer"}
	err := s3.Table(common.BKTableNameBaseApp).Find(filter).Fields(fields...).All(context.Background(), &bizs)
	if err != nil {
		fmt.Printf("list business info from db failed, err: %v", err)
		return errors.New("find biz failed")
	}
	fmt.Printf("########################## biz Num: %d", len(bizs))
	// 这个是存的每个业务对应的主机数量
	bizHostNumMap := make(map[int64]int)
	// 2、通过主机关系表获取到主机id并且去重
	for _, biz := range bizs {
		bizID, _ := util.GetInt64ByInterface(biz["bk_biz_id"])
		// 未去重的hostIDs
		hostIDs := make([]map[string]interface{}, 0)
		// 去重之后的hostIDs
		hostIDMap := make(map[int64]struct{})
		op := mapstr.MapStr{
			"bk_biz_id": bizID,
		}
		err := s3.Table(common.BKTableNameModuleHostConfig).Find(op).All(context.Background(), &hostIDs)
		if err != nil {
			fmt.Printf("list business info from db failed, err: %v", err)
			return errors.New("find biz failed")
		}
		for _, host := range hostIDs {
			id, _ := util.GetInt64ByInterface(host["bk_host_id"])
			hostIDMap[id] = struct{}{}
		}
		bizHostNumMap[bizID] = len(hostIDMap)
	}
	// 获取排名前20的业务id
	for _, biz := range bizs {
		bizID, _ := util.GetInt64ByInterface(biz["bk_biz_id"])
		name := util.GetStrByInterface(biz["bk_biz_name"])
		if bizHostNumMap[bizID] > 400 {
			fmt.Printf("biz_id:%d,biz_name:%s,bk_biz_maintainer:%v,hostNum:%d\n", bizID, name, biz["bk_biz_maintainer"], bizHostNumMap[bizID])
		}
	}

	return nil
}

func runFindHostNumCmd() error {
	s3, err := newFindHostNumMongoV3()
	if err != nil {
		fmt.Printf("connect v3 mongo db fail ,err: %v\n", err)
		return err
	}

	defer func() {
		defer s3.Close()
	}()
	if err := findBizHostNum(s3); err != nil {
		fmt.Printf("fail ,err: %v\n", err)
		return err
	}
	return nil
}

func newFindHostNumMongoV3() (dal.RDB, error) {

	mgoConfig := mongo.Config{
		Address:       "cmdbP.cloud.bk.db:27000,cmdbS.cloud.bk.db:27000",
		User:          "cloudcc",
		Password:      "CloudCC3",
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

	return db, nil
}
