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
	"time"

	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/metadata"
	"configcenter/src/storage/dal"
	"configcenter/src/storage/dal/mongo"
	"configcenter/src/storage/dal/mongo/local"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(NewChangeCategoryCommand())
}

func NewChangeCategoryCommand() *cobra.Command {

	cmd := &cobra.Command{
		Use:   "changeCategory",
		Short: "change category to default",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChangeCategoryCmd()
		},
	}

	return cmd
}

func changeCategoryToDefault(s3 dal.RDB) error {

	fields := []string{common.BKServiceCategoryIDField, common.BKFieldID}
	cond := make(map[string]interface{})

	// 模板id对应的分类ID map
	serviceTemplateIDCategory := make(map[int64]int64)

	// 模板对应的模块列表
	serviceTemplateIDModules := make(map[int64][]metadata.ModuleInst)
	serviceTemplates := make([]metadata.ServiceTemplate, 0)

	if err := s3.Table(common.BKTableNameServiceTemplate).Find(cond).Fields(fields...).All(context.Background(),
		&serviceTemplates); nil != err {
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
		serviceTemplateIDModules[serviceTemplateID] = modules
	}

	// 这里要看一下是否有一些模块不是 2 而且其他的数据
	serviceTempIDToModuleIDRemoveMap := make(map[int64]struct{})
	idsMap := make(map[int64]struct{})
	for id, value := range serviceTemplateIDCategory {
		for _, module := range serviceTemplateIDModules[id] {
			if !reflect.DeepEqual(value, module.ServiceCategoryID) {
				// 本次刷的数据需要满足的是服务模板是0并且 对应的模块id是2的场景
				if value == 0 && module.ServiceCategoryID == 2 {
					idsMap[id] = struct{}{}
					fmt.Printf("********************* serviceTempalte id: %d, module id: %d.\n", id, module.ModuleID)
				} else {
					serviceTempIDToModuleIDRemoveMap[id] = struct{}{}
					fmt.Printf("##################### serviceTempalte id: %d, module id: %d, serviceCategory id: %d.\n",
						id, module.ModuleID, module.ServiceCategoryID)
				}
			}
		}
	}

	deleteIDs := make([]int64, 0)
	// 这里还得看一下 服务模板下面的模块是否有种类不是2的，如果有这种的话 再把对应的服务模板id给拿出来
	if len(serviceTempIDToModuleIDRemoveMap) > 0 {
		for id := range serviceTempIDToModuleIDRemoveMap {
			deleteIDs = append(deleteIDs, id)
			delete(idsMap, id)
		}
	}
	if len(deleteIDs) > 0 {
		fmt.Printf("##################### handle fail service template ids: %+v.\n", deleteIDs)
	}
	ids := make([]int64, 0)
	for id := range idsMap {
		ids = append(ids, id)
	}

	if err := updateServiceTemplateCategoryID(s3, ids); err != nil {
		fmt.Printf("updateService template category ID, err: %s", err)
		return err
	}
	return nil
}

func updateServiceTemplateCategoryID(s3 dal.RDB, ids []int64) error {

	data := map[string]interface{}{
		common.BKServiceCategoryIDField: 2,
	}

	updateFilter := map[string]interface{}{
		common.BKFieldID: map[string]interface{}{common.BKDBIN: ids},
	}

	if err := s3.Table(common.BKTableNameServiceTemplate).Update(context.Background(), updateFilter, data); nil != err {
		blog.Errorf("update service template category id failed, filter: %+v,  err: %+v", updateFilter, err)
		return err
	}
	return nil
}

func runChangeCategoryCmd() error {
	s3, err := newTestMongo()
	if err != nil {
		fmt.Printf("connect v3 mongo db fail ,err: %v\n", err)
		return err
	}

	defer func() {
		defer s3.Close()
	}()
	if err := changeCategoryToDefault(s3); err != nil {
		fmt.Printf("fail ,err: %v\n", err)
		return err
	}
	return nil
}

func newChangeCategoryMongoV3() (dal.RDB, error) {

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

	return db, nil
}

func newTestMongo() (dal.RDB, error) {
	mgoConfig := mongo.Config{
		Address:       "127.0.0.1:27017",
		User:          "cc",
		Password:      "cc",
		Port:          "27017",
		Database:      "cmdb",
		Mechanism:     "SCRAM-SHA-1",
		MaxOpenConns:  10,
		MaxIdleConns:  5,
		RsName:        "rs0",
		SocketTimeout: 10,
	}

	db, dbErr := local.NewMgo(mgoConfig.GetMongoConf(), 1*time.Minute)
	if dbErr != nil {
		blog.Errorf("failed to connect the mongo server, error info is %s", dbErr.Error())
		return nil, dbErr
	}

	return db, nil
}
