/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package remote

import (
	"context"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/common"
	"strconv"
	"testing"
	"time"
)

func TestDeleteExpiredFilesOnOss(t *testing.T) {
	auth := map[string]string{}
	params := map[string]string{}

	auth["endpoint"] = "oss-cn-beijing.aliyuncs.com"
	auth["access_key"] = ""
	auth["access_secret"] = ""
	params["bucket"] = "beijing-busu"
	params["deadline"] = strconv.FormatInt(time.Now().Unix(), 10)
	fileService, _ := GetFileService("aliyun-oss")

	expiredFiles := make([]string, 0)
	expiredFilesPtr := &expiredFiles
	ctx := context.WithValue(context.Background(), common.AffectedFiles, expiredFilesPtr)
	ft, _ := fileService.DeleteExpiredFile(ctx, "binlogbackup/default/rebuild-demo/67c43e24-c18e-4821-82bd-996db340bf01/", auth, params)
	ft.Wait()
	val, _ := ctx.Value(common.AffectedFiles).(*[]string)
	fmt.Println(*val)
}

func TestDeleteExpiredFilesOnSftp(t *testing.T) {
	auth := map[string]string{}
	auth["port"] = "22"
	auth["host"] = "11.165.72.152"
	auth["username"] = "root"
	auth["password"] = "ATP@linux2016"
	params := map[string]string{}
	params["deadline"] = strconv.FormatInt(time.Now().Unix(), 10)
	fileService, _ := GetFileService("sftp")
	expiredFiles := make([]string, 0)
	expiredFilesPtr := &expiredFiles
	ctx := context.WithValue(context.Background(), common.AffectedFiles, expiredFilesPtr)
	ft, _ := fileService.DeleteExpiredFile(ctx, "busuhhhh", auth, params)
	ft.Wait()
	fmt.Println(*expiredFilesPtr)
}

func TestDeleteExpiredFilesOnMinio(t *testing.T) {
	auth := map[string]string{}
	auth["endpoint"] = "play.min.io"
	auth["access_key"] = ""
	auth["secret_key"] = ""
	auth["useSSL"] = "true"
	params := map[string]string{}
	params["bucket"] = "yj-test-bucket"
	params["deadline"] = strconv.FormatInt(time.Now().Unix(), 10)
	fileService, _ := GetFileService("s3")
	expiredFiles := make([]string, 0)
	expiredFilesPtr := &expiredFiles
	ctx := context.WithValue(context.Background(), common.AffectedFiles, expiredFilesPtr)
	ft, _ := fileService.DeleteExpiredFile(ctx, "busuhhhh", auth, params)
	ft.Wait()
	fmt.Println(*expiredFilesPtr)
}
