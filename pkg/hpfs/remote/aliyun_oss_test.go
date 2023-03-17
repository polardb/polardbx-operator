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
