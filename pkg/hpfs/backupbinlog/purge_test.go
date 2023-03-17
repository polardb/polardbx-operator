package backupbinlog

import (
	"fmt"
	"testing"
)

func TestFindMysqlSock(t *testing.T) {
	logDir := "/Users/busu/tmp/mysqldata/log"
	rootDirs := []string{"/Users/busu/", "/Users/dingfeng"}
	mysqlSockPath, _ := FindMysqlSockByLogDir(logDir, rootDirs)
	fmt.Println(mysqlSockPath)
}
