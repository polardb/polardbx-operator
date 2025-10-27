package services

import (
	"errors"
	"fmt"

	"polardbx-ui-backend/pkg/api/domain/polardbxclusters/services/runner"
	api_restore "polardbx-ui-backend/pkg/api/restore"
	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
)

// Flow: Precheck → Prepare → Apply → Verify
func RunRestoreFlow(c *gin.Context) {
	rr := runner.Run(c,
		runner.Step{Name: "precheck", Fn: func(c *gin.Context) error {
			// 简单校验：参数存在与 kube 客户端
			if _, ok := util.K8sClientFromContext(c); !ok {
				return errors.New("k8s client missing")
			}
			ns := c.Param("namespace")
			name := c.Param("name")
			if ns == "" || name == "" {
				return fmt.Errorf("invalid path params: namespace=%s name=%s", ns, name)
			}
			return nil
		}},
		runner.Step{Name: "prepare", Fn: func(c *gin.Context) error {
			// 预留更多校验；此处不阻塞
			return nil
		}},
		runner.Step{Name: "apply", Fn: func(c *gin.Context) error {
			api_restore.RestoreCluster(c)
			if c.IsAborted() {
				return errors.New("restore apply aborted")
			}
			return nil
		}},
		runner.Step{Name: "verify", Fn: func(c *gin.Context) error {
			// 预留：根据状态轮询或即时检查一次目标集群创建态
			return nil
		}},
	)
	runner.RespondOK(c, rr)
}
