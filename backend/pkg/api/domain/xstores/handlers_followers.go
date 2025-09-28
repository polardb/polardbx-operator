package xstores

import (
	"polardbx-ui-backend/pkg/api/domain/xstores/services"

	"github.com/gin-gonic/gin"
)

func ListFollowers(c *gin.Context)  { services.NewFollowersService().List(c) }
func CreateFollower(c *gin.Context) { services.NewFollowersService().Create(c) }
func GetFollower(c *gin.Context)    { services.NewFollowersService().Get(c) }
func UpdateFollower(c *gin.Context) { services.NewFollowersService().Update(c) }
func DeleteFollower(c *gin.Context) { services.NewFollowersService().Delete(c) }

func RebuildLogger(c *gin.Context)   { services.NewRebuildService().Logger(c) }
func RebuildLearner(c *gin.Context)  { services.NewRebuildService().Learner(c) }
func AutoRebuild(c *gin.Context)     { services.NewRebuildService().Auto(c) }
func RebuildStatus(c *gin.Context)   { services.NewRebuildService().Status(c) }
func RebuildWait(c *gin.Context)     { services.NewRebuildService().Wait(c) }
func RebuildProgress(c *gin.Context) { services.NewRebuildService().Progress(c) }
func RebuildCancel(c *gin.Context)   { services.NewRebuildService().Cancel(c) }

func RetryFollower(c *gin.Context)  { services.NewFollowersService().Retry(c) }
func CancelFollower(c *gin.Context) { services.NewFollowersService().Cancel(c) }
