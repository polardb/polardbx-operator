package xstores

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes 统一注册“存储/引擎域”的入口（薄壳，转发到既有 handlers）。
func RegisterRoutes(v1 *gin.RouterGroup) {
	g := v1.Group("/xstores")

	// XStore CRUD & Pods
	g.GET("", List)
	g.POST("", Create)
	item := g.Group("/:namespace/:name")
	item.GET("", Get)
	item.PUT("", Update)
	item.DELETE("", Delete)
	item.GET("/pods", ListPods)

	// XStoreBackups (domain alias)
	g.GET("/backups", ListBackups)
	g.POST("/backups", CreateBackup)
	g.GET("/backups/:namespace/:name", GetBackup)
	g.PUT("/backups/:namespace/:name", UpdateBackup)
	g.DELETE("/backups/:namespace/:name", DeleteBackup)
	g.POST("/backups/:namespace/:name/force-delete", ForceDeleteBackup)
	g.GET("/backups/:namespace/:name/remote-info", GetBackupRemoteInfo)

	// XStoreBackupBinlogs (domain alias for standard edition binlog backup)
	g.GET("/backup-binlogs", ListBackupBinlogs)
	g.POST("/backup-binlogs", CreateBackupBinlog)
	g.GET("/backup-binlogs/:namespace/:name", GetBackupBinlog)
	g.PUT("/backup-binlogs/:namespace/:name", UpdateBackupBinlog)
	g.DELETE("/backup-binlogs/:namespace/:name", DeleteBackupBinlog)

	// XStoreFollower (domain alias)
	g.GET("/followers", ListFollowers)
	g.POST("/followers", CreateFollower)
	g.GET("/followers/:namespace/:name", GetFollower)
	g.PUT("/followers/:namespace/:name", UpdateFollower)
	g.DELETE("/followers/:namespace/:name", DeleteFollower)

	// Rebuild wrappers aligned with docs terminology
	item.POST("/rebuild/logger", RebuildLogger)
	item.POST("/rebuild/learner", RebuildLearner)
	item.POST("/rebuild/auto", AutoRebuild)
	item.GET("/rebuild/status", RebuildStatus)
	item.GET("/rebuild/wait", RebuildWait)
	item.GET("/rebuild/progress", RebuildProgress)
	item.DELETE("/rebuild/cancel", RebuildCancel)

	// XStoreFollower operations
	g.POST("/followers/:namespace/:name/retry", RetryFollower)
	g.DELETE("/followers/:namespace/:name/cancel", CancelFollower)
}
