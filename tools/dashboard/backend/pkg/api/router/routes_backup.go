package router

import (
	domain_settings "polardbx-dashboard-backend/pkg/api/domain/platform/settings/handler"
	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"

	"github.com/gin-gonic/gin"
)

// RegisterBackupRoutes registers backup-related routes
func RegisterBackupRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterBackupRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterBackupRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Backup and schedule routes",
		Routes: []Route{
			// Backups under cluster
			{Method: "GET", Path: "/clusters/:namespace/:name/backups", Handler: domain_pxc.ListBackups},
			{Method: "POST", Path: "/clusters/:namespace/:name/backups", Handler: domain_pxc.CreateBackup},
			{Method: "GET", Path: "/clusters/:namespace/:name/backup-advice", Handler: domain_pxc.GetBackupAdvice},

			// Root-level backup operations
			{Method: "POST", Path: "/backups/validate", Handler: domain_pxc.ValidateBackup},
			{Method: "GET", Path: "/backups/:namespace/:name/stream", Handler: domain_pxc.StreamBackupEvents},
			{Method: "GET", Path: "/backups/:namespace/:name/metrics", Handler: domain_pxc.GetBackupMetrics},
			{Method: "DELETE", Path: "/backups/:namespace/:name", Handler: domain_pxc.DeleteBackup},
			{Method: "POST", Path: "/backups/:namespace/:name/force-delete", Handler: domain_pxc.ForceDeleteBackup},
			{Method: "GET", Path: "/backups/overview", Handler: domain_pxc.GetBackupOverview},
			{Method: "GET", Path: "/backups/cluster-state", Handler: domain_pxc.GetClusterBackupState},
			{Method: "GET", Path: "/backups/binlog/metrics", Handler: domain_pxc.GetBinlogMetrics},

			// Backup Schedules
			{Method: "GET", Path: "/backup-schedules", Handler: domain_pxc.ListSchedules},
			{Method: "POST", Path: "/backup-schedules", Handler: domain_pxc.CreateSchedule},
			{Method: "GET", Path: "/backup-schedules/:namespace/:name", Handler: domain_pxc.GetSchedule},
			{Method: "PUT", Path: "/backup-schedules/:namespace/:name", Handler: domain_pxc.UpdateSchedule},
			{Method: "DELETE", Path: "/backup-schedules/:namespace/:name", Handler: domain_pxc.DeleteSchedule},

			// Backup Binlogs
			{Method: "GET", Path: "/backup-binlogs", Handler: domain_pxc.ListBackupBinlogs},
			{Method: "POST", Path: "/backup-binlogs", Handler: domain_pxc.CreateBackupBinlog},
			{Method: "GET", Path: "/backup-binlogs/:namespace/:name", Handler: domain_pxc.GetBackupBinlog},
			{Method: "PUT", Path: "/backup-binlogs/:namespace/:name", Handler: domain_pxc.UpdateBackupBinlog},
			{Method: "DELETE", Path: "/backup-binlogs/:namespace/:name", Handler: domain_pxc.DeleteBackupBinlog},

			// HPFS sinks
			{Method: "GET", Path: "/hpfs/sinks", Handler: domain_pxc.ListHpfsSinks},
			{Method: "POST", Path: "/hpfs/sinks/validate", Handler: domain_pxc.ValidateHpfsSink},

			// Settings for backup dashboard
			{Method: "GET", Path: "/settings/backup-dashboard", Handler: domain_settings.Get},
			{Method: "PUT", Path: "/settings/backup-dashboard", Handler: domain_settings.Update},
			{Method: "GET", Path: "/settings", Handler: domain_settings.Get},
			{Method: "PUT", Path: "/settings", Handler: domain_settings.Update},
		},
	})
}
