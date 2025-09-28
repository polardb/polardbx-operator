package polardbxclusters

import (
	"polardbx-ui-backend/pkg/api/domain/polardbxclusters/services"

	"github.com/gin-gonic/gin"
)

// --- Thin handlers using services ---

func ListBackups(c *gin.Context)     { services.NewBackupService().List(c) }
func CreateBackup(c *gin.Context)    { services.NewBackupService().Create(c) }
func GetBackupAdvice(c *gin.Context) { services.NewBackupService().GetBackupAdvice(c) }

// via services
func ValidateBackup(c *gin.Context)     { services.NewBackupService().Validate(c) }
func StreamBackupEvents(c *gin.Context) { services.NewBackupService().StreamEvents(c) }
func GetBackupMetrics(c *gin.Context)   { services.NewBackupService().GetMetrics(c) }
func GetBinlogMetrics(c *gin.Context)   { services.NewBackupService().GetBinlogMetrics(c) }

func DeleteBackup(c *gin.Context) { services.NewBackupService().Delete(c) }

// Force delete backup (remove finalizers then delete)
func ForceDeleteBackup(c *gin.Context) { services.NewBackupService().ForceDelete(c) }

// Schedules via services
func ListSchedules(c *gin.Context)  { services.NewBackupScheduleService().List(c) }
func CreateSchedule(c *gin.Context) { services.NewBackupScheduleService().Create(c) }
func GetSchedule(c *gin.Context)    { services.NewBackupScheduleService().Get(c) }
func UpdateSchedule(c *gin.Context) { services.NewBackupScheduleService().Update(c) }
func DeleteSchedule(c *gin.Context) { services.NewBackupScheduleService().Delete(c) }

// Next-run aggregation
func GetScheduleNextRuns(c *gin.Context) { services.NewBackupScheduleService().GetNextRuns(c) }

// Overview
func GetBackupOverview(c *gin.Context)     { services.NewBackupService().GetOverview(c) }
func GetClusterBackupState(c *gin.Context) { services.NewBackupService().GetClusterState(c) }

// HPFS
func ListHpfsSinks(c *gin.Context)    { services.NewBackupService().ListHpfsSinks(c) }
func ValidateHpfsSink(c *gin.Context) { services.NewBackupService().ValidateHpfsSink(c) }
