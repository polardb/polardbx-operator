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

package group

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	dbutil "github.com/alibaba/polardbx-operator/pkg/util/database"
)

var (
	ErrAlreadyInRebalance = errors.New("already in rebalance")
)

type Group struct {
	ID         int    `json:"id"`
	StorageId  string `json:"storage_id"`
	DB         string `json:"db"`
	Group      string `json:"group"`
	PhysicalDB string `json:"physical_db"`
	Movable    bool   `json:"movable"`
}

type DDLStatus struct {
	JobId     string    `json:"job_id,omitempty"`     // JOB_ID
	Schema    string    `json:"schema,omitempty"`     // OBJECT_SCHEMA
	Object    string    `json:"object,omitempty"`     // OBJECT_NAME
	Type      string    `json:"type,omitempty"`       // DDL_TYPE
	State     string    `json:"state,omitempty"`      // STATE
	Progress  int       `json:"progress,omitempty"`   // PROGRESS
	StartTime time.Time `json:"start_time,omitempty"` // START_TIME
}

type DDLResult struct {
	JobId   string `json:"job_id,omitempty"`  // JOB_ID
	Schema  string `json:"schema,omitempty"`  // SCHEMA_NAME
	Object  string `json:"object,omitempty"`  // OBJECT_NAME
	Type    string `json:"type,omitempty"`    // DDL_TYPE
	Result  string `json:"result,omitempty"`  // RESULT_TYPE
	Content string `json:"content,omitempty"` // RESULT_CONTENT
}

type DDLPlanStatus struct {
	PlanId   string `json:"plan_id,omitempty"`  // PLAN_ID
	State    string `json:"state,omitempty"`    // State
	Progress int    `json:"progress,omitempty"` // Progress
}

// SlaveStatus describes slave status for xstore follower/logger
type SlaveStatus struct {
	SlaveSQLRunning string `json:"slave_sql_running,omitempty"` // Slave_SQL_Running
	LastError       string `json:"last_error,omitempty"`        // Last_Error
}

// ClusterStatus describes status of xstore cluster
type ClusterStatus struct {
	ServerId       int    `json:"server_id,omitempty"`       // SERVER_ID
	IPPort         string `json:"ip_port,omitempty"`         // IP_PORT
	MatchIndex     int64  `json:"match_index,omitempty"`     // MATCH_INDEX
	NextIndex      int64  `json:"next_index,omitempty"`      // NEXT_INDEX
	Role           string `json:"role,omitempty"`            // ROLE
	HasVoted       string `json:"has_voted,omitempty"`       // HAS_VOTED
	ForceSync      string `json:"force_sync,omitempty"`      // FORCE_SYNC
	ElectionWeight int64  `json:"election_weight,omitempty"` // ELECTION_WEIGHT
	LearnerSource  int64  `json:"learner_source,omitempty"`  // LEARNER_SOURCE
	AppliedIndex   int64  `json:"applied_index,omitempty"`   // APPLIED_INDEX
	Pipelining     string `json:"pipelining,omitempty"`      // PIPELINING
	SendApplied    string `json:"send_applied,omitempty"`    // SEND_APPLIED
}

func (s *DDLPlanStatus) IsSuccess() bool {
	return strings.ToUpper(s.State) == "SUCCESS"
}

type GroupManager interface {
	ListSchemas() ([]string, error)
	CreateSchema(schema string, createTables ...string) error
	CreateTable(schema string, createTables string) error
	ListAllGroups() (map[string][]Group, error)
	ListGroups(schema string) ([]Group, error)
	CountStorages() (int, error)
	GetGroupsOn(schema, storageId string) ([]Group, error)
	PreloadSchema(schema string, addrs ...string) error
	RebalanceCluster(storageExpected int) (string, error)
	DrainStorageNodes(storageNodes ...string) (string, error)
	GetClusterVersion() (string, error)
	ShowDDL(jobId string) (*DDLStatus, error)
	ShowDDLResult(jobId string) (*DDLResult, error)
	ShowDDLPlanStatus(planId string) (*DDLPlanStatus, error)
	SetGlobalVariables(map[string]string) error
	Close() error
	GetBinlogOffset() (string, error)
	GetTrans(column string, table string) (map[string]bool, error)
	IsTransCommited(column string, table string) error
	SendHeartBeat(sname string) error
	ListFileStorage() ([]polardbx.FileStorageInfo, error)
	CreateFileStorage(info polardbx.FileStorageInfo, config config.Config) error
	DropFileStorage(fileStorageName string) error
	ShowSlaveStatus() (*SlaveStatus, error)
	ShowClusterStatus() ([]*ClusterStatus, error)
}

type groupManager struct {
	ctx context.Context

	dataSource dbutil.MySQLDataSource
	db         *sql.DB

	caseInsensitive bool
}

func (m *groupManager) CreateTable(schema string, createTable string) error {
	conn, err := m.getConn(schema)
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(conn)

	_, err = conn.ExecContext(m.ctx, createTable)
	return err

}

func (m *groupManager) SendHeartBeat(sname string) error {
	conn, err := m.getConn("")
	if err != nil {
		return err
	}
	defer conn.Close()

	retryLimits := 10
	retry := 0
	for retry < retryLimits {
		tx, err := conn.BeginTx(m.ctx, &sql.TxOptions{})
		now := time.Now()
		_, err = tx.ExecContext(m.ctx, fmt.Sprintf("replace into `__cdc__`.`__cdc_heartbeat__`(id, sname, gmt_modified) values(%d, %s, '%s')", now.Unix(), sname, now.Format("2006-01-02 15:04:05")))
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
			break
		}
		retry++
	}
	return err
}

//goland:noinspection SqlDialectInspection,SqlNoDataSourceInspection
func (m *groupManager) ShowDDLPlanStatus(planId string) (*DDLPlanStatus, error) {
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	row := conn.QueryRowContext(m.ctx, "SELECT plan_id, state, progress FROM information_schema.ddl_plan WHERE plan_id = ?", planId)
	var s DDLPlanStatus
	if err := row.Scan(&s.PlanId, &s.State, &s.Progress); err != nil {
		return nil, err
	}
	return &s, nil
}

func (m *groupManager) CountStorages() (int, error) {
	conn, err := m.getConn("")
	if err != nil {
		return 0, err
	}
	defer dbutil.DeferClose(conn)

	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	return m.countStorages(conn, ctx)
}

func (m *groupManager) ShowDDL(jobId string) (*DDLStatus, error) {
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	rs, err := conn.QueryContext(m.ctx, fmt.Sprintf("SHOW DDL %s", jobId))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	if !rs.Next() {
		return nil, nil
	}

	status := &DDLStatus{}
	var progress sql.NullString
	dest := map[string]interface{}{
		"JOB_ID":        &status.JobId,
		"OBJECT_SCHEMA": &status.Schema,
		"OBJECT_NAME":   &status.Object,
		"DDL_TYPE":      &status.Type,
		"STATE":         &status.State,
		"PROGRESS":      &progress,
	}
	err = dbutil.Scan(rs, dest, dbutil.ScanOpt{CaseInsensitive: true})
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(progress.String, "%") {
		s := progress.String
		progressVal, _ := strconv.Atoi(s[:len(s)-1])
		status.Progress = progressVal
	}

	return status, nil
}

func (m *groupManager) ShowDDLResult(jobId string) (*DDLResult, error) {
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	rs, err := conn.QueryContext(m.ctx, fmt.Sprintf("SHOW DDL RESULT %s", jobId))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	if !rs.Next() {
		return nil, nil
	}

	result := &DDLResult{}
	dest := map[string]interface{}{
		"JOB_ID":         &result.JobId,
		"SCHEMA_NAME":    &result.Schema,
		"OBJECT_NAME":    &result.Object,
		"DDL_TYPE":       &result.Type,
		"RESULT_TYPE":    &result.Result,
		"RESULT_CONTENT": &result.Content,
	}
	err = dbutil.Scan(rs, dest, dbutil.ScanOpt{CaseInsensitive: true})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *groupManager) getDB() (*sql.DB, error) {
	if m.db == nil {
		db, err := dbutil.OpenMySQLDB(&m.dataSource)
		if err != nil {
			return nil, err
		}
		m.db = db
	}
	return m.db, nil
}

func (m *groupManager) getConn(schema string) (*sql.Conn, error) {
	db, err := m.getDB()
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(m.ctx)
	if err != nil {
		return nil, err
	}

	if len(schema) > 0 {
		_, err = conn.ExecContext(m.ctx, fmt.Sprintf("USE %s", schema))
		if err != nil {
			defer dbutil.DeferClose(conn)
			return nil, err
		}
	}

	return conn, nil
}

func (m *groupManager) preloadSchema(schema string, addr string) error {
	ds := m.dataSource
	ds.Addr = addr

	db, err := dbutil.OpenMySQLDB(&ds)
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(db)

	//goland:noinspection SqlNoDataSourceInspection
	if _, err = db.Exec(fmt.Sprintf("USE %s", schema)); err != nil {
		return err
	}
	_, err = db.Exec("SHOW TABLES")
	return err
}

func (m *groupManager) PreloadSchema(schema string, addrs ...string) error {
	if len(addrs) == 0 {
		return nil
	}

	// Case-insensitive for database name
	if m.caseInsensitive {
		schema = strings.ToLower(schema)
	}

	if len(addrs) == 1 {
		return m.preloadSchema(schema, addrs[0])
	}

	errs := make([]error, len(addrs))
	errCnt := int32(0)
	wg := &sync.WaitGroup{}
	for i := range addrs {
		wg.Add(1)

		idx, addr := i, addrs[i]
		go func() {
			errs[idx] = m.preloadSchema(schema, addr)
			if errs[idx] != nil {
				atomic.AddInt32(&errCnt, 1)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	if errCnt > 0 {
		errStrs := make([]string, 0, errCnt)
		for i, err := range errs {
			if err != nil {
				errStrs = append(errStrs, "on "+addrs[i]+": "+err.Error())
			}
		}
		return fmt.Errorf("preload schema failed: \n  %s", strings.Join(errStrs, "\n  "))
	}

	return nil
}

func (m *groupManager) CreateSchema(schema string, createTables ...string) error {
	db, err := m.getDB()
	if err != nil {
		return err
	}

	// Case-insensitive for database name
	if m.caseInsensitive {
		schema = strings.ToLower(schema)
	}

	_, err = db.ExecContext(m.ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", schema))
	if err != nil {
		return err
	}

	if len(createTables) > 0 {
		conn, err := m.getConn(schema)
		if err != nil {
			return err
		}
		defer dbutil.DeferClose(conn)

		for _, ddl := range createTables {
			if _, err := conn.ExecContext(m.ctx, ddl); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *groupManager) ListSchemas() ([]string, error) {
	db, err := m.getDB()
	if err != nil {
		return nil, err
	}

	rs, err := db.QueryContext(m.ctx, fmt.Sprintf("SHOW DATABASES"))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	schemas := make([]string, 0, 4)
	var schema string
	for rs.Next() {
		err = rs.Scan(&schema)
		if err != nil {
			return nil, err
		}

		// Case insensitive for database name
		if m.caseInsensitive {
			schema = strings.ToLower(schema)
		}

		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func IsTempGroup(group string) bool {
	// Group's format is "XXXX_XXXX_000000_GROUP"
	// and temp group is something like "XXXX_XXXX_S00000_GROUP"
	tempRune := group[len(group)-12]
	return tempRune != '0'
}

func (m *groupManager) ListAllGroups() (map[string][]Group, error) {
	conn, err := m.getConn("polardbx")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	rs, err := conn.QueryContext(m.ctx, fmt.Sprintf("SHOW DS"))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	groups := make(map[string][]Group)
	var id, movable int
	var storageId, db, group, phyDB string
	for rs.Next() {
		err = rs.Scan(&id, &storageId, &db, &group, &phyDB, &movable)
		if err != nil {
			return nil, err
		}

		// Case insensitive for database name
		if m.caseInsensitive {
			db = strings.ToLower(db)
		}

		// Ignore temporary groups.
		if IsTempGroup(group) {
			continue
		}

		if _, ok := groups[db]; !ok {
			groups[db] = make([]Group, 0, 1)
		}
		groups[db] = append(groups[db], Group{
			ID:         id,
			StorageId:  storageId,
			DB:         db,
			Group:      group,
			PhysicalDB: phyDB,
			Movable:    movable > 0,
		})
	}

	return groups, nil
}

func (m *groupManager) ListGroups(schema string) ([]Group, error) {
	groups, err := m.ListAllGroups()
	if err != nil {
		return nil, err
	}
	if r, ok := groups[schema]; ok {
		return r, nil
	}
	return make([]Group, 0), nil
}

func (m *groupManager) GetGroupsOn(schema, storageId string) ([]Group, error) {
	groups, err := m.ListGroups(schema)
	if err != nil {
		return nil, err
	}

	filteredGroups := make([]Group, 0, 8)
	for i := range groups {
		grp := &groups[i]
		if grp.StorageId == storageId {
			filteredGroups = append(filteredGroups, *grp)
		}
	}

	return filteredGroups, nil
}

//goland:noinspection SqlNoDataSourceInspection,SqlDialectInspection
func (m *groupManager) countStorages(conn *sql.Conn, ctx context.Context) (int, error) {
	rs, err := conn.QueryContext(ctx, "SHOW STORAGE")
	if err != nil {
		return 0, err
	}
	defer dbutil.DeferClose(rs)

	cnt := 0
	var storageInstId, leaderNode, isHealthy, instKind, dbCount, groupCnt sql.NullString
	dest := map[string]interface{}{
		"STORAGE_INST_ID": &storageInstId,
		"LEADER_NODE":     &leaderNode,
		"IS_HEALTHY":      &isHealthy,
		"INST_KIND":       &instKind,
		"DB_COUNT":        &dbCount,
		"GROUP_COUNT":     &groupCnt,
	}
	for rs.Next() {
		err := dbutil.Scan(rs, dest, dbutil.ScanOpt{
			CaseInsensitive: true,
		})
		if err != nil {
			return 0, err
		}
		if instKind.String == "MASTER" {
			cnt++
		}
	}

	return cnt, nil
}

func (m *groupManager) convertRebalanceError(err error) error {
	if merr, ok := err.(*mysql.MySQLError); ok {
		if strings.Contains(merr.Message, "already in rebalance") {
			return ErrAlreadyInRebalance
		}
	}
	return err
}

//goland:noinspection SqlNoDataSourceInspection,SqlDialectInspection
func (m *groupManager) RebalanceCluster(storageExpected int) (string, error) {
	conn, err := m.getConn("")
	if err != nil {
		return "", err
	}
	defer dbutil.DeferClose(conn)

	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	storageCnt, err := m.countStorages(conn, ctx)
	if err != nil {
		return "", err
	}
	if storageCnt < storageExpected {
		return "", errors.New("storage size not match")
	}

	row := conn.QueryRowContext(ctx, `schedule rebalance cluster`)
	var planId string
	if err := row.Scan(&planId); err != nil {
		return "", m.convertRebalanceError(err)
	}
	return planId, nil
}

//goland:noinspection SqlNoDataSourceInspection
func (m *groupManager) DrainStorageNodes(storageNodes ...string) (string, error) {
	conn, err := m.getConn("")
	if err != nil {
		return "", err
	}
	defer dbutil.DeferClose(conn)

	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	row := conn.QueryRowContext(ctx, fmt.Sprintf(`schedule rebalance cluster drain_node='%s'`,
		strings.Join(storageNodes, ",")))
	var planId string
	if err := row.Scan(&planId); err != nil {
		return "", m.convertRebalanceError(err)
	}
	return planId, nil
}

func (m *groupManager) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *groupManager) ListFileStorage() ([]polardbx.FileStorageInfo, error) {
	var fileStorageInfoList []polardbx.FileStorageInfo
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	// Select all file storage info
	selectStmt := `SELECT engine, external_endpoint, file_uri, access_key_id, access_key_secret FROM metadb.file_storage_info`

	rs, err := conn.QueryContext(m.ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	var engine, endpoint, fileUri, accessKeyId, accessKeySecret string

	for rs.Next() {
		err = rs.Scan(&engine, &endpoint, &fileUri, &accessKeyId, &accessKeySecret)
		if err != nil {
			return nil, err
		}
		fileStorageInfoList = append(fileStorageInfoList, polardbx.FileStorageInfo{
			Engine: engine,
			//endpoint,
			//fileUri,
			//accessKeyId,
			//accessKeySecret,
		})
	}

	return fileStorageInfoList, nil
}

func (m *groupManager) CreateFileStorage(info polardbx.FileStorageInfo, config config.Config) error {
	conn, err := m.getConn("")
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(conn)

	stmt := ""
	fileUri := ""
	endpoint := ""
	accessKeyId := ""
	accessKeySecret := ""
	switch info.GetEngineType() {
	case polardbx.EngineTypeOss:
		ossConfig := config.Oss()
		endpoint = ossConfig.Endpoint()
		fileUri = fmt.Sprintf("oss://%s", ossConfig.Bucket())
		accessKeyId = ossConfig.AccessKey()
		accessKeySecret = ossConfig.AccessSecret()
		stmt = fmt.Sprintf("create filestorage oss with ('file_uri' = '%s', 'endpoint'='%s', 'access_key_id'='%s', 'access_key_secret'='%s')", fileUri, endpoint, accessKeyId, accessKeySecret)
	case polardbx.EngineTypeLocalDisk:
		fileUri = "file:///home/admin/drds-server/cold-data"
		stmt = fmt.Sprintf("create filestorage local_disk with ('file_uri' = '%s')", fileUri)
	default:
		return errors.New(fmt.Sprintf("Unsupported file storage type: %s", info.Engine))
	}

	// Execute the statements
	if _, err = conn.ExecContext(m.ctx, stmt); err != nil {
		return errors.New("unable to create file storage " + info.Engine + ": " + err.Error())
	}
	return nil
}

func (m *groupManager) DropFileStorage(fileStorageName string) error {
	conn, err := m.getConn("")
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(conn)
	// Generate drop file storage statement
	stmt := fmt.Sprintf("drop filestorage %s", fileStorageName)

	// Execute the statements
	if _, err = conn.ExecContext(m.ctx, stmt); err != nil {
		return errors.New("unable to drop file storage " + fileStorageName + ": " + err.Error())
	}
	return nil
}

func (m *groupManager) getCnVersion() (string, error) {
	db, err := m.getDB()
	if err != nil {
		return "", err
	}

	ver := ""
	rs, err := db.Query(fmt.Sprint("select version()"))
	if err != nil {
		return "", err
	}
	defer dbutil.DeferClose(rs)

	if !rs.Next() {
		return "", errors.New("no rows returned")
	}
	err = rs.Scan(&ver)
	if err != nil {
		return "", nil
	}

	return ver, nil
}

func (m *groupManager) getDnVersion() (string, error) {
	db, err := m.getDB()
	if err != nil {
		return "", err
	}

	ver := ""
	rs, err := db.Query(fmt.Sprint("select @@version"))
	if err != nil {
		return "", err
	}
	defer dbutil.DeferClose(rs)

	if !rs.Next() {
		return "", errors.New("no rows returned")
	}
	err = rs.Scan(&ver)
	if err != nil {
		return "", nil
	}

	return ver, nil
}

func (m *groupManager) GetClusterVersion() (string, error) {
	cnVer, err := m.getCnVersion()
	if err != nil {
		return "", err
	}
	dnVer, err := m.getDnVersion()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%s", cnVer, dnVer), nil
}

func (m *groupManager) GetBinlogOffset() (string, error) {
	db, err := m.getDB()
	if err != nil {
		return "", err
	}
	rs, err := db.Query(fmt.Sprint("show master status"))
	if err != nil {
		return "", err
	}
	defer dbutil.DeferClose(rs)

	if !rs.Next() {
		return "", errors.New("no rows returned")
	}
	var file_name, file_size, binlog_do_db, binlog_ignore_db, executed_gtid_set string

	err = rs.Scan(&file_name, &file_size, &binlog_do_db, &binlog_ignore_db, &executed_gtid_set)
	if err != nil {
		return "", nil
	}
	ans := file_name + ":" + file_size
	return ans, nil
}

func (m *groupManager) GetTrans(columnName string, tableName string) (map[string]bool, error) {
	conn, err := m.getConn("information_schema")
	if err != nil {
		return nil, err
	}
	rs, err := conn.QueryContext(m.ctx, fmt.Sprintf("SELECT %s FROM %s", columnName, tableName))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	var transId string
	transIdSet := make(map[string]bool)
	for rs.Next() {
		err = rs.Scan(&transId)
		transIdSet[transId] = true
	}
	if err != nil {
		return nil, err
	}
	return transIdSet, nil
}

func (m *groupManager) IsTransCommited(columnName string, tableName string) error {
	transIdSet, err := m.GetTrans(columnName, tableName)
	if transIdSet == nil || len(transIdSet) == 0 {
		return nil
	}
	conn, err := m.getConn("information_schema")
	if err != nil {
		return nil
	}
	for {
		rs, err := conn.QueryContext(m.ctx, fmt.Sprintf("SELECT %s FROM %s", columnName, tableName))
		if err != nil {
			return err
		}
		var transId string
		for rs.Next() {
			err = rs.Scan(&transId)
			if transIdSet[transId] {
				break
			}
		}
		err = rs.Close()
		if err != nil {
			return err
		}
		if !rs.Next() {
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (m *groupManager) SetGlobalVariables(variables map[string]string) error {
	db, err := m.getDB()
	if err != nil {
		return err
	}

	for k, v := range variables {
		_, err = db.ExecContext(m.ctx, fmt.Sprintf("SET GLOBAL %s = %s", k, v))
		if err != nil {
			return err
		}
	}

	return nil
}

// ShowSlaveStatus aims to check slave status, which should only be used for follower/logger of xstore
func (m *groupManager) ShowSlaveStatus() (*SlaveStatus, error) {
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	rs, err := conn.QueryContext(m.ctx, fmt.Sprintf("SHOW SLAVE STATUS"))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	if !rs.Next() {
		return nil, nil
	}

	status := &SlaveStatus{}
	dest := map[string]interface{}{
		"Slave_SQL_Running": &status.SlaveSQLRunning,
		"Last_Error":        &status.LastError,
	}
	err = dbutil.Scan(rs, dest, dbutil.ScanOpt{CaseInsensitive: true})
	if err != nil {
		return nil, err
	}
	return status, nil
}

// ShowClusterStatus aims to check global status of xstore cluster
func (m *groupManager) ShowClusterStatus() ([]*ClusterStatus, error) {
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	rs, err := conn.QueryContext(m.ctx, fmt.Sprintf("SELECT * FROM INFORMATION_SCHEMA.ALISQL_CLUSTER_GLOBAL"))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	var statusList []*ClusterStatus
	for rs.Next() {
		status := &ClusterStatus{}
		dest := map[string]interface{}{
			"SERVER_ID":       &status.ServerId,
			"IP_PORT":         &status.IPPort,
			"MATCH_INDEX":     &status.MatchIndex,
			"NEXT_INDEX":      &status.NextIndex,
			"ROLE":            &status.Role,
			"HAS_VOTED":       &status.HasVoted,
			"FORCE_SYNC":      &status.ForceSync,
			"ELECTION_WEIGHT": &status.ElectionWeight,
			"LEARNER_SOURCE":  &status.LearnerSource,
			"APPLIED_INDEX":   &status.AppliedIndex,
			"PIPELINING":      &status.Pipelining,
			"SEND_APPLIED":    &status.SendApplied,
		}
		err = dbutil.Scan(rs, dest, dbutil.ScanOpt{CaseInsensitive: true})
		if err != nil {
			return nil, err
		}
		statusList = append(statusList, status)
	}
	return statusList, nil
}

func NewGroupManagerWithDB(ctx context.Context, db *sql.DB, caseInsensitive bool) GroupManager {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		panic("db must be non-nil")
	}
	return &groupManager{
		ctx:             ctx,
		db:              db,
		caseInsensitive: caseInsensitive,
	}
}

func NewGroupManager(ctx context.Context, ds dbutil.MySQLDataSource, caseInsensitive bool) GroupManager {
	if ctx == nil {
		ctx = context.Background()
	}

	if runtime.GOOS == "darwin" {
		ds.Host = "127.0.0.1"
		ds.Port = 3406
	}

	return &groupManager{
		ctx:             ctx,
		dataSource:      ds,
		caseInsensitive: caseInsensitive,
	}
}
