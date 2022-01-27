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

type RebalanceAction struct {
	JobId  string `json:"job_id,omitempty"` // JOB_ID
	Schema string `json:"schema,omitempty"` // SCHEMA
	Name   string `json:"name,omitempty"`   // NAME
	Action string `json:"action,omitempty"` // ACTION
}

type DDLStatus struct {
	JobId     string    `json:"job_id,omitempty"`     // JOB_ID
	Schema    string    `json:"schema,omitempty"`     // OBJECT_SCHEMA
	Object    string    `json:"object,omitempty"`     // OBJECT_NAME
	Type      string    `json:"type,omitempty"`       // DDL_TYPE
	State     string    `json:"status,omitempty"`     // STATE
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

type GroupManager interface {
	ListSchemas() ([]string, error)
	CreateSchema(schema string, createTables ...string) error
	ListAllGroups() (map[string][]Group, error)
	ListGroups(schema string) ([]Group, error)
	CountStorages() (int, error)
	GetGroupsOn(schema, storageId string) ([]Group, error)
	PreloadSchema(schema string, addrs ...string) error
	RebalanceCluster(storageExpected int) ([]RebalanceAction, error)
	DrainStorageNodes(storageNodes ...string) ([]RebalanceAction, error)
	GetClusterVersion() (string, error)
	ShowDDL(jobId string) (*DDLStatus, error)
	ShowDDLResult(jobId string) (*DDLResult, error)
	Close() error
}

type groupManager struct {
	ctx context.Context

	dataSource dbutil.MySQLDataSource
	db         *sql.DB

	caseInsensitive bool
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

func scanRebalanceActions(rs *sql.Rows) ([]RebalanceAction, error) {
	rebalanceActions := make([]RebalanceAction, 0)

	var jobId, schema, name, action sql.NullString
	m := map[string]interface{}{
		"JOB_ID": &jobId,
		"SCHEMA": &schema,
		"NAME":   &name,
		"ACTION": &action,
	}
	for rs.Next() {
		err := dbutil.Scan(rs, m, dbutil.ScanOpt{CaseInsensitive: true})
		if err != nil {
			return nil, err
		}
		rebalanceActions = append(rebalanceActions, RebalanceAction{
			JobId:  jobId.String,
			Schema: schema.String,
			Name:   name.String,
			Action: action.String,
		})
	}

	return rebalanceActions, nil
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
func (m *groupManager) RebalanceCluster(storageExpected int) ([]RebalanceAction, error) {
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	storageCnt, err := m.countStorages(conn, ctx)
	if err != nil {
		return nil, err
	}
	if storageCnt < storageExpected {
		return nil, errors.New("storage size not match")
	}

	rs, err := conn.QueryContext(ctx, `rebalance cluster`)
	if err != nil {
		return nil, m.convertRebalanceError(err)
	}
	defer dbutil.DeferClose(rs)

	return scanRebalanceActions(rs)
}

//goland:noinspection SqlNoDataSourceInspection
func (m *groupManager) DrainStorageNodes(storageNodes ...string) ([]RebalanceAction, error) {
	conn, err := m.getConn("")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	rs, err := conn.QueryContext(ctx, fmt.Sprintf(`rebalance cluster drain_node='%s'`,
		strings.Join(storageNodes, ",")))
	if err != nil {
		return nil, m.convertRebalanceError(err)
	}
	defer dbutil.DeferClose(rs)

	return scanRebalanceActions(rs)
}

func (m *groupManager) Close() error {
	if m.db != nil {
		return m.db.Close()
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
