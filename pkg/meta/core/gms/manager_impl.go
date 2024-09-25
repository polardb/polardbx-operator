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

package gms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	meta2 "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"runtime"
	"strings"

	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	dbutil "github.com/alibaba/polardbx-operator/pkg/util/database"
)

// These are initialization DMLs for metadb.
//
//goland:noinspection SqlNoDataSourceInspection,SqlDialectInspection
const (
	createTableServerInfoIfNotExists = `CREATE TABLE IF NOT EXISTS server_info (
  id BIGINT(11) NOT NULL auto_increment,
  gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on UPDATE CURRENT_TIMESTAMP,
  inst_id VARCHAR(128) NOT NULL,
  inst_type INT(11) NOT NULL,
  ip VARCHAR(128) NOT NULL,
  port INT(11) NOT NULL,
  htap_port INT(11) NOT NULL,
  mgr_port INT(11) NOT NULL,
  mpp_port INT(11) NOT NULL,
  status INT(11) NOT NULL,
  region_id VARCHAR(128) DEFAULT NULL,
  azone_id VARCHAR(128) DEFAULT NULL,
  idc_id VARCHAR(128) DEFAULT NULL,
  cpu_core INT(11) DEFAULT NULL,
  mem_size INT(11) DEFAULT NULL,
  extras text DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_inst_id_addr (inst_id, ip, port),
  INDEX idx_inst_id_status (inst_id, status)
) engine = innodb DEFAULT charset = utf8`

	createTableStorageInfoIfNotExists = `CREATE TABLE IF NOT EXISTS storage_info (
  id BIGINT(11) NOT NULL auto_increment,
  gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on UPDATE CURRENT_TIMESTAMP,
  inst_id VARCHAR(128) NOT NULL,
  storage_inst_id VARCHAR(128) NOT NULL,
  storage_master_inst_id VARCHAR(128) NOT NULL,
  ip VARCHAR(128) NOT NULL,
  port INT(11) NOT NULL comment 'port for mysql',
  xport INT(11) DEFAULT NULL comment 'port for x-protocol',
  user VARCHAR(128) NOT NULL,
  passwd_enc text NOT NULL,
  storage_type INT(11) NOT NULL comment '0:x-cluster, 1:mysql, 2:polardb',
  inst_kind INT(11) NOT NULL comment '0:master, 1:slave, 2:metadb',
  status INT(11) NOT NULL comment '0:storage ready, 1:storage not_ready',
  region_id VARCHAR(128) DEFAULT NULL,
  azone_id VARCHAR(128) DEFAULT NULL,
  idc_id VARCHAR(128) DEFAULT NULL,
  max_conn INT(11) NOT NULL,
  cpu_core INT(11) DEFAULT NULL,
  mem_size INT(11) DEFAULT NULL comment 'mem unit: MB',
  is_vip INT(11) DEFAULT NULL COMMENT '0:ip is NOT vip, 1:ip is vip',
  extras text DEFAULT NULL COMMENT 'reserve for extra info',
  PRIMARY KEY (id),
  INDEX idx_inst_id_status (inst_id, status),
  UNIQUE KEY uk_inst_id_addr (storage_inst_id, ip, port, inst_kind, is_vip)
) engine = innodb DEFAULT charset = utf8`

	createTableUserPrivIfNotExists = `CREATE TABLE if not exists user_priv (
  id bigint(11) NOT NULL AUTO_INCREMENT,
  gmt_created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  user_name char(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  host char(60) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  account_type tinyint(1) NOT NULL DEFAULT '0',
  password char(100) COLLATE utf8_unicode_ci NOT NULL,
  select_priv tinyint(1) NOT NULL DEFAULT '0',
  insert_priv tinyint(1) NOT NULL DEFAULT '0',
  update_priv tinyint(1) NOT NULL DEFAULT '0',
  delete_priv tinyint(1) NOT NULL DEFAULT '0',
  create_priv tinyint(1) NOT NULL DEFAULT '0',
  drop_priv tinyint(1) NOT NULL DEFAULT '0',
  grant_priv tinyint(1) NOT NULL DEFAULT '0',
  index_priv tinyint(1) NOT NULL DEFAULT '0',
  alter_priv tinyint(1) NOT NULL DEFAULT '0',
  show_view_priv int(11) NOT NULL DEFAULT '0',
  create_view_priv int(11) NOT NULL DEFAULT '0',
  create_user_priv int(11) NOT NULL DEFAULT '0',
  meta_db_priv int(11) NOT NULL DEFAULT '0',
  show_audit_log_priv int(11) NOT NULL DEFAULT '0',
  replication_client_priv tinyint(1) NOT NULL DEFAULT '0',
  replication_slave_priv tinyint(1) NOT NULL DEFAULT '0',
  plugin varchar(64) COLLATE utf8_bin NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY uk (user_name, host)
) ENGINE = InnoDB DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci COMMENT = 'Users and global privileges'`

	createTableQuarantineConfigIfNotExists = `CREATE TABLE IF NOT EXISTS quarantine_config (
  id BIGINT(11) NOT NULL auto_increment,
  gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on UPDATE CURRENT_TIMESTAMP,
  inst_id VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  group_name VARCHAR(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  net_work_type VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  security_ip_type VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  security_ips text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  PRIMARY KEY (id),
  UNIQUE KEY uk (inst_id, group_name)
) engine = innodb DEFAULT charset = utf8 comment = 'Quarantine config'`

	createTableConfigListenerIfNotExists = `CREATE TABLE IF NOT EXISTS config_listener (
  id bigint(11) NOT NULL AUTO_INCREMENT,
  gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  data_id varchar(200) NOT NULL,
  status int NOT NULL COMMENT '0:normal, 1:removed',
  op_version bigint NOT NULL,
  extras varchar(1024) DEFAULT NULL,
  PRIMARY KEY (id),
  INDEX idx_modify_ts (gmt_modified),
  INDEX idx_status (status),
  UNIQUE KEY uk_data_id (data_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8`

	createTableInstConfigIfNotExists = `create table if not exists inst_config (
  id bigint(11) NOT NULL AUTO_INCREMENT,
  gmt_created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  inst_id varchar(128) NOT NULL,
  param_key varchar(128) NOT NULL,
  param_val varchar(1024) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_inst_id_key (inst_id, param_key)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;`

	createTablePolarDBXExtraIfNotExists = `CREATE TABLE IF NOT EXISTS polardbx_extra (
  id BIGINT(11) NOT NULL auto_increment,
  inst_id VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  type VARCHAR(10) NOT NULL,
  comment VARCHAR(256) NOT NULL,
  status INT(4) NOT NULL,
  gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on
             UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE uk_inst_id_name_type (inst_id, name, type)
) engine = innodb DEFAULT charset = utf8 COLLATE = utf8_unicode_ci comment = 'extra table for polardbx manager'`

	createTableSchemaChangeIfNotExists = `CREATE TABLE IF NOT EXISTS schema_change (
    id           BIGINT(11)      NOT NULL AUTO_INCREMENT,
    table_name   varchar(64)     NOT NULL,
    version      int unsigned    NOT NULL,
    gmt_created  timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    gmt_modified timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY table_name (table_name)
    )   ENGINE = innodb
    DEFAULT CHARSET=utf8;`

	createTableK8sTopologyIfNotExists = `CREATE TABLE IF NOT EXISTS k8s_topology (
    id BIGINT(11) NOT NULL AUTO_INCREMENT,
    uid VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL,
    type VARCHAR(10) NOT NULL,
    gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY(uid),
    UNIQUE KEY(name, type)
) ENGINE = InnoDB DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci COMMENT = 'PolarDBX K8s Topology'`
)

// MetaDB schema map exclude polardbx_extra
var metadbSchemas = map[string]string{
	"server_info":       createTableServerInfoIfNotExists,
	"storage_info":      createTableStorageInfoIfNotExists,
	"user_priv":         createTableUserPrivIfNotExists,
	"quarantine_config": createTableQuarantineConfigIfNotExists,
	"config_listener":   createTableConfigListenerIfNotExists,
	"inst_config":       createTableInstConfigIfNotExists,
	"polardbx_extra":    createTablePolarDBXExtraIfNotExists,
	"schema_change":     createTableSchemaChangeIfNotExists,
}

// These are valid data id formats for config_listener
const (
	clServerInfoDataIdFormat       = "polardbx.server.info.%s"        // {inst_id}
	clStorageInfoDataIdFormat      = "polardbx.storage.info.%s"       // {inst_id}
	clConfigDataIdFormat           = "polardbx.inst.config.%s"        // {inst_id}
	clPrivilegeInfoDataId          = "polardbx.privilege.info"        // {inst_id}
	clInstanceInfoDataId           = "polardbx.inst.info"             //
	clQuarantineConfigDataIdFormat = "polardbx.quarantine.config.%s"  // {inst_id}
	clLockDataIdFormat             = "polardbx.inst.lock.%s"          // {inst_id}
	clDatabaseDataId               = "polardbx.db.info"               //
	clTopologyDataIdFormat         = "polardbx.db.topology.%s"        // %s for {db_name}
	clGroupDataIdFormat            = "polardbx.group.config.%s.%s.%s" // {inst_id}.{db_name}.{grp_name}
	clTableDataIdFormat            = "polardbx.meta.tables.%s"        // {db_name}
	clTableMetaDataIdFormat        = "polardbx.meta.table.%s.%s"      // {db_name}.{table_name}
)

const MetaDBName = "polardbx_meta_db"

type manager struct {
	ctx context.Context

	clusterId      string
	passwordCipher security.PasswordCipher

	metadb MetaDB

	db *sql.DB
}

func (meta *manager) getClusterID() string {
	return meta.clusterId
}

func (meta *manager) getMetaDB() *MetaDB {
	if runtime.GOOS == "darwin" {
		// intercept the metadb host and port in local
		// metadb.Host = "127.1"
		// metadb.Port = 3306
		// metadb.XPort = 31306
		return &MetaDB{
			Id:        meta.metadb.Id,
			Host:      meta.metadb.Host,
			Host4Conn: "127.1",
			Port:      3306,
			XPort:     31306,
			User:      meta.metadb.User,
			Passwd:    meta.metadb.Passwd,
		}
	} else {
		return &meta.metadb
	}
}

func (meta *manager) createMetaDBDatabaseIfNotExist(ctx context.Context) error {
	metadb := meta.getMetaDB()
	db, err := dbutil.OpenMySQLDB(&dbutil.MySQLDataSource{
		Host:     metadb.Host4Conn,
		Port:     metadb.Port,
		Username: metadb.User,
		Password: metadb.Passwd,
		Timeout:  5000,
	})
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(db)

	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+MetaDBName)
	if err != nil {
		return err
	}

	return nil
}

func (meta *manager) checkMetaDBIfDatabaseExist(ctx context.Context) (bool, error) {
	metadb := meta.getMetaDB()
	db, err := dbutil.OpenMySQLDB(&dbutil.MySQLDataSource{
		Host:     metadb.Host4Conn,
		Port:     metadb.Port,
		Username: metadb.User,
		Password: metadb.Passwd,
		Timeout:  5000,
	})
	if err != nil {
		return false, err
	}
	defer dbutil.DeferClose(db)

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SHOW DATABASES LIKE '%s'", MetaDBName))
	if err != nil {
		return false, err
	}
	defer dbutil.DeferClose(rows)

	return rows.Next(), nil
}

func (meta *manager) getConnectionForMetaDB(ctx context.Context) (*sql.Conn, error) {
	if meta.db == nil {
		var err error
		metadb := meta.getMetaDB()
		if meta.db, err = dbutil.OpenMySQLDB(&dbutil.MySQLDataSource{
			Host:     metadb.Host4Conn,
			Port:     metadb.Port,
			Username: metadb.User,
			Password: metadb.Passwd,
			Database: MetaDBName,
			Timeout:  5000,
		}); err != nil {
			return nil, err
		}
	}
	return meta.db.Conn(ctx)
}

func (meta *manager) Close() error {
	if meta.db != nil {
		return meta.db.Close()
	}
	return nil
}

func (meta *manager) ExecuteStatementsAndNotify(statements ...string) error {
	if len(statements) == 0 {
		return nil
	}
	notifyStmt := meta.newNotifyStmt(clInstanceInfoDataId)
	if statements[len(statements)-1] != notifyStmt {
		statements = append(statements, notifyStmt)
	}
	return meta.ExecuteStatementsOnMetaDBInTransaction(statements...)
}

func (meta *manager) ExecuteStatementsOnMetaDBInTransaction(statements ...string) error {
	if len(statements) == 0 {
		return nil
	}

	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		klog.Error("Error when getting connection for metadb: " + err.Error())
		return err
	}
	defer dbutil.DeferClose(conn)

	ctx := meta.ctx

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		klog.Error("Error when begin transaction for metadb: " + err.Error())
		return err
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			klog.Error("Error when executing " + stmt + " on metadb: " + err.Error())
			_ = tx.Rollback()
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		klog.Error("Error when committing transaction on metadb: " + err.Error())
		return err
	}
	return nil
}

//goland:noinspection SqlDialectInspection
func (meta *manager) IsMetaDBExisted() (bool, error) {
	if exists, err := meta.checkMetaDBIfDatabaseExist(meta.ctx); err != nil {
		return false, err
	} else if !exists {
		return false, nil
	}

	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return false, err
	}
	defer dbutil.DeferClose(conn)

	// Execute show tables like to determine if table 'k8s_topology' exists.
	ctx := meta.ctx

	//goland:noinspection SqlNoDataSourceInspection
	rows, err := conn.QueryContext(ctx, `SHOW TABLES LIKE 'k8s_topology'`)
	if err != nil {
		return false, err
	}
	// k8s_topology should be the last table created in initialization.
	initialized := rows.Next()
	if err = rows.Close(); err != nil {
		return false, err
	}

	// Check if current metadb record exists
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	row := conn.QueryRowContext(ctx, `SELECT count(1) FROM storage_info WHERE storage_inst_id = ?`, meta.metadb.Id)
	cnt := 0
	if err = row.Scan(&cnt); err != nil {
		if dbutil.IsMySQLErrTableNotExists(err) {
			return false, nil
		}
		return false, err
	}
	if cnt == 0 {
		return false, nil
	}

	return initialized, nil
}

//goland:noinspection SqlDialectInspection
func (meta *manager) IsMetaDBInitialized(clusterId string) (bool, error) {
	existed, err := meta.IsMetaDBExisted()

	if err != nil {
		return false, err
	}

	if !existed {
		return false, nil
	}

	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return false, err
	}
	defer dbutil.DeferClose(conn)

	ctx := meta.ctx
	// Check if k8s_topology contains current cluster
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	row := conn.QueryRowContext(ctx, `SELECT count(1) FROM k8s_topology WHERE name = ?`, clusterId)
	cnt := 0
	if err = row.Scan(&cnt); err != nil {
		if dbutil.IsMySQLErrTableNotExists(err) {
			return false, nil
		}
		return false, err
	}
	if cnt == 0 {
		return false, nil
	}

	return true, nil
}

func (meta *manager) newStorageNodeInfoForMetaDB() StorageNodeInfo {
	return StorageNodeInfo{
		Id:            meta.metadb.Id,
		MasterId:      meta.metadb.Id,
		ClusterId:     meta.getClusterID(),
		Host:          meta.metadb.Host,
		Port:          int32(meta.metadb.Port),
		XProtocolPort: int32(meta.metadb.XPort),
		User:          meta.metadb.User,
		Passwd:        meta.metadb.Passwd,
		Type:          meta.metadb.Type,
		Kind:          StorageKindMetaDB,
		Status:        PSNodeEnabled,
		MaxConn:       10000,
		CpuCore:       4,
		MemSize:       int64(8) << 30,
		IsVip:         IsVip,
	}
}

//goland:noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func (meta *manager) InitializeMetaDBSchema() error {
	// Create database if not exists
	if err := meta.createMetaDBDatabaseIfNotExist(meta.ctx); err != nil {
		return err
	}

	// Do other initializations
	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(conn)

	// Create tables one-by-one
	ctx := meta.ctx

	for tableName, ddlStmt := range metadbSchemas {
		if _, err := conn.ExecContext(ctx, ddlStmt); err != nil {
			return errors.New("unable to create table " + tableName + ":" + err.Error())
		}
	}

	// Insert the version record
	const schemaChangeInitDml = "insert ignore into `schema_change`(`table_name`, `version`) values('user_priv', 10)"
	if _, err := conn.ExecContext(ctx, schemaChangeInitDml); err != nil {
		return fmt.Errorf("unable to insert schema change init record: %w", err)
	}

	// Insert the metadb record
	if _, err := conn.ExecContext(ctx, meta.newInsertStorageNodeStatement(meta.newStorageNodeInfoForMetaDB())); err != nil {
		return errors.New("unable to insert metadb record: " + err.Error())
	}

	// Finally initialize the k8s topology table as a sign of initialized
	_, err = conn.ExecContext(ctx, createTableK8sTopologyIfNotExists)
	if err != nil {
		return errors.New("failed to create table polardbx_extra: " + err.Error())
	}

	return nil
}

//goland:noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func (meta *manager) InitializeMetaDBInfo(readonly bool) error {
	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(conn)

	ctx := meta.ctx

	// Initialize the necessary instance level config_listener records
	for _, dataId := range append([]string{
		fmt.Sprintf(clServerInfoDataIdFormat, meta.getClusterID()),
		fmt.Sprintf(clStorageInfoDataIdFormat, meta.getClusterID()),
		fmt.Sprintf(clConfigDataIdFormat, meta.getClusterID()),
		fmt.Sprintf(clQuarantineConfigDataIdFormat, meta.getClusterID()),
		clPrivilegeInfoDataId}) {
		createStmt := fmt.Sprintf(`INSERT IGNORE INTO config_listener 
						(id, gmt_created, gmt_modified, data_id, status, op_version, extras) 
						VALUES (NULL, NOW(), NOW(), '%s', 0, 0, NULL)`, dataId)
		if _, err = conn.ExecContext(ctx, createStmt); err != nil {
			return errors.New("failed to create record in config_listener for " + dataId + ": " + err.Error())
		}
	}

	if _, err := conn.ExecContext(ctx, "INSERT IGNORE INTO `quarantine_config` VALUES (NULL, NOW(), NOW(), ?, 'default', NULL, NULL, ?)",
		meta.getClusterID(), "0.0.0.0/0"); err != nil {
		return errors.New("unable to insert default quarantine_config: " + err.Error())
	}

	clusterType := meta2.TypeMaster
	if readonly {
		clusterType = meta2.TypeReadonly
	}

	// Finally add current cluster info to k8s topology table to mark the initialization is finished
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`INSERT IGNORE INTO k8s_topology
    							(uid, name, type, gmt_created, gmt_modified)
								VALUES ('%s', '%s', '%s', NOW(), NOW())`, meta.getClusterID(), meta.getClusterID(), clusterType))

	if err != nil {
		return errors.New("failed to insert record into k8s_topology: " + err.Error())
	}

	return nil
}

//goland:noinspection SqlDialectInspection
func (meta *manager) IsGmsSchemaRestored() (bool, error) {
	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return false, err
	}
	defer dbutil.DeferClose(conn)

	// Execute show tables like to determine if table 'polardbx_extra' exists.
	ctx := meta.ctx

	// Check if current metadb record exists
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	row := conn.QueryRowContext(ctx, `SELECT count(1) FROM storage_info WHERE storage_inst_id = ? AND is_vip = 1 AND inst_kind = 2`, meta.metadb.Id)
	cnt := 0
	if err = row.Scan(&cnt); err != nil {
		return false, err
	}
	if cnt == 0 {
		return false, nil
	} else if cnt == 1 {
		return true, nil
	} else {
		return false, errors.New("invalid gms, found multiple metadb records")
	}
}

//goland:noinspection SqlDialectInspection
func (meta *manager) RestoreSchemas(fromPxcCluster, fromPxcHash, PxcHash string, originalDnMap map[string]string) error {
	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return err
	}
	defer dbutil.DeferClose(conn)

	ctx := meta.ctx

	// update DB info
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	_, err = conn.ExecContext(ctx, "UPDATE db_info SET app_name=CONCAT(db_name, CONCAT('@', ?))", meta.getClusterID())
	if err != nil {
		return errors.New("unable to update db info: " + err.Error())
	}

	// Clear non-master configs
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	_, err = conn.ExecContext(ctx, "DELETE FROM inst_config WHERE inst_id IN (SELECT inst_id FROM server_info WHERE inst_type != 0 )")
	if err != nil {
		return errors.New("unable to clear non-master configs: " + err.Error())
	}

	for k, v := range originalDnMap {
		// Reset the single group's storage id
		_, err = conn.ExecContext(ctx, "UPDATE inst_config SET param_val=REPLACE(param_val, ?, ?) where param_key='SINGLE_GROUP_STORAGE_INST_LIST'", k, v)
		if err != nil {
			return errors.New("unable to reset single group's storage id: " + err.Error())
		}
		// update topologies
		_, err = conn.ExecContext(ctx, "UPDATE group_detail_info SET inst_id=?, storage_inst_id=REPLACE(storage_inst_id, ?, ?)", meta.getClusterID(), k, v)
		if err != nil {
			return errors.New("unable to update group topologies: " + err.Error())
		}
	}

	// FIXME not robust, some name's like 'gms-gms' will be replaced to 'a-a'. Just avoid names like 'gms', 'dn-%d'.
	// Clean non-master topologies
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	_, err = conn.ExecContext(ctx, "DELETE FROM group_detail_info WHERE inst_id IN (SELECT inst_id FROM server_info WHERE inst_type != 0 )")
	if err != nil {
		return errors.New("unable to clear non-master topologies: " + err.Error())
	}

	// update configs
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	_, err = conn.ExecContext(ctx, "UPDATE inst_config SET inst_id = ? WHERE inst_id = ?", meta.getClusterID(), fromPxcCluster)
	if err != nil {
		return errors.New("unable to update configs: " + err.Error())
	}

	// Clean config_listener
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DELETE FROM config_listener WHERE data_id LIKE '%%.%s.%%'", meta.getClusterID()))
	if err != nil {
		return errors.New("unable to clean config listeners: " + err.Error())
	}

	// Check available tables in meta db
	tables := make(map[string]struct{})
	rows, err := conn.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return errors.New("unable to get available tables in meta db: " + err.Error())
	}
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return errors.New("unable to get available tables in meta db: " + err.Error())
		}
		tables[table] = struct{}{}
	}

	// Truncate tables.
	for _, table := range []string{"server_info", "storage_info", "quarantine_config", "inst_lock", "node_info",
		"backfill_objects", "scaleout_backfill_objects", "scaleout_checker_reports", "scaleout_outline",
		"checker_reports", "ddl_engine", "ddl_engine_task", "read_write_lock", "binlog_dumper_info", "binlog_logic_meta_history",
		"binlog_node_info", "binlog_oss_record", "binlog_phy_ddl_history", "binlog_polarx_command", "binlog_storage_history",
		"binlog_system_config", "binlog_task_config", "binlog_task_info", "binlog_schedule_history", "inst_lock", "quarantine_config",
		"concurrency_control_rule", "concurrency_control_trigger"} {
		//goland:noinspection SqlNoDataSourceInspection
		if _, ok := tables[table]; !ok {
			continue
		}
		_, err := conn.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", table))
		if err != nil {
			return fmt.Errorf("unable to truncate table %s: %s", table, err.Error())
		}
	}

	// Delete the polardbx_root account from `user_priv`.
	//goland:noinspection SqlResolve,SqlNoDataSourceInspection
	_, err = conn.ExecContext(ctx, "DELETE FROM user_priv WHERE user_name = ?", convention.RootAccount)
	if err != nil {
		return errors.New("unable to delete root account: " + err.Error())
	}

	// Insert the default quarantine config
	//goland:noinspection SqlNoDataSourceInspection, SqlResolve
	if _, err := conn.ExecContext(ctx, "INSERT IGNORE INTO `quarantine_config` VALUES (NULL, NOW(), NOW(), ?, 'default', NULL, NULL, ?)",
		meta.getClusterID(), "0.0.0.0/0"); err != nil {
		return errors.New("unable to insert default quarantine_config: " + err.Error())
	}

	// Finally insert the metadb record.
	if _, err := conn.ExecContext(ctx, meta.newInsertStorageNodeStatement(meta.newStorageNodeInfoForMetaDB())); err != nil {
		return errors.New("unable to insert metadb record: " + err.Error())
	}

	return nil
}

func (c *ComputeNodeInfo) toInsertValue(clusterId string, clusterKind ClusterKind, status PCNodeStatus) string {
	return fmt.Sprintf("(NULL, NOW(), NOW(), '%s', %d, '%s', %d, %d, %d, %d, %d, NULL, NULL, NULL, %d, %d, '%s')",
		clusterId, clusterKind, c.Host, c.Port, c.HtapPort, c.MgrPort, c.MppPort, status, c.CpuCore, c.MemSize, c.Extra)
}

func (meta *manager) EnableComputeNodes(computeNodes ...ComputeNodeInfo) error {
	if len(computeNodes) == 0 {
		return nil
	}

	// Generate statement for insert
	insertValues := make([]string, len(computeNodes))
	for i, c := range computeNodes {
		insertValues[i] = c.toInsertValue(meta.getClusterID(), MasterCluster, PCNodeEnabled)
	}

	stmt := fmt.Sprintf(`
			 INSERT INTO server_info (id, gmt_created, gmt_modified, inst_id, inst_type, ip, 
			   port, htap_port, mgr_port, mpp_port, status, region_id, azone_id, idc_id, cpu_core, mem_size, extras) 
			 VALUES %s
			 ON DUPLICATE KEY UPDATE status = VALUES(status), gmt_modified = NOW()`, insertValues)
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clServerInfoDataIdFormat, meta.getClusterID()))

	// Execute the statement
	return meta.ExecuteStatementsAndNotify(stmt, notifyStmt)
}

func (c *ComputeNodeInfo) toSelectCriteria() string {
	return fmt.Sprintf("( (ip = '%s' AND port = %d and  extras is null ) OR (extras like '%%%s%%' ))", c.Host, c.Port, c.Extra)
}

func (meta *manager) DisableComputeNodes(computeNodes ...ComputeNodeInfo) error {
	if len(computeNodes) == 0 {
		return nil
	}

	// Generate statement for update
	selectCriteria := make([]string, len(computeNodes))
	for i, c := range computeNodes {
		selectCriteria[i] = c.toSelectCriteria()
	}

	stmt := fmt.Sprintf(`UPDATE server_info SET status = %d WHERE %s`, PCNodeDisabled,
		strings.Join(selectCriteria, " OR "))
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clServerInfoDataIdFormat, meta.getClusterID()))

	// Execute the statement
	return meta.ExecuteStatementsAndNotify(stmt, notifyStmt)
}

func (meta *manager) DisableAllComputeNodes(primaryPolardbxName string) error {
	deleteStmt := fmt.Sprintf(`UPDATE server_info SET status = %d WHERE inst_id = '%s'`, PCNodeDisabled, meta.getClusterID())
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clServerInfoDataIdFormat, primaryPolardbxName))

	return meta.ExecuteStatementsAndNotify(deleteStmt, notifyStmt)
}

func (meta *manager) DeleteComputeNodes(computeNodes ...ComputeNodeInfo) error {
	if len(computeNodes) == 0 {
		return nil
	}

	// Generate statement for delete
	selectCriteria := make([]string, len(computeNodes))
	for i, c := range computeNodes {
		selectCriteria[i] = c.toSelectCriteria()
	}

	stmt := fmt.Sprintf(`DELETE FROM server_info WHERE %s`,
		strings.Join(selectCriteria, " OR "))
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clServerInfoDataIdFormat, meta.getClusterID()))

	// Execute the statement
	return meta.ExecuteStatementsAndNotify(stmt, notifyStmt)
}

func (meta *manager) ListComputeNodes() ([]ComputeNodeInfo, error) {
	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	selectStmt := fmt.Sprintf(`SELECT id, inst_id, inst_type, ip, port, htap_port, mgr_port, mpp_port, status, cpu_core, mem_size, extras 
							  FROM server_info WHERE status = %d`, PCNodeEnabled)

	rs, err := conn.QueryContext(meta.ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	nodes := make([]ComputeNodeInfo, 0, 1)

	var id, port, htapPort, mgrPort, mppPort, status, cpuCore, memSize int
	var instId, instType, ip string
	var extras sql.NullString
	for rs.Next() {
		err = rs.Scan(&id, &instId, &instType, &ip, &port, &htapPort, &mgrPort, &mppPort, &status, &cpuCore, &memSize, &extras)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, ComputeNodeInfo{
			Host:     ip,
			Port:     int32(port),
			HtapPort: int32(htapPort),
			MgrPort:  int32(mgrPort),
			MppPort:  int32(mppPort),
			Status:   PCNodeStatus(status),
			CpuCore:  int32(cpuCore),
			MemSize:  int64(memSize),
			Extra:    extras.String,
		})
	}

	return nodes, nil
}

func (meta *manager) SyncComputeNodes(computeNodes ...ComputeNodeInfo) error {
	if len(computeNodes) == 0 {
		return nil
	}

	// Generate statements
	selectCriteria := make([]string, len(computeNodes))
	insertValues := make([]string, len(computeNodes))
	for i, c := range computeNodes {
		selectCriteria[i] = "NOT " + c.toSelectCriteria()
		insertValues[i] = c.toInsertValue(meta.getClusterID(), MasterCluster, PCNodeEnabled)
	}

	deleteStmt := fmt.Sprintf(`DELETE FROM server_info WHERE %s`, strings.Join(selectCriteria, " AND "))
	insertStmt := fmt.Sprintf(`INSERT IGNORE INTO server_info (id, gmt_created, gmt_modified, inst_id, inst_type, ip, 
			   port, htap_port, mgr_port, mpp_port, status, region_id, azone_id, idc_id, cpu_core, mem_size, extras) WHERE %s`,
		strings.Join(insertValues, ", "))
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clServerInfoDataIdFormat, meta.getClusterID()))

	// Execute statements
	return meta.ExecuteStatementsAndNotify(deleteStmt, insertStmt, notifyStmt)
}

func (s *StorageNodeInfo) toInsertValues(cipher security.PasswordCipher) string {
	return fmt.Sprintf("(NULL, NOW(), NOW(), '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', "+
		"%d, %d, %d, NULL, NULL, NULL, %d, %d, %d, %d, '%s')",
		s.ClusterId, s.Id, s.MasterId, s.Host, s.Port, s.XProtocolPort, s.User, cipher.Encrypt(s.Passwd), s.Type, s.Kind,
		PSNodeEnabled, s.MaxConn, s.CpuCore, s.MemSize, s.IsVip, s.Extra)
}

func (meta *manager) newInsertStorageNodeStatement(storageNodes ...StorageNodeInfo) string {
	insertValues := make([]string, len(storageNodes))
	for i, s := range storageNodes {
		insertValues[i] = s.toInsertValues(meta.passwordCipher)
	}

	return `INSERT IGNORE INTO storage_info (id, gmt_created, gmt_modified, inst_id, storage_inst_id, storage_master_inst_id,
				ip, port, xport, user, passwd_enc, storage_type, inst_kind, status, region_id, azone_id, idc_id, max_conn, 
				cpu_core, mem_size, is_vip, extras) VALUES ` + strings.Join(insertValues, ", ")
}

func (meta *manager) SyncStorageNodes(storageNodes ...StorageNodeInfo) error {
	if len(storageNodes) == 0 {
		return nil
	}

	// Generate statement for insert
	storageIds := make([]string, len(storageNodes))
	insertValues := make([]string, len(storageNodes))
	for i, s := range storageNodes {
		storageIds[i] = "'" + s.Id + "'"
		insertValues[i] = s.toInsertValues(meta.passwordCipher)
	}

	deleteStmt := fmt.Sprintf(`DELETE FROM storage_info WHERE storage_inst_id NOT IN (%s)`, strings.Join(storageIds, ", "))
	stmt := `INSERT IGNORE INTO storage_info (id, gmt_created, gmt_modified, inst_id, storage_inst_id, storage_master_inst_id,
				ip, port, xport, user, passwd_enc, storage_type, inst_kind, status, region_id, azone_id, idc_id, max_conn, 
				cpu_core, mem_size, is_vip, extras) VALUES ` +
		strings.Join(insertValues, ", ")
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clStorageInfoDataIdFormat, meta.getClusterID()))

	// Execute the statement
	return meta.ExecuteStatementsAndNotify(deleteStmt, stmt, notifyStmt)
}

func (meta *manager) EnableStorageNodes(storageNodes ...StorageNodeInfo) error {
	if len(storageNodes) == 0 {
		return nil
	}

	// Generate statement for insert
	insertValues := make([]string, len(storageNodes))
	for i, s := range storageNodes {
		insertValues[i] = s.toInsertValues(meta.passwordCipher)
	}

	stmt := `INSERT IGNORE INTO storage_info (id, gmt_created, gmt_modified, inst_id, storage_inst_id, storage_master_inst_id,
				ip, port, xport, user, passwd_enc, storage_type, inst_kind, status, region_id, azone_id, idc_id, max_conn, 
				cpu_core, mem_size, is_vip, extras) VALUES ` +
		strings.Join(insertValues, ", ")
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clStorageInfoDataIdFormat, meta.getClusterID()))

	// Execute the statement
	return meta.ExecuteStatementsAndNotify(stmt, notifyStmt)
}

func (meta *manager) UpdateRoStorageNodes(storageNodes ...StorageNodeInfo) error {
	if len(storageNodes) == 0 {
		return nil
	}
	stmts := make([]string, 0)
	for _, storageNode := range storageNodes {
		stmts = append(stmts, meta.newUpdateRoStorageInfoStmt(storageNode))
	}
	stmts = append(stmts, meta.newNotifyStmt(fmt.Sprintf(clStorageInfoDataIdFormat, meta.getClusterID())))

	// Execute the statement
	return meta.ExecuteStatementsAndNotify(stmts...)
}

func (s *StorageNodeInfo) toSelectCriteria() string {
	return fmt.Sprintf("(storage_inst_id = '%s')", s.Id)
}

func (meta *manager) DisableStorageNodes(storageNodes ...StorageNodeInfo) error {
	if len(storageNodes) == 0 {
		return nil
	}

	// Generate statement for delete
	selectCriteria := make([]string, len(storageNodes))
	for i, s := range storageNodes {
		selectCriteria[i] = s.toSelectCriteria()
	}

	stmt := `DELETE FROM storage_info WHERE ` + strings.Join(selectCriteria, " OR ")
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clStorageInfoDataIdFormat, meta.getClusterID()))

	// Execute the statement
	return meta.ExecuteStatementsAndNotify(stmt, notifyStmt)
}

func (meta *manager) DisableAllStorageNodes(primaryPolardbxName string) error {
	stmt := fmt.Sprintf(`DELETE FROM storage_info WHERE inst_id = '%s'`, meta.getClusterID())
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clStorageInfoDataIdFormat, primaryPolardbxName))

	return meta.ExecuteStatementsAndNotify(stmt, notifyStmt)
}

//goland:noinspection SqlDialectInspection,SqlNoDataSourceInspection
func (meta *manager) ListStorageNodes(kind StorageKind) ([]StorageNodeInfo, error) {
	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	// Select all storage nodes which records the VIP.
	selectStmt := `SELECT storage_inst_id, ip, port, storage_type, inst_kind, status 
FROM storage_info WHERE is_vip = 1 AND inst_id = ? AND inst_kind = ?`

	rs, err := conn.QueryContext(meta.ctx, selectStmt, meta.getClusterID(), kind)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	nodes := make([]StorageNodeInfo, 0, 1)

	for rs.Next() {
		storageNode := StorageNodeInfo{}
		err = rs.Scan(
			&storageNode.Id,
			&storageNode.Host,
			&storageNode.Port,
			&storageNode.Type,
			&storageNode.Kind,
			&storageNode.Status,
		)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, storageNode)
	}

	return nodes, nil
}

func (meta *manager) ListDynamicParams() (map[string]string, error) {
	ctx := meta.ctx
	conn, err := meta.getConnectionForMetaDB(ctx)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SELECT param_key, param_val FROM inst_config WHERE inst_id = '%s'`,
		meta.getClusterID()))
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rows)

	params := make(map[string]string)
	var paramKey, paramVal string
	for rows.Next() {
		if err := rows.Scan(&paramKey, &paramVal); err != nil {
			return nil, err
		}
		params[paramKey] = paramVal
	}

	return params, nil
}

func (meta *manager) SyncDynamicParams(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}

	// Generate statement for insert
	stmtValues := make([]string, 0, len(params))
	for paramKey, paramValue := range params {
		stmtValues = append(stmtValues, fmt.Sprintf("(NULL, NOW(), NOW(), '%s', '%s', '%s')",
			meta.getClusterID(), paramKey, paramValue))
	}

	stmt := `INSERT INTO inst_config (id, gmt_created, gmt_modified, inst_id, param_key, param_val) VALUES ` +
		strings.Join(stmtValues, ", ") +
		` ON DUPLICATE KEY UPDATE param_val = VALUES(param_val), gmt_modified = NOW()`
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clConfigDataIdFormat, meta.getClusterID()))

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(stmt, notifyStmt)
}

func (meta *manager) SetGlobalVariables(key, value string) error {
	stmt := fmt.Sprintf("SET GlOBAL %s=%s", key, value)

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(stmt)
}

type userPrivSchema struct {
	User                  string
	Host                  string
	encPasswd             string
	SelectPriv            int8
	InsertPriv            int8
	UpdatePriv            int8
	DeletePriv            int8
	CreatePriv            int8
	DropPriv              int8
	GrantPriv             int8
	IndexPriv             int8
	AlterPriv             int8
	ShowViewPriv          int8
	CreateViewPriv        int8
	CreateUserPriv        int8
	MetaDbPriv            int8
	AccountType           int8 // 0: normal, 5: god
	ShowAuditLogPriv      int8
	ReplicationClientPriv int8
	ReplicationSlavePriv  int8
}

func newSchemaSuperUser(user, encPasswd string, grantType GrantPrivilegeType) *userPrivSchema {
	switch grantType {
	case GrantSuperPrivilege, GrantAllPrivilege:
		return &userPrivSchema{
			User:                  user,
			Host:                  "%",
			encPasswd:             encPasswd,
			SelectPriv:            1,
			InsertPriv:            1,
			UpdatePriv:            1,
			DeletePriv:            1,
			CreatePriv:            1,
			DropPriv:              1,
			GrantPriv:             1,
			IndexPriv:             1,
			AlterPriv:             1,
			ShowViewPriv:          1,
			CreateViewPriv:        1,
			CreateUserPriv:        1,
			MetaDbPriv:            1,
			AccountType:           5,
			ShowAuditLogPriv:      1,
			ReplicationClientPriv: 1,
			ReplicationSlavePriv:  1,
		}
	case GrantDdlPrivilege:
		return &userPrivSchema{
			User:                  user,
			Host:                  "%",
			encPasswd:             encPasswd,
			SelectPriv:            0,
			InsertPriv:            0,
			UpdatePriv:            0,
			DeletePriv:            0,
			CreatePriv:            1,
			DropPriv:              1,
			GrantPriv:             0,
			IndexPriv:             1,
			AlterPriv:             1,
			ShowViewPriv:          1,
			CreateViewPriv:        1,
			CreateUserPriv:        0,
			MetaDbPriv:            0,
			AccountType:           0,
			ShowAuditLogPriv:      0,
			ReplicationClientPriv: 0,
			ReplicationSlavePriv:  0,
		}
	case GrantReadWritePrivilege:
		return &userPrivSchema{
			User:                  user,
			Host:                  "%",
			encPasswd:             encPasswd,
			SelectPriv:            1,
			InsertPriv:            1,
			UpdatePriv:            1,
			DeletePriv:            1,
			CreatePriv:            0,
			DropPriv:              0,
			GrantPriv:             0,
			IndexPriv:             0,
			AlterPriv:             0,
			ShowViewPriv:          1,
			CreateViewPriv:        0,
			CreateUserPriv:        0,
			MetaDbPriv:            0,
			AccountType:           0,
			ShowAuditLogPriv:      0,
			ReplicationClientPriv: 1,
			ReplicationSlavePriv:  1,
		}
	case GrantReadOnlyPrivilege:
		return &userPrivSchema{
			User:                  user,
			Host:                  "%",
			encPasswd:             encPasswd,
			SelectPriv:            1,
			InsertPriv:            0,
			UpdatePriv:            0,
			DeletePriv:            0,
			CreatePriv:            0,
			DropPriv:              0,
			GrantPriv:             0,
			IndexPriv:             0,
			AlterPriv:             0,
			ShowViewPriv:          1,
			CreateViewPriv:        0,
			CreateUserPriv:        0,
			MetaDbPriv:            0,
			AccountType:           0,
			ShowAuditLogPriv:      0,
			ReplicationClientPriv: 1,
			ReplicationSlavePriv:  1,
		}
	default:
		return &userPrivSchema{
			User:                  user,
			Host:                  "%",
			encPasswd:             encPasswd,
			SelectPriv:            0,
			InsertPriv:            0,
			UpdatePriv:            0,
			DeletePriv:            0,
			CreatePriv:            0,
			DropPriv:              0,
			GrantPriv:             0,
			IndexPriv:             0,
			AlterPriv:             0,
			ShowViewPriv:          0,
			CreateViewPriv:        0,
			CreateUserPriv:        0,
			MetaDbPriv:            0,
			AccountType:           0,
			ShowAuditLogPriv:      0,
			ReplicationClientPriv: 0,
			ReplicationSlavePriv:  0,
		}
	}
}

func (schema *userPrivSchema) valueHeader() []string {
	return []string{"id", "gmt_created", "gmt_modified", "user_name", "host", "password", "select_priv", "insert_priv",
		"update_priv", "delete_priv", "create_priv", "drop_priv", "grant_priv", "index_priv", "alter_priv", "show_view_priv",
		"create_view_priv", "create_user_priv", "meta_db_priv", "account_type", "show_audit_log_priv", "replication_client_priv", "replication_slave_priv"}
}

func (schema *userPrivSchema) toInsertValue() string {
	return fmt.Sprintf("(NULL, NOW(), NOW(), '%s', '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)",
		schema.User, schema.Host, schema.encPasswd, schema.SelectPriv, schema.IndexPriv, schema.UpdatePriv, schema.DeletePriv,
		schema.CreatePriv, schema.DropPriv, schema.GrantPriv, schema.IndexPriv, schema.AlterPriv, schema.ShowViewPriv,
		schema.CreateViewPriv, schema.CreateUserPriv, schema.MetaDbPriv, schema.AccountType, schema.ShowAuditLogPriv,
		schema.ReplicationClientPriv, schema.ReplicationSlavePriv)
}

func (meta *manager) newNotifyStmt(dataId string) string {
	return fmt.Sprintf(`UPDATE config_listener SET op_version = op_version + 1 WHERE data_id = '%s'`, dataId)
}

func (meta *manager) newUpdateRoStorageInfoStmt(record StorageNodeInfo) string {
	return fmt.Sprintf(` 
        update storage_info 
                    set ip = '%s', port = %d, xport = %d, gmt_modified = now() 
        where 
              storage_inst_id = '%s' and inst_id = '%s' and inst_kind = 1 and is_vip = 0 limit 1`, record.Host, record.Port, record.XProtocolPort, record.Id, record.ClusterId)
}

func (meta *manager) CreateDBAccount(user, passwd string, grantOptions ...*GrantOption) error {
	// FIXME current only the first valid grant type is considered.
	grantType := GrantSuperPrivilege
	if len(grantOptions) > 0 {
		grantType = grantOptions[0].Type
	}

	// Generate schema and the insert statement
	schema := newSchemaSuperUser(user, security.MustSha1Hash(passwd), grantType)

	insertStmt := fmt.Sprintf(`INSERT IGNORE INTO user_priv (%s) VALUES %s`,
		strings.Join(schema.valueHeader(), ", "), schema.toInsertValue())
	notifyStmt := meta.newNotifyStmt(clPrivilegeInfoDataId)

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(insertStmt, notifyStmt)
}

func (meta *manager) ModifyDBAccountType(user string, accountType AccountType) error {
	modifyStmt := fmt.Sprintf("UPDATE user_priv SET account_type = '%s' WHERE user_name='%s'", string(accountType), user)
	notifyStmt := meta.newNotifyStmt(clPrivilegeInfoDataId)

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(modifyStmt, notifyStmt)
}

func (meta *manager) DeleteDBAccount(user string) error {
	// Generate delete statement
	deleteStmt := fmt.Sprintf("DELETE FROM user_priv WHERE user = '%s'", user)
	notifyStmt := meta.newNotifyStmt(clPrivilegeInfoDataId)

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(deleteStmt, notifyStmt)
}

func (meta *manager) SyncAccountPasswd(user, passwd string) error {
	// Generate update statement
	updateStmt := fmt.Sprintf("UPDATE user_priv SET password = '%s' WHERE user = '%s'", security.MustSha1Hash(passwd), user)
	notifyStmt := meta.newNotifyStmt(clPrivilegeInfoDataId)

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(updateStmt, notifyStmt)
}

func (meta *manager) SyncSecurityIPs(ips []string) error {
	// No-op.
	return nil
}

func (n *CdcNodeInfo) toSelectCriteria() string {
	if len(n.ContainerId) > 0 {
		return fmt.Sprintf("(container_id = '%s')", n.ContainerId)
	} else {
		return fmt.Sprintf("(ip = '%s' AND daemon_port = %d)", n.Host, n.DaemonPort)
	}
}

func (meta *manager) ListCdcNodes() ([]CdcNodeInfo, error) {
	conn, err := meta.getConnectionForMetaDB(meta.ctx)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	selectStmt := "SELECT container_id, ip, daemon_port FROM binlog_node_info"

	rs, err := conn.QueryContext(meta.ctx, selectStmt)
	if err != nil {
		if dbutil.IsMySQLErrTableNotExists(err) {
			return nil, nil
		}
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	cdcNodes := make([]CdcNodeInfo, 0)
	for rs.Next() {
		cdcNode := CdcNodeInfo{}
		err := rs.Scan(&cdcNode.ContainerId, &cdcNode.Host, &cdcNode.DaemonPort)
		if err != nil {
			return nil, err
		}
		cdcNodes = append(cdcNodes, cdcNode)
	}
	return cdcNodes, nil
}

func (meta *manager) DeleteCdcNodes(cdcNodes ...CdcNodeInfo) error {
	if len(cdcNodes) == 0 {
		return nil
	}

	selectCriteriaList := make([]string, 0, len(cdcNodes))
	for _, n := range cdcNodes {
		selectCriteriaList = append(selectCriteriaList, n.toSelectCriteria())
	}

	deleteStmt := fmt.Sprintf("DELETE from binlog_node_info WHERE %s", strings.Join(selectCriteriaList, " OR "))

	err := meta.ExecuteStatementsAndNotify(deleteStmt)
	if dbutil.IsMySQLErrTableNotExists(err) {
		return nil
	}
	return err
}

func (meta *manager) Lock() error {
	// Generate update statement
	lockStmt := fmt.Sprintf(`INSERT IGNORE INTO  
							inst_lock (id, gmt_created, gmt_modified, inst_id, locked) 
							VALUES (NULL, NOW(), NOW(), "%s", 1)`, meta.getClusterID())
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clLockDataIdFormat, meta.getClusterID()))

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(lockStmt, notifyStmt)
}

func (meta *manager) Unlock() error {
	// Generate update statement
	unlockStmt := fmt.Sprintf(`DELETE FROM inst_lock WHERE inst_id = "%s"`, meta.getClusterID())
	notifyStmt := meta.newNotifyStmt(fmt.Sprintf(clLockDataIdFormat, meta.getClusterID()))

	// Execute the statements
	return meta.ExecuteStatementsAndNotify(unlockStmt, notifyStmt)
}

func quoteStringSlice(l []string) []string {
	t := make([]string, len(l))
	for i, s := range l {
		t[i] = "'" + s + "'"
	}
	return t
}

func (meta *manager) SyncK8sTopology(topology *K8sTopology) error {
	uids := make([]string, 0, 2)
	uids = append(uids, string(topology.GMS.Uid))
	for _, obj := range topology.DN {
		uids = append(uids, string(obj.Uid))
	}
	excludeCriteria := fmt.Sprintf("uid NOT IN (%s)", strings.Join(quoteStringSlice(uids), ", "))
	deleteStmt := fmt.Sprintf("DELETE FROM k8s_topology WHERE " + excludeCriteria)

	insertValues := make([]string, 0, 2)
	insertValues = append(insertValues, fmt.Sprintf("('%s', '%s', '%s')", topology.GMS.Uid, topology.GMS.Name, "gms"))
	for name, obj := range topology.DN {
		insertValues = append(insertValues, fmt.Sprintf("('%s', '%s', '%s')", obj.Uid, name, "dn"))
	}
	insertStmt := fmt.Sprintf("INSERT IGNORE INTO k8s_topology (uid, name, type) VALUES %s", strings.Join(insertValues, ", "))

	return meta.ExecuteStatementsOnMetaDBInTransaction(createTableK8sTopologyIfNotExists, deleteStmt, insertStmt)
}

//goland:noinspection SqlDialectInspection
func (meta *manager) GetK8sTopology() (*K8sTopology, error) {
	ctx := meta.ctx
	conn, err := meta.getConnectionForMetaDB(ctx)
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(conn)

	topology := &K8sTopology{
		DN: make(map[string]K8sObject),
	}

	//goland:noinspection SqlResolve,SqlNoDataSourceInspection
	rs, err := conn.QueryContext(ctx, "SELECT uid, name, type FROM k8s_topology")
	if err != nil {
		return nil, err
	}
	defer dbutil.DeferClose(rs)

	var uid, name, objType string
	for rs.Next() {
		if err := rs.Scan(&uid, &name, &objType); err != nil {
			return nil, err
		}

		switch objType {
		case "gms":
			topology.GMS = K8sObject{Uid: types.UID(uid), Name: name}
		case "dn":
			topology.DN[name] = K8sObject{Uid: types.UID(uid), Name: name}
		}
	}

	return topology, nil
}

type MetaDB struct {
	Id        string
	Host      string
	Host4Conn string
	Port      int
	XPort     int
	User      string
	Passwd    string

	Type StorageType
}

func NewGmsManager(ctx context.Context, clusterId string, metadb *MetaDB, cipher security.PasswordCipher) Manager {
	if ctx == nil {
		ctx = context.Background()
	}

	mgr := &manager{
		ctx:            ctx,
		clusterId:      clusterId,
		metadb:         *metadb,
		passwordCipher: cipher,
	}
	return mgr
}
