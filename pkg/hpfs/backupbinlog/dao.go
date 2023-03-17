package backupbinlog

import (
	"database/sql"
	_ "modernc.org/sqlite"
)

const (
	TableName      = "upload_record"
	CreateTableSQL = `CREATE TABLE IF NOT EXISTS ` + TableName + ` (
    id BIGINT not null,
    pxc_name varchar(200) not null,
	pxc_uid  varchar(200) not null,
    xstore_name varchar(200) not null,
    pod_name varchar(200) not null,
    version varchar(200) not null,
    filepath varchar(300) not null,
    filename varchar(50) not null,
    num BIGINT not null,
    start_index BIGINT,
    sink_name varchar(50) not null,
    sink_type varchar(50) not null,
	binlog_checksum varchar(50) not null,
    hash_val varchar(200),
    status INT DEFAULT 0 ,
    err_message TEXT,
	file_mod_at datetime default CURRENT_TIMESTAMP,
 	created_at datetime default CURRENT_TIMESTAMP,
	updated_at datetime default CURRENT_TIMESTAMP,
    PRIMARY key (id)
  )`
	CreateIndexSQL   = `CREATE INDEX IF NOT EXISTS num_idx on ` + TableName + `(num)`
	ReplaceRecordSQL = "REPLACE INTO " + TableName + "(id,pxc_name,pxc_uid,xstore_name,pod_name,version,filepath,filename,num,start_index,hash_val,status,err_message,updated_at,sink_name,sink_type,binlog_checksum,file_mod_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?,?,?,?)"
	SelectSQL        = "SELECT id,pxc_name,pxc_uid,xstore_name,pod_name,version,filepath,filename,num,start_index,hash_val,status,err_message,created_at,updated_at,sink_name,sink_type,binlog_checksum,file_mod_at FROM " + TableName + " WHERE id=?"
	// IdBitNumMask use the low bit num of binlog no. for record id
	IdBitNumMask        = 0x0ffff
	SelectExpiredOneSQL = ""
)

func GetDb(dbFile string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func TryCreateTableOfUploadRecord(db *sql.DB) error {
	_, err := db.Exec(CreateTableSQL)
	if err != nil {
		return err
	}
	_, err = db.Exec(CreateIndexSQL)
	if err != nil {
		return err
	}
	return nil
}

func getRecordId(num int64) int64 {
	return num & IdBitNumMask
}

func AddRecord(db *sql.DB, binlogFile BinlogFile) error {
	id := getRecordId(binlogFile.Num)
	_, err := db.Exec(ReplaceRecordSQL, id, binlogFile.PxcName, binlogFile.PxcUid, binlogFile.XStoreName, binlogFile.PodName, binlogFile.Version, binlogFile.Filepath, binlogFile.Filename, binlogFile.Num, binlogFile.StartIndex, binlogFile.Sha256, binlogFile.Status, binlogFile.ErrMsg, binlogFile.SinkName, binlogFile.SinkType, binlogFile.BinlogChecksum, binlogFile.FileLastModifiedAt)
	if err != nil {
		return err
	}
	return nil
}

func FindRecord(db *sql.DB, num int64) (*BinlogFile, error) {
	binlogFile := BinlogFile{}
	var id int64
	err := db.QueryRow(SelectSQL, num).Scan(&id, &binlogFile.PxcName, &binlogFile.PxcUid, &binlogFile.XStoreName, &binlogFile.PodName, &binlogFile.Version, &binlogFile.Filepath, &binlogFile.Filename, &binlogFile.Num, &binlogFile.StartIndex, &binlogFile.Sha256, &binlogFile.Status, &binlogFile.ErrMsg, &binlogFile.CreatedAt, &binlogFile.UpdatedAt, &binlogFile.SinkName, &binlogFile.SinkType, &binlogFile.BinlogChecksum, &binlogFile.FileLastModifiedAt)
	if err != nil {
		if sql.ErrNoRows == err {
			return nil, nil
		}
		return nil, err
	}
	return &binlogFile, nil
}
