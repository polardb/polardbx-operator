package backupbinlog

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
)

const (
	ShowConsensusLogSQL          = "show consensus logs"
	ShowVersion                  = "select @@version"
	PurgeConsensusLogSQLFormat57 = "purge local consensus_log before %d"
	PurgeConsensusLogSQLFormat80 = "call dbms_consensus.local_purge_log(%d)"
)

func FindMysqlSockByLogDir(logDir string, rootDirs []string) (string, error) {
	var relativeLogDir string
	for _, rootDir := range rootDirs {
		if strings.HasPrefix(logDir, rootDir) {
			relativeLogDir = logDir[len(rootDir):]
			if filepath.IsAbs(relativeLogDir) {
				relativeLogDir = relativeLogDir[1:]
			}
		}
	}
	relativeMysqlSockPath := filepath.Join(filepath.Dir(relativeLogDir), "run", "mysql.sock")
	for _, rootDir := range rootDirs {
		absMysqlSockPath := filepath.Join(rootDir, relativeMysqlSockPath)
		_, err := os.Stat(absMysqlSockPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", err
		}
		return absMysqlSockPath, nil
	}
	return "", os.ErrNotExist
}

func GetMysqlDb(mysqlSockPath string) (*sql.DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@unix(%s)/", mysqlSockPath))
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Purge(db *sql.DB, beforeIndex uint64) error {
	//check version
	var version string
	err := db.QueryRow(ShowVersion).Scan(&version)
	if err != nil {
		return err
	}
	if strings.HasPrefix(version, "5") {
		_, err = db.Exec(fmt.Sprintf(PurgeConsensusLogSQLFormat57, beforeIndex))
	} else {
		_, err = db.Exec(fmt.Sprintf(PurgeConsensusLogSQLFormat80, beforeIndex))
	}
	if err != nil {
		return err
	}
	return nil
}

type ConsensusLogRow struct {
	LogName       string
	FileSize      uint64
	StartLogIndex uint64
}

func ShowConsensusLogs(db *sql.DB) ([]ConsensusLogRow, error) {
	rows, err := db.Query(ShowConsensusLogSQL)
	if err != nil {
		return nil, err
	}
	result := make([]ConsensusLogRow, 0)
	defer rows.Close()
	for {
		if !rows.Next() {
			break
		}
		row := ConsensusLogRow{}
		err := rows.Scan(&row.LogName, &row.FileSize, &row.StartLogIndex)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, nil
}
