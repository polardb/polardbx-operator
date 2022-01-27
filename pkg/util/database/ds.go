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

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type MySQLDataSource struct {
	Addr     string
	Host     string
	Port     int
	Username string
	Password string
	Database string
	Timeout  time.Duration
	SSL      string
}

func formatParams(params map[string]string) string {
	if len(params) == 0 {
		return ""
	}
	p := make([]string, 0, len(params))
	for k, v := range params {
		p = append(p, k+"="+v)
	}
	return "?" + strings.Join(p, "&")
}

func (ds *MySQLDataSource) datasource() string {
	var datasource string
	if len(ds.Addr) > 0 {
		datasource = fmt.Sprintf("%s:%s@tcp(%s)/%s",
			ds.Username, ds.Password, ds.Addr, ds.Database)
	} else {
		datasource = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			ds.Username, ds.Password, ds.Host, ds.Port, ds.Database)
	}
	params := make(map[string]string)
	if ds.Timeout > 0 {
		second := int(ds.Timeout / time.Second)
		if second > 0 {
			params["timeout"] = strconv.Itoa(second) + "s"
		}
	}
	if len(ds.SSL) > 0 {
		params["tls"] = ds.SSL
	}

	return datasource + formatParams(params)
}

func OpenMySQLDB(ds *MySQLDataSource) (*sql.DB, error) {
	if ds == nil {
		return nil, errors.New("nil open ds")
	}

	return sql.Open("mysql", ds.datasource())
}

func IsMySQLErrCode(err error, code uint16) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}
	return mysqlErr.Number == code
}

func IsMySQLErrTableNotExists(err error) bool {
	return IsMySQLErrCode(err, 1146)
}

// DeferClose for io.Closer which swallows the error
func DeferClose(closer io.Closer) {
	_ = closer.Close()
}
