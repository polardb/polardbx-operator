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
	"strings"
)

type ScanOpt struct {
	CaseInsensitive bool
}

func Scan(rows *sql.Rows, dest map[string]interface{}, opt ScanOpt) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	columnMap := make(map[string]int)
	for i, col := range columns {
		if opt.CaseInsensitive {
			col = strings.ToLower(col)
		}
		_, ok := columnMap[col]
		if ok {
			return errors.New("duplicate column found: " + col + ", consider disable case-insensitive")
		}
		columnMap[col] = i
	}

	destVec := make([]interface{}, len(columns))
	for col, dest := range dest {
		if opt.CaseInsensitive {
			col = strings.ToLower(col)
		}
		idx, ok := columnMap[col]
		if !ok {
			return errors.New("column not found: " + col)
		}
		if destVec[idx] != nil {
			return errors.New("duplicate column dest: " + col)
		}
		destVec[idx] = dest
	}
	for i := range destVec {
		if destVec[i] == nil {
			destVec[i] = &sql.RawBytes{}
		}
	}

	return rows.Scan(destVec...)
}
