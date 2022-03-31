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

package plugin

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	"github.com/alibaba/polardbx-operator/pkg/probe/xstore_ext"
	"github.com/alibaba/polardbx-operator/pkg/util/network"
)

//goland:noinspection SqlDialectInspection,SqlNoDataSourceInspection
func init() {
	xstore_ext.RegisterXStoreExt("galaxy",
		newXStoreExt(func(ctx context.Context, host string, db *sql.DB) error {
			// Check for private protocol port.
			row := db.QueryRowContext(ctx, "select @@galaxyx_port")
			var polarxPort uint16
			if err := row.Scan(&polarxPort); err != nil {
				return err
			}

			if err := network.TestTcpConnectivity(ctx, host, polarxPort); err != nil {
				return err
			}

			if featuregate.EnableGalaxyClusterMode.Enabled() {
				// Check the read write status if leader.
				row = db.QueryRowContext(ctx, "SELECT ROLE, SERVER_READY_FOR_RW FROM information_schema.ALISQL_CLUSTER_LOCAL")
				var role, rwReady string
				if err := row.Scan(&role, &rwReady); err != nil {
					return err
				}
				if role == "Leader" {
					if strings.ToLower(rwReady) != "yes" {
						return errors.New("leader not ready")
					}
				}
			}
			return nil
		}))
}
