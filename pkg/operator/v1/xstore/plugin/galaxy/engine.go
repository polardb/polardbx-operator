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

package galaxy

import (
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/reconcilers"
)

func init() {
	plugin.RegisterXStoreReconciler(galaxy.Engine, &reconcilers.GalaxyReconciler{})
	plugin.RegisterXStoreBackupReconciler(galaxy.Engine, &reconcilers.GalaxyBackupReconciler{})
	plugin.RegisterXStoreBackupBinlogReconciler(galaxy.Engine, &reconcilers.GalaxyBackupBinlogReconciler{})
}
