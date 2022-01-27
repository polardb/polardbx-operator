/*
Copyright 2022 Alibaba Group Holding Limited.

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

package knobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	dbutil "github.com/alibaba/polardbx-operator/pkg/util/database"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
)

type Sync struct {
	client.Reader
	logr.Logger
}

func (s *Sync) openGMS(ctx context.Context, polardbx *polardbxv1.PolarDBXCluster) (*sql.DB, error) {
	gmsName := convention.NewGMSName(polardbx)
	if polardbx.Spec.ShareGMS {
		gmsName = convention.NewDNName(polardbx, 0)
	}
	var xstoreGms polardbxv1.XStore
	if err := s.Get(ctx, types.NamespacedName{
		Namespace: polardbx.Namespace,
		Name:      gmsName,
	}, &xstoreGms); err != nil {
		return nil, err
	}

	var gmsSecret corev1.Secret
	if err := s.Get(ctx, types.NamespacedName{
		Namespace: polardbx.Namespace,
		Name:      xstoreconvention.NewSecretName(&xstoreGms),
	}, &gmsSecret); err != nil {
		return nil, err
	}

	gmsSvcName := xstoreconvention.NewServiceName(&xstoreGms, xstoreconvention.ServiceTypeReadWrite)
	var gmsSvc corev1.Service
	if err := s.Get(ctx, types.NamespacedName{
		Namespace: polardbx.Namespace,
		Name:      gmsSvcName,
	}, &gmsSvc); err != nil {
		return nil, err
	}

	return dbutil.OpenMySQLDB(&dbutil.MySQLDataSource{
		Host:     k8shelper.GetServiceDNSRecordWithSvc(&gmsSvc, false),
		Port:     int(k8shelper.MustGetPortFromService(&gmsSvc, xstoreconvention.PortAccess).Port),
		Username: xstoreconvention.SuperAccount,
		Password: string(gmsSecret.Data[xstoreconvention.SuperAccount]),
		Database: gms.MetaDBName,
		Timeout:  2 * time.Second,
		SSL:      "",
	})
}

func (s *Sync) operateGmsInTransaction(ctx context.Context, polardbx *polardbxv1.PolarDBXCluster, op func(ctx context.Context, tx *sql.Tx) error) error {
	// Open the DB.
	db, err := s.openGMS(ctx, polardbx)
	if err != nil {
		return err
	}
	defer db.Close()

	// Begin a transaction.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}

	// Execute the operation.
	if err := op(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	// Commit.
	return tx.Commit()
}

const clConfigDataIdFormat = "polardbx.inst.config.%s" // {inst_id}

func autoConvertToIntStr(val string) intstr.IntOrString {
	intVal, err := strconv.Atoi(val)
	if err == nil && (intVal <= math.MaxInt32 && intVal >= math.MinInt32) {
		return intstr.FromInt(intVal)
	} else {
		return intstr.FromString(val)
	}
}

//goland:noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlResolve
func loadKnobsWithinTx(ctx context.Context, tx *sql.Tx, knobs *polardbxv1.PolarDBXClusterKnobs) error {
	// Query the current version.
	var version int64
	row := tx.QueryRowContext(ctx, "SELECT op_version FROM config_listener WHERE data_id = ?",
		fmt.Sprintf(clConfigDataIdFormat, knobs.Spec.ClusterName))

	if err := row.Scan(&version); err != nil {
		return err
	}
	knobs.Status.Version = version

	// Load the knobs if force or the recorded version is outdated.
	{
		rs, err := tx.QueryContext(ctx, "SELECT param_key, param_val FROM inst_config WHERE inst_id = ?",
			knobs.Spec.ClusterName)
		if err != nil {
			return err
		}
		defer rs.Close()

		// Scan the rows.
		knobs.Spec.Knobs = make(map[string]intstr.IntOrString)
		var key, val string
		for rs.Next() {
			if err := rs.Scan(&key, &val); err != nil {
				return err
			}

			knobs.Spec.Knobs[key] = autoConvertToIntStr(val)
		}
	}

	// Update the update time.
	knobs.Status.LastUpdated = metav1.Now()
	knobs.Status.Size = int32(len(knobs.Spec.Knobs))

	return nil
}

func (s *Sync) MutateOnCreate(ctx context.Context, obj runtime.Object) error {
	knobs := obj.(*polardbxv1.PolarDBXClusterKnobs)

	if knobs.Status.LastUpdated.IsZero() {
		knobs.Status.Version = -1
		knobs.Status.LastUpdated = metav1.Now()
	}

	// Do nothing if there's no cluster name specified.
	if len(knobs.Spec.ClusterName) == 0 {
		return nil
	}

	var polardbxcluster polardbxv1.PolarDBXCluster
	if err := s.Get(ctx, types.NamespacedName{
		Namespace: knobs.Namespace,
		Name:      knobs.Spec.ClusterName,
	}, &polardbxcluster); err != nil {
		if apierrors.IsNotFound(err) {
			s.Logger.Info("Specified polardbxcluster not exists.", "polardbxcluster", knobs.Spec.ClusterName)
		} else {
			s.Logger.Error(err, "Failed to get polardbxcluster.")
		}
		return nil
	}

	// Load the initial knobs.
	if err := s.operateGmsInTransaction(ctx, &polardbxcluster,
		func(ctx context.Context, tx *sql.Tx) error {
			return loadKnobsWithinTx(ctx, tx, knobs)
		},
	); err != nil {
		// Failed to load, setup with the default.
		s.Logger.Error(err, "Failed to load knobs within transaction, set with default.")

		knobs.Status.Version = -1
		knobs.Spec.Knobs = nil
	}

	return nil
}

type operation string

const (
	opAdd    operation = "Add"
	opDelete operation = "Delete"
	opUpdate operation = "Update"
)

type action struct {
	Key    string
	Op     operation
	Val    string
	OldVal string
}

func newUpdateAction(key string, oldVal, newVal fmt.Stringer) action {
	return action{Key: key, Op: opUpdate, Val: newVal.String(), OldVal: oldVal.String()}
}

func newAddAction(key string, val fmt.Stringer) action {
	return action{Key: key, Op: opAdd, Val: val.String()}
}

func newDeleteAction(key string) action {
	return action{Key: key, Op: opDelete}
}

//goland:noinspection SqlDialectInspection,SqlResolve,SqlNoDataSourceInspection
const (
	insertOrUpdateConfigSQL = "INSERT INTO inst_config (inst_id, param_key, param_val) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE param_val = VALUES(param_val), gmt_modified = NOW()"
	deleteConfigSQL         = "DELETE FROM inst_config WHERE inst_id = ? AND param_key = ?"
)

func (act *action) IsDelete() bool {
	return act.Op == opDelete
}

func (act *action) Apply(knobs map[string]intstr.IntOrString) bool {
	switch act.Op {
	case opAdd:
		val, exists := knobs[act.Key]
		if exists {
			// Convert to update if already exists and value not equals.
			if val.String() != act.Val {
				knobs[act.Key] = autoConvertToIntStr(act.Val)
				return true
			}
		} else {
			knobs[act.Key] = autoConvertToIntStr(act.Val)
			return true
		}
	case opUpdate:
		val, exists := knobs[act.Key]
		if !exists {
			// Convert to add if not exists.
			knobs[act.Key] = autoConvertToIntStr(act.Val)
			return true
		} else {
			if val.String() != act.Val {
				knobs[act.Key] = autoConvertToIntStr(act.Val)
				return true
			}
		}
	case opDelete:
		_, exists := knobs[act.Key]
		if exists {
			delete(knobs, act.Key)
			return true
		}
	default:
		panic("unknown operation: " + act.Op)
	}
	return false
}

func diffKnobs(oldKnobs, newKnobs map[string]intstr.IntOrString) []action {
	diff := make([]action, 0)

	for key, val := range newKnobs {
		if oldVal, exists := oldKnobs[key]; exists {
			// Update on value change.
			if val.String() != oldVal.String() {
				diff = append(diff, newUpdateAction(key, &oldVal, &val))
			}
		} else {
			diff = append(diff, newAddAction(key, &val))
		}
	}

	for key := range oldKnobs {
		if _, exists := newKnobs[key]; !exists {
			diff = append(diff, newDeleteAction(key))
		}
	}

	return diff
}

//goland:noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlResolve
func (s *Sync) MutateOnUpdate(ctx context.Context, oldObj, newObj runtime.Object) error {
	knobs := newObj.(*polardbxv1.PolarDBXClusterKnobs)

	// Do nothing if there's no cluster name specified.
	if len(knobs.Spec.ClusterName) == 0 {
		return nil
	}

	oldKnobs := oldObj.(*polardbxv1.PolarDBXClusterKnobs)

	// Calculate the difference.
	knobsDiff := diffKnobs(oldKnobs.Spec.Knobs, knobs.Spec.Knobs)

	var polardbxcluster polardbxv1.PolarDBXCluster
	if err := s.Get(ctx, types.NamespacedName{
		Namespace: knobs.Namespace,
		Name:      knobs.Spec.ClusterName,
	}, &polardbxcluster); err != nil {
		if apierrors.IsNotFound(err) {
			s.Logger.Info("Specified polardbxcluster not exists, reset.", "polardbxcluster", knobs.Spec.ClusterName)
			// Reset all if not reset.
			if knobs.Status.Version >= 0 {
				knobs.Spec.Knobs = nil
				knobs.Status.LastUpdated = metav1.Now()
				knobs.Status.Version = -1
				knobs.Status.Size = 0
			} else {
				oldKnobs.DeepCopyInto(knobs)
			}
		} else {
			s.Logger.Error(err, "Failed to get polardbxcluster.")
			// Reject the update.
			return err
		}

		return nil
	}

	if len(knobsDiff) == 0 {
		// If difference is zero, just do another load.
		return s.operateGmsInTransaction(ctx, &polardbxcluster,
			func(ctx context.Context, tx *sql.Tx) error {
				return loadKnobsWithinTx(ctx, tx, knobs)
			},
		)
	} else {
		return s.operateGmsInTransaction(ctx, &polardbxcluster,
			func(ctx context.Context, tx *sql.Tx) error {
				// Load the knobs in GMS.
				if err := loadKnobsWithinTx(ctx, tx, knobs); err != nil {
					return err
				}

				// Apply the diff.
				for _, act := range knobsDiff {
					if act.Apply(knobs.Spec.Knobs) {
						if act.IsDelete() {
							_, err := tx.ExecContext(ctx, deleteConfigSQL, knobs.Spec.ClusterName, act.Key)
							if err != nil {
								return err
							}
						} else {
							_, err := tx.ExecContext(ctx, insertOrUpdateConfigSQL, knobs.Spec.ClusterName, act.Key, act.Val)
							if err != nil {
								return err
							}
						}
					}
				}

				// Update the version.
				rs, err := tx.ExecContext(ctx, "UPDATE config_listener SET op_version = op_version + 1 WHERE data_id = ? AND op_version = ?",
					fmt.Sprintf(clConfigDataIdFormat, knobs.Spec.ClusterName), knobs.Status.Version)
				if err != nil {
					return err
				}
				if rowsAffected, err := rs.RowsAffected(); err != nil {
					return err
				} else if rowsAffected == 0 {
					return errors.New("knobs has been updated, abort")
				}

				knobs.Status.Version++
				knobs.Status.LastUpdated = metav1.Now()
				knobs.Status.Size = int32(len(knobs.Spec.Knobs))

				return nil
			},
		)
	}
}

func NewSync(r client.Reader, logger logr.Logger) extension.CustomMutator {
	return &Sync{Reader: r, Logger: logger}
}
