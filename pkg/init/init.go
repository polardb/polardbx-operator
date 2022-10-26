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

package init

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	dbutil "github.com/alibaba/polardbx-operator/pkg/util/database"
	"github.com/alibaba/polardbx-operator/pkg/util/network"
)

const metadbDatabaseName = "polardbx_meta_db"

type Env struct {
	InstanceID        string `json:"instance_id"`
	InstanceType      string `json:"instance_type"`
	PrimaryInstanceID string `json:"primary_instance_id"`
	PodId             string `json:"pod_id"`
	LocalIP           string `json:"local_ip"`
	ServerPort        int    `json:"server_port"`
	HtapPort          int    `json:"htap_port"`
	MgrPort           int    `json:"mgr_port"`
	MppPort           int    `json:"mpp_port"`
	CpuCore           int    `json:"cpu_core"`
	MemSize           int64  `json:"mem_size"`
	MetaDBHost        string `json:"metadb_host"`
	MetaDBPort        int    `json:"metadb_port"`
	MetaDBUser        string `json:"metadb_user"`
	MetaDBEncPasswd   string `json:"metadb_enc_passwd"`
	EncKey            string `json:"enc_key"`
}

func (env *Env) lookupInt64Env(key string) (int64, error) {
	if strVal, exists := os.LookupEnv(key); !exists {
		return 0, fmt.Errorf("env '%s' not found", key)
	} else {
		val, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("env '%s' is invalid: %s", key, err.Error())
		}
		return val, nil
	}
}

func (env *Env) lookupIntEnv(key string) (int, error) {
	val, err := env.lookupInt64Env(key)
	if err != nil {
		return 0, err
	}
	return int(val), err
}

func (env *Env) Load() error {
	var exists bool
	var err error

	if env.PodId, exists = os.LookupEnv("POD_ID"); !exists {
		return errors.New("env 'POD_ID' not found")
	}

	if localIp, exists := os.LookupEnv("POD_IP"); exists {
		env.LocalIP = localIp
	} else {
		ip, err := network.GetLocalIP()
		if err != nil {
			return err
		}
		env.LocalIP = ip.String()
	}
	fmt.Println("Local IP: " + env.LocalIP)

	if metaDbAddr, exists := os.LookupEnv("metaDbAddr"); !exists {
		return errors.New("env 'metaDbAddr' not found")
	} else {
		hostPort := strings.SplitN(metaDbAddr, ":", 2)
		if len(hostPort) < 2 {
			return errors.New("env 'metaDbAddr' is invalid: invalid addr, syntax host:port")
		}
		env.MetaDBHost = hostPort[0]
		if env.MetaDBPort, err = strconv.Atoi(hostPort[1]); err != nil {
			return errors.New("env 'metaDbAddr' is invalid: invalid port, " + err.Error())
		}
	}

	if env.MetaDBUser, exists = os.LookupEnv("metaDbUser"); !exists {
		return errors.New("env 'metaDbUser' not found")
	}

	if encPasswd, exists := os.LookupEnv("metaDbPasswd"); !exists {
		return errors.New("env 'metaDbPasswd' not found")
	} else {
		env.MetaDBEncPasswd = encPasswd
	}

	if encKey, exists := os.LookupEnv("dnPasswordKey"); !exists {
		return errors.New("env 'dnPasswordKey' not found")
	} else {
		env.EncKey = encKey
	}

	if env.InstanceID, exists = os.LookupEnv("instanceId"); !exists {
		return errors.New("env 'instanceId' not found")
	}

	if env.InstanceType, exists = os.LookupEnv("instanceType"); !exists {
		return errors.New("env 'instanceType' not found")
	}

	if env.PrimaryInstanceID, exists = os.LookupEnv("primaryInstanceId"); !exists {
		env.PrimaryInstanceID = env.InstanceID
	}

	if env.ServerPort, err = env.lookupIntEnv("serverPort"); err != nil {
		return err
	}

	if env.HtapPort, err = env.lookupIntEnv("htapPort"); err != nil {
		return err
	}

	if env.MgrPort, err = env.lookupIntEnv("mgrPort"); err != nil {
		return err
	}

	if env.MppPort, err = env.lookupIntEnv("mppPort"); err != nil {
		return err
	}

	if env.CpuCore, err = env.lookupIntEnv("cpuCore"); err != nil {
		return err
	}

	if env.MemSize, err = env.lookupInt64Env("memSize"); err != nil {
		return err
	}

	return nil
}

func MustMarshalJSON(obj interface{}) string {
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func Do() {
	fmt.Println("Begin initializing...")

	fmt.Println("Loading from environment...")
	env := &Env{}
	if err := env.Load(); err != nil {
		fmt.Println("Error when loading from environment: " + err.Error())
		os.Exit(-1)
	}

	fmt.Println("Environment loaded: " + MustMarshalJSON(env))

	fmt.Println("Connecting to metadb...")
	passwd, err := security.MustNewPasswordCipher(env.EncKey).Decrypt(env.MetaDBEncPasswd)
	if err != nil {
		fmt.Println("Error when decrypting password: " + err.Error())
		os.Exit(-1)
	}
	db, err := dbutil.OpenMySQLDB(&dbutil.MySQLDataSource{
		Host:     env.MetaDBHost,
		Port:     env.MetaDBPort,
		Username: env.MetaDBUser,
		Password: passwd,
		Database: metadbDatabaseName,
		Timeout:  10 * time.Second,
	})
	if err != nil {
		fmt.Println("Error when connecting to metadb: " + err.Error())
		os.Exit(-1)
	}
	defer db.Close()
	fmt.Println("Connected to metadb!")

	fmt.Printf("Try self-registration, register %s:%d to metadb...\n", env.LocalIP, env.ServerPort)
	stmt := fmt.Sprintf(`INSERT IGNORE INTO server_info 
		(inst_id, inst_type, ip, port, htap_port, mgr_port, mpp_port, status, cpu_core, mem_size, extras) 
		VALUES ('%s', '%s', '%s', %d, %d, %d, %d, %d, %d, %d, '%s')`, env.InstanceID, env.InstanceType, env.LocalIP,
		env.ServerPort, env.HtapPort, env.MgrPort, env.MppPort, 0, env.CpuCore, env.MemSize, env.PodId)
	notifyStmt := fmt.Sprintf(`UPDATE config_listener SET op_version = op_version + 1 WHERE data_id = 'polardbx.server.info.%s'`, env.InstanceID)

	notifyStmtForPrimary := fmt.Sprintf(`UPDATE config_listener SET op_version = op_version + 1 WHERE data_id = 'polardbx.server.info.%s'`, env.PrimaryInstanceID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fmt.Println("Error when begin transaction: " + err.Error())
		os.Exit(-1)
	}

	if _, err := tx.ExecContext(ctx, stmt); err != nil {
		fmt.Println("Error when self-registering: " + err.Error())
		os.Exit(-1)
	}

	if _, err := tx.ExecContext(ctx, notifyStmt); err != nil {
		fmt.Println("Error when updating config listener: " + err.Error())
		os.Exit(-1)
	}

	if _, err := tx.ExecContext(ctx, notifyStmtForPrimary); err != nil {
		fmt.Println("Error when updating config listener: " + err.Error())
		os.Exit(-1)
	}

	if err = tx.Commit(); err != nil {
		fmt.Println("Failed to commit transaction: " + err.Error())
		os.Exit(-1)
	}

	fmt.Println("Registered!")

	fmt.Println("Successfully initialized!")
}
