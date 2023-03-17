package backupbinlog

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-logr/logr"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"strconv"
	"sync"
	"time"
)

const (
	Sname             = "pitr_sname"
	heartbeatTableDDL = "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=FALSE)*/\n" +
		"CREATE TABLE IF NOT EXISTS `__cdc_heartbeat__` (\n" +
		"  `id` bigint(20) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
		"  `sname` varchar(10) DEFAULT NULL,\n" +
		"  `gmt_modified` datetime(3) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 broadcast"
	PitrHeartbeatId = 111
)

const (
	EnvHeartbeatPrefix   = "heartbeat_"
	EnvHeartbeatHost     = EnvHeartbeatPrefix + "host"
	EnvHeartbeatPort     = EnvHeartbeatPrefix + "port"
	EnvHeartbeatUser     = EnvHeartbeatPrefix + "user"
	EnvHeartbeatPassword = EnvHeartbeatPrefix + "password"
	EnvHeartbeatInterval = EnvHeartbeatPrefix + "interval"
	EnvMaxRetryCount     = EnvHeartbeatPrefix + "max_retry_count"
)

type HeartBeat struct {
	interval      time.Duration
	host          string
	port          int
	user          string
	pwd           string
	sql           []string
	maxRetryCount int
	ctx           context.Context
	cancelFunc    context.CancelFunc
	lock          sync.Mutex
	logger        logr.Logger
}

func checkValid(values ...string) {
	for _, value := range values {
		if value == "" {
			panic("invalid value")
		}
	}
}

func NewHeatBeat() *HeartBeat {
	host := os.Getenv(EnvHeartbeatHost)
	portStr := os.Getenv(EnvHeartbeatPort)
	user := os.Getenv(EnvHeartbeatUser)
	pwd := os.Getenv(EnvHeartbeatPassword)
	intervalStr := os.Getenv(EnvHeartbeatInterval)
	maxRetryCountStr := os.Getenv(EnvMaxRetryCount)
	logger := zap.New(zap.UseDevMode(true)).WithName("HeatBeat")
	logger.Info("env values", "host", host, "port", portStr, "user", user, "interval", intervalStr, "maxRetryCount", maxRetryCountStr)
	checkValid(host, portStr, user, intervalStr, maxRetryCountStr)
	parsedPort, err := strconv.ParseInt(portStr, 10, 64)
	if err != nil {
		panic("failed to parse port=" + portStr)
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		panic("failed to parse interval=" + intervalStr)
	}
	maxRetryCount, err := strconv.ParseInt(maxRetryCountStr, 10, 64)
	if err != nil {
		panic("failed to parse maxRetryCount=" + maxRetryCountStr)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	return &HeartBeat{
		interval:      interval,
		sql:           []string{"set drds_transaction_policy='TSO'", fmt.Sprintf("replace into `__cdc_heartbeat__`(id,Sname, gmt_modified) values(%d,'%s', now())", PitrHeartbeatId, Sname)},
		host:          host,
		port:          int(parsedPort),
		user:          user,
		pwd:           pwd,
		maxRetryCount: int(maxRetryCount),
		ctx:           ctx,
		cancelFunc:    cancelFunc,
		logger:        logger,
		lock:          sync.Mutex{},
	}
}

func (h *HeartBeat) Cancel() {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.cancelFunc != nil {
		h.logger.Info("cancelled")
		h.cancelFunc()
	}
}

func (h *HeartBeat) connect() *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/__cdc__?timeout=5s&readTimeout=5s", h.user, h.pwd, h.host, h.port))
	if err != nil {
		panic(err)
	}
	return db
}

func (h *HeartBeat) Do() {
	go func() {
		defer h.Cancel()
		db := h.connect()
		defer db.Close()
		lastTime := time.Now().Add(-h.interval)
		_, err := db.ExecContext(h.ctx, heartbeatTableDDL)
		if err != nil {
			panic(err)
		}
		retryCount := 0
		for retryCount < h.maxRetryCount {
			nextDuration := int64(h.interval.Milliseconds()) - (time.Now().UnixMilli() - lastTime.UnixMilli())
			select {
			case <-time.After(time.Duration(nextDuration) * time.Millisecond):
				h.logger.Info("heartbeat")
				lastTime = time.Now()
			case <-h.ctx.Done():
				return
			}
			conn, err := db.Conn(h.ctx)
			if err != nil {
				h.logger.Error(err, "failed to get conn")
				retryCount = retryCount + 1
				return
			}
			tx, err := conn.BeginTx(h.ctx, &sql.TxOptions{})
			if err != nil {
				h.logger.Error(err, "fail to begin tx")
				retryCount = retryCount + 1
				conn.Close()
				continue
			}
			commit := true
			for _, query := range h.sql {
				_, err := tx.ExecContext(h.ctx, query)
				if err != nil {
					h.logger.Error(err, fmt.Sprintf("failed to exec query=%s", query))
					retryCount = retryCount + 1
					tx.Rollback()
					commit = false
					break
				}
			}
			if commit {
				retryCount = 0
				tx.Commit()
			}
			conn.Close()
		}
	}()
}

func (h *HeartBeat) Wait() {
	if h.ctx != nil {
		<-h.ctx.Done()
	}
}
