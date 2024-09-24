package importbackupset

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func GetDownloadUrl(addresses ...string) string {
	var val atomic.Value
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	for _, address := range addresses {
		if address == "" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			parsedUrl, err := url.Parse(address)
			if err != nil {
				panic(err)
			}
			hostPort := parsedUrl.Host
			if !strings.Contains(hostPort, ":") {
				hostPort = hostPort + ":443"
			}
			conn, err := net.DialTimeout("tcp", hostPort, 2*time.Second)
			if err != nil {
				fmt.Println("dial err for " + hostPort)
				return
			}
			defer conn.Close()
			val.CompareAndSwap(nil, address)
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(10 * time.Millisecond):
			if val.Load() != nil {
				return val.Load().(string)
			}
			break
		}
	}
}
