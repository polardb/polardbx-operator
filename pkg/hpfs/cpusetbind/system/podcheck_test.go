package cpusetbind

import (
	. "github.com/onsi/gomega"
	"testing"
)

func TestCheckEngineNoMatch(t *testing.T) {
	g := NewGomegaWithT(t)
	lines := []string{
		"/usr/bin/java",
		"-Djava.net.preferIPv4Stack=true",
		"-Djava.util.prefs.systemRoot=/home/admin/.java",
		"-Djava.util.prefs.userRoot=/home/admin/.java/.userPrefs",
		"-Xms256m",
		"-Xmx256m",
		"-Djava.util.prefs.systemRoot=/home/admin/.java",
		"com.aliyun.polardbx.binlog.daemon.DaemonBootStrap",
	}
	cdcItems := BuildCdcCheckerItems()
	ProcessItems(lines, cdcItems)
	g.Expect(GetPodName(cdcItems)).Should(BeEquivalentTo(""))
	cnItems := BuildCnCheckerItems()
	ProcessItems(lines, cnItems)
	g.Expect(GetPodName(cnItems)).Should(BeEquivalentTo(""))
	dnItems := BuildDnCheckerItems()
	ProcessItems(lines, dnItems)
	g.Expect(GetPodName(dnItems)).Should(BeEquivalentTo(""))
}

func TestCheckEngineMatchCdc(t *testing.T) {
	g := NewGomegaWithT(t)
	lines := []string{
		"/usr/bin/java",
		"-Djava.net.preferIPv4Stack=true",
		"-Djava.util.prefs.systemRoot=/home/admin/.java",
		"-Djava.util.prefs.userRoot=/home/admin/.java/.userPrefs",
		"-Xms256m",
		"-Xmx256m",
		"-Djava.util.prefs.systemRoot=/home/admin/.java",
		"com.aliyun.polardbx.binlog.daemon.DaemonBootStrap",
		"POD_ID=polardb-x-study-wh2t-cdc-default-c848d747-wcfqk",
	}
	cdcItems := BuildCdcCheckerItems()
	ProcessItems(lines, cdcItems)
	g.Expect(GetPodName(cdcItems)).Should(BeEquivalentTo("polardb-x-study-wh2t-cdc-default-c848d747-wcfqk"))
	cnItems := BuildCnCheckerItems()
	ProcessItems(lines, cnItems)
	g.Expect(GetPodName(cnItems)).Should(BeEquivalentTo(""))
	dnItems := BuildDnCheckerItems()
	ProcessItems(lines, dnItems)
	g.Expect(GetPodName(dnItems)).Should(BeEquivalentTo(""))
}

func TestCheckEngineMatchCn(t *testing.T) {
	g := NewGomegaWithT(t)
	lines := []string{
		"/opt/alibaba/ajdk11/bin/java",
		"-server",
		"-Xms2g",
		"-Xmx2g",
		"-XX:+UnlockExperimentalVMOptions",
		"-Dtxc.vip.skip=true",
		"-Djava.net.preferIPv4Stack=true",
		"-XX:-UseBiasedLocking",
		"-Dfile.encoding=UTF-8",
		"-Dio.netty.transport.noNative=true",
		"com.alibaba.polardbx.server.TddlLauncher",
		"-Dpod.id=polardb-x-study-wh2t-cn-default-6546c9bfcf-2npxg",
	}
	cdcItems := BuildCdcCheckerItems()
	ProcessItems(lines, cdcItems)
	g.Expect(GetPodName(cdcItems)).Should(BeEquivalentTo(""))
	cnItems := BuildCnCheckerItems()
	ProcessItems(lines, cnItems)
	g.Expect(GetPodName(cnItems)).Should(BeEquivalentTo("polardb-x-study-wh2t-cn-default-6546c9bfcf-2npxg"))
	dnItems := BuildDnCheckerItems()
	ProcessItems(lines, dnItems)
	g.Expect(GetPodName(dnItems)).Should(BeEquivalentTo(""))
}

func TestCheckEngineMatchDn(t *testing.T) {
	g := NewGomegaWithT(t)
	lines := []string{
		"/opt/galaxy_engine/bin/mysqld",
		"--defaults-file=/data/mysql/conf/my.cnf",
		"--basedir=/opt/galaxy_engine",
		"--datadir=/data/mysql/data",
		"--plugin-dir=/opt/galaxy_engine/lib/plugin",
		"--user=mysql",
		"--loose-pod-name=polardb-x-study-wh2t-dn-0-cand-1",
		"--log-error=/data/mysql/log/alert.log",
		"--open-files-limit=615350",
		"--pid-file=/data/mysql/run/mysql.pid",
		"--socket=/data/mysql/run/mysql.sock",
		"--port=3306",
	}
	cdcItems := BuildCdcCheckerItems()
	ProcessItems(lines, cdcItems)
	g.Expect(GetPodName(cdcItems)).Should(BeEquivalentTo(""))
	cnItems := BuildCnCheckerItems()
	ProcessItems(lines, cnItems)
	g.Expect(GetPodName(cnItems)).Should(BeEquivalentTo(""))
	dnItems := BuildDnCheckerItems()
	ProcessItems(lines, dnItems)
	g.Expect(GetPodName(dnItems)).Should(BeEquivalentTo("polardb-x-study-wh2t-dn-0-cand-1"))
}
