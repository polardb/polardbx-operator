package cpusetbind

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"strconv"
	"strings"
)

const DirectoryPath = "/host/proc"
const CmdLineFilepathFormat = "/host/proc/%d/cmdline"
const CommFilepathFormat = "/host/proc/%d/comm"
const EnvFilepathFormat = "/host/proc/%d/environ"
const PodCmdParamPrefix = "-Dpod.id="
const NumaMapsFilepath = "/proc/%d/numa_maps"
const NumaNodeNumAttrPattern = `N(\d+)=(\d+)`

var logger = zap.New(zap.UseDevMode(true)).WithName("proc")

func GetFirstPageNumaNodeNumByPid(pid int) (string, error) {
	numaMapsFilepath := fmt.Sprintf(NumaMapsFilepath, pid)
	f, err := os.OpenFile(numaMapsFilepath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return "", err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		numaNode := GetNumaNode(line)
		if numaNode != "" {
			return numaNode, nil
		}
	}
	return "", nil
}

func GetNumaNode(line string) string {
	attributes := strings.Split(line, " ")
	if len(attributes) < 2 {
		return ""
	}
	r := regexp.MustCompile(NumaNodeNumAttrPattern)
	for i := len(attributes) - 1; i >= 0; i-- {
		matchResult := r.FindStringSubmatch(attributes[i])
		if len(matchResult) == 3 {
			return matchResult[1]
		}
	}
	return ""
}

func GetCmdLineByComm(comm string) ([]string, error) {
	pids, err := GetPids()
	if err != nil {
		return nil, err
	}
	for _, pid := range pids {
		commFilepath := fmt.Sprintf(CommFilepathFormat, pid)
		commBytes, err := os.ReadFile(commFilepath)
		if err != nil {
			// ignore err
			logger.Error(err, "failed to read file", "filepath", commFilepath)
		}
		if strings.TrimSpace(comm) == strings.TrimSpace(string(commBytes)) {
			//os.ReadFile()
			cmdLineFilepath := fmt.Sprintf(CmdLineFilepathFormat, pid)
			cmdLineBytes, err := os.ReadFile(cmdLineFilepath)
			if err == nil {
				cmdLines := bytes.Split(cmdLineBytes, []byte{0})
				return convertBytes2String(cmdLines), nil
			}
		}
	}
	return nil, errors.New("NotFound")
}

func GetCmdLineByPid(pid int) ([]string, error) {
	cmdLineFilepath := fmt.Sprintf(CmdLineFilepathFormat, pid)
	return readProcLineFile(cmdLineFilepath)
}

func GetEnvByPid(pid int) ([]string, error) {
	envFilepath := fmt.Sprintf(EnvFilepathFormat, pid)
	return readProcLineFile(envFilepath)
}

func readProcLineFile(filepath string) ([]string, error) {
	lineBytes, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(lineBytes, []byte{0})
	return convertBytes2String(lines), nil
}

func convertBytes2String(bytesArray [][]byte) []string {
	result := make([]string, 0, len(bytesArray))
	for _, bytes := range bytesArray {
		result = append(result, string(bytes))
	}
	return result
}

func GetPids() ([]int, error) {
	entries, err := os.ReadDir(DirectoryPath)
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0)
	for _, dirEntry := range entries {
		//dirName = dirEntry.Name()
		if dirEntry.IsDir() {
			pid, err := strconv.Atoi(dirEntry.Name())
			if err == nil {
				pids = append(pids, pid)
			}
		}
	}
	return pids, nil
}
