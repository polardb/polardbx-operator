package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/discovery"
	. "github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/google/uuid"
	"io"
	"os"
	"strings"
)

/*
*
Examples:
1. uploadRemote

2. uploadLocal

3. downloadLocal

4. downloadRemote

5. uploadOss

6. downOss
*/
var (
	host             string //filestream server host
	port             int    //filestream port
	action           string
	instanceId       string
	filename         string
	redirectAddr     string
	filepath         string
	retentionTime    string
	destNodeName     string
	hostInfoFilePath string
	stream           string
	sink             string
	ossBufferSize    string
)

var activeHosts map[string]discovery.HostInfo

func init() {
	flag.StringVar(&host, "host", "", "Host of filestream server")
	flag.IntVar(&port, "port", -1, "Port of filestream server")
	flag.StringVar(&action, "meta.action", "", "Field Action of metadata")
	flag.StringVar(&instanceId, "meta.instanceId", "", "Field InstanceId of metadata")
	flag.StringVar(&filename, "meta.filename", "", "Field Filename of metadata")
	flag.StringVar(&redirectAddr, "meta.redirectAddr", "", "The format is host:port, for example 127.0.0.1:999")
	flag.StringVar(&filepath, "meta.filepath", "", "Field Filepath of metadata")
	flag.StringVar(&retentionTime, "meta.retentionTime", "", "Field RetentionTime of metadata")
	flag.StringVar(&sink, "meta.sink", "", "Sink name of metadata")
	flag.StringVar(&ossBufferSize, "meta.ossBufferSize", "", "oss buffer size of metadata")
	flag.StringVar(&destNodeName, "destNodeName", "", "The name of the destination node name")
	flag.StringVar(&hostInfoFilePath, "hostInfoFilePath", "/tools/xstore/hdfs-nodes.json", "The file path of the host info file")
	flag.StringVar(&stream, "stream", "", "The file stream type such as tar, default: empty string")
	flag.Parse()

	activeHosts = getHostInfo()
	currentNodeName := os.Getenv("NODE_NAME")
	hostInfo, ok := activeHosts[currentNodeName]
	if !ok {
		printErrAndExit(errors.New("node "+currentNodeName+" does not exists"), ActionMetadata{})
	}
	if host == "" {
		host = hostInfo.HpfsHost
	}
	if port == -1 {
		port = int(hostInfo.FsPort)
	}
	if destNodeName == currentNodeName || strings.Trim(redirectAddr, " ") == fmt.Sprintf("%s:%d", host, port) {
		if strings.HasSuffix(action, "Remote") {
			action = strings.ReplaceAll(action, "Remote", "Local")
		}
	}
	if destNodeName != "" {
		destHostInfo, ok := activeHosts[destNodeName]
		if !ok {
			printErrAndExit(errors.New(fmt.Sprintf("dest node %s does not exist", destNodeName)), ActionMetadata{})
		}
		redirectAddr = fmt.Sprintf("%s:%d", destHostInfo.HpfsHost, destHostInfo.FsPort)
	}
}

func getHostInfo() map[string]discovery.HostInfo {
	fInfo, err := os.Stat(hostInfoFilePath)
	if err != nil || fInfo.Size() == 0 {
		printErrAndExit(errors.New("no file "+hostInfoFilePath), ActionMetadata{})
	}
	f, err := os.OpenFile(hostInfoFilePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		printErrAndExit(err, ActionMetadata{})
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		printErrAndExit(err, ActionMetadata{})
	}
	result := map[string]discovery.HostInfo{}
	if err = json.Unmarshal(data, &result); err != nil {
		printErrAndExit(err, ActionMetadata{})
	}
	return result
}

func main() {
	client := NewFileClient(host, port, nil)
	metadata := ActionMetadata{
		Action:        Action(action),
		InstanceId:    instanceId,
		Filename:      filename,
		RedirectAddr:  redirectAddr,
		Filepath:      filepath,
		Stream:        stream,
		RequestId:     uuid.New().String(),
		Sink:          sink,
		OssBufferSize: ossBufferSize,
	}
	if strings.HasPrefix(strings.ToLower(action), "upload") {
		len, err := client.Upload(os.Stdin, metadata)
		if err != nil {
			printErrAndExit(err, metadata)
		}
		err = client.Check(metadata)
		if err != nil {
			printErrAndExit(err, metadata)
		}
		fmt.Print(len)
	} else if strings.HasPrefix(strings.ToLower(action), "download") {
		_, err := client.Download(os.Stdout, metadata)
		if err != nil {
			printErrAndExit(err, metadata)
		}
	} else if strings.HasPrefix(strings.ToLower(action), "list") {
		_, err := client.List(os.Stdout, metadata)
		if err != nil {
			printErrAndExit(err, metadata)
		}
	} else {
		printErrAndExit(errors.New("invalid action"), metadata)
	}
}

func printErrAndExit(err error, metadata ActionMetadata) {
	metadataJsonBytes, _ := json.Marshal(metadata)
	fmt.Fprintf(os.Stdout, "Failed,   error %v metadata %v host %s port %d", err, string(metadataJsonBytes), host, port)
	os.Exit(1)
}
