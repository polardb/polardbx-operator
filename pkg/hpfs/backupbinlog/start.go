package backupbinlog

import (
	. "github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/pkg/errors"
	"io/fs"
	"os"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"time"
)

func StartAllWatchers() {
	go func() {
		logger := zap.New(zap.UseDevMode(true)).WithName("StartAllWatchers")
		for {
			config := GetConfig()
			if config.BackupBinlogConfig != nil {
				for i := 0; i < len(config.BackupBinlogConfig.RootDirectories); i++ {
					//key: namespace, value: pod name list
					pods := map[string][]string{}
					rootDir := config.BackupBinlogConfig.RootDirectories[i]
					if filepath.Base(rootDir) == "xstore" && isDir(rootDir) && filepath.IsAbs(rootDir) {
						namespaceDirectoryEntries, err := os.ReadDir(rootDir)
						if err != nil {
							logger.Error(err, "failed to read dir", "dir", rootDir)
							continue
						}
						for _, dirEntry := range namespaceDirectoryEntries {
							if dirEntry.IsDir() {
								podNameDirectoryEntries, err := os.ReadDir(filepath.Join(rootDir, dirEntry.Name()))
								if err != nil {
									logger.Error(err, "failed to read dir", "dir", rootDir)
									continue
								}
								podNames := make([]string, 0)
								for _, podNameDirEntry := range podNameDirectoryEntries {
									if podNameDirEntry.IsDir() {
										podNames = append(podNames, podNameDirEntry.Name())
									}
								}
								pods[dirEntry.Name()] = podNames
							}
						}
					}
					for k, v := range pods {
						for _, pod := range v {
							watcherWorkDir := filepath.Join(rootDir, k, pod, "log")
							infoFilepath := filepath.Join(watcherWorkDir, InfoFilename)
							_, err := os.Stat(infoFilepath)
							if err != nil {
								if !errors.Is(err, fs.ErrNotExist) {
									logger.Error(err, "cannot stat file", "filepath", infoFilepath)
								}
								continue
							}
							indexFilepath := filepath.Join(watcherWorkDir, IndexFilename)
							_, err = os.Stat(indexFilepath)
							if err != nil {
								logger.Error(err, "cannot stat file", "indexFilepath", indexFilepath)
								continue
							}
							NewWatcher(watcherWorkDir, BeforeUpload, FetchStartIndex, Upload, RecordUpload, AfterUpload).Start()
						}

					}
				}
			}
			time.Sleep(30 * time.Second)
		}
	}()
}

func isDir(filepath string) bool {
	s, err := os.Stat(filepath)
	if err != nil {
		return false
	}
	return s.IsDir()
}
