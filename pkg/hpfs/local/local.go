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

package local

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/dghubble/trie"
	"github.com/minio/minio/pkg/disk"
)

type LocalFileService interface {
	CreateFile(path string, perm os.FileMode) error
	CreateDirectory(path string, perm os.FileMode) error
	CreateDirectories(path string, perm os.FileMode) error
	CreateSymlink(target, link string) error
	ListDirectory(path string) ([]os.FileInfo, error)
	IsExists(path string) (bool, error)
	IsFile(path string) (bool, error)
	IsDirectory(path string) (bool, error)
	IsSymbolicLink(path string) (bool, error)
	OpenFile(path string, truncate bool) (*os.File, error)
	OpenFileReadOnly(path string) (*os.File, error)
	WriteFile(path string, content []byte) error
	DeleteFile(path string) error
	DeleteDirectory(path string, recursively bool) error
	TruncateFile(path string, size int64) error
	RenameFile(src, dest string) error
	Chown(path string, uid, gid int) error
	Lchown(path string, uid, gid int) error
	Stat(path string) (os.FileInfo, error)
	Lstat(path string) (os.FileInfo, error)
	GetDiskUsage(path string) (uint64, error)
	GetDiskInfo(path string) (disk.Info, error)
	GetMajorMinorNumber(path string, physical bool) (uint32, uint32, error)
}

type BlockDevice struct {
	Name          string
	Dev           uint32
	Major         uint32
	Minor         uint32
	SysDevicePath string
	ParentDev     uint32
}

type localFileService struct {
	pathTestFunc func(string) bool

	blockDevices map[uint32]BlockDevice
}

func (l *localFileService) loadBlockDevices() error {
	l.blockDevices = make(map[uint32]BlockDevice)

	// Read /sys/block
	const sysBlockPath = "/sys/block"
	dir, err := os.ReadDir(sysBlockPath)
	if err != nil {
		return err
	}

	// name to device number
	phyBlockDev := make(map[string]uint64, 0)
	// /sys/devices/... path to device number
	phyBlockDevPath := make(map[string]uint64, 0)
	for _, f := range dir {
		if strings.HasPrefix(f.Name(), "loop") {
			continue
		}
		major, minor, err := l.GetMajorMinorNumber(path.Join("/dev", f.Name()), false)
		if err != nil {
			return err
		}
		// Under /sys/devices
		sysDevicePath, err := os.Readlink(path.Join(sysBlockPath, f.Name()))
		if err != nil {
			return err
		}
		sysDevicePath, err = filepath.Abs(path.Join(sysBlockPath, sysDevicePath))
		if err != nil {
			return err
		}
		dev := unix.Mkdev(major, minor)
		phyBlockDev[f.Name()] = dev
		phyBlockDevPath[sysDevicePath] = dev
		l.blockDevices[uint32(dev)] = BlockDevice{
			Name:          f.Name(),
			Dev:           uint32(dev),
			Major:         major,
			Minor:         minor,
			ParentDev:     0,
			SysDevicePath: sysDevicePath,
		}
	}

	// Read /sys/class/block
	const sysClassBlockPath = "/sys/class/block"
	dir, err = os.ReadDir(sysClassBlockPath)
	if err != nil {
		return err
	}

	for _, f := range dir {
		if strings.HasPrefix(f.Name(), "loop") {
			continue
		}
		if _, ok := phyBlockDev[f.Name()]; ok {
			continue
		}
		major, minor, err := l.GetMajorMinorNumber(path.Join("/dev", f.Name()), false)
		if err != nil {
			return err
		}
		sysDevicePath, err := os.Readlink(path.Join(sysClassBlockPath, f.Name()))
		if err != nil {
			return err
		}
		sysDevicePath, err = filepath.Abs(path.Join(sysClassBlockPath, sysDevicePath))
		if err != nil {
			return err
		}
		parentDevicePath := path.Dir(sysDevicePath)
		parentDev, ok := phyBlockDevPath[parentDevicePath]
		if !ok {
			fmt.Println("WARN: block device " + f.Name() + " doesn't found parent physical block device!")
			continue
		}

		dev := unix.Mkdev(major, minor)
		l.blockDevices[uint32(dev)] = BlockDevice{
			Name:          f.Name(),
			Dev:           uint32(dev),
			Major:         major,
			Minor:         minor,
			SysDevicePath: sysDevicePath,
			ParentDev:     uint32(parentDev),
		}
	}

	// Print all
	fmt.Println("Found block devices:")
	for dev, blk := range l.blockDevices {
		fmt.Printf("  %s %d %d:%d %d\n", blk.Name, dev, blk.Major, blk.Minor, blk.ParentDev)
	}

	return nil
}

func (l *localFileService) getPhysicalBlockDevice(dev uint32) *BlockDevice {
	blk, ok := l.blockDevices[dev]
	if !ok {
		return nil
	}
	for blk.ParentDev > 0 {
		blk, ok = l.blockDevices[blk.ParentDev]
	}
	return &blk
}

func (l *localFileService) GetMajorMinorNumber(path string, physical bool) (uint32, uint32, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	sysStat, ok := stat.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("unable to determine the device major and minor numbers")
	}

	// ModeDir | ModeSymlink | ModeNamedPipe | ModeSocket | ModeDevice | ModeCharDevice | ModeIrregular
	switch stat.Mode().Type() {
	case os.ModeDevice, os.ModeCharDevice:
		return unix.Major(uint64(sysStat.Rdev)), unix.Minor(uint64(sysStat.Rdev)), nil
	case 0, os.ModeDir, os.ModeSymlink:
		if !physical {
			return unix.Major(uint64(sysStat.Dev)), unix.Minor(uint64(sysStat.Dev)), nil
		} else {
			blk := l.getPhysicalBlockDevice(uint32(sysStat.Dev))
			if blk == nil {
				return 0, 0, fmt.Errorf("physical block device not found")
			}
			return blk.Major, blk.Minor, nil
		}
	default:
		return 0, 0, fmt.Errorf("unsupported file mode: %d", stat.Mode())
	}
}

func (l *localFileService) checkPaths(paths ...*string) error {
	var err error
	for _, p := range paths {
		if *p, err = l.Abs(*p); err != nil {
			return err
		}

		if !l.isOperationOnFileOrDirectoryPermitted(*p) {
			return os.ErrPermission
		}
	}

	return nil
}

func (l *localFileService) ListDirectory(path string) (fileInfo []os.FileInfo, err error) {
	if err := l.checkPaths(&path); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			return nil, err
		}
		fileInfo = append(fileInfo, info)
	}

	return fileInfo, nil
}

func (l *localFileService) isOperationOnFileOrDirectoryPermitted(path string) bool {
	return l.pathTestFunc(path)
}

func (l *localFileService) IsExists(path string) (bool, error) {
	if err := l.checkPaths(&path); err != nil {
		return false, err
	}

	if _, err := l.Lstat(path); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (l *localFileService) IsFile(path string) (bool, error) {
	if err := l.checkPaths(&path); err != nil {
		return false, err
	}

	fi, err := l.Lstat(path)
	if err != nil {
		return false, err
	}
	return fi.Mode().IsRegular(), nil
}

func (l *localFileService) IsDirectory(path string) (bool, error) {
	if err := l.checkPaths(&path); err != nil {
		return false, err
	}

	fi, err := l.Lstat(path)
	if err != nil {
		return false, err
	}
	return fi.Mode().IsDir(), nil
}

func (l *localFileService) IsSymbolicLink(path string) (bool, error) {
	if err := l.checkPaths(&path); err != nil {
		return false, err
	}

	fi, err := l.Lstat(path)
	if err != nil {
		return false, err
	}
	return fi.Mode()&os.ModeSymlink != 0, nil
}

func (l *localFileService) Abs(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	return filepath.Abs(path)
}

func (l *localFileService) CreateFile(path string, perm os.FileMode) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer f.Close()

	return nil
}

func (l *localFileService) CreateDirectory(path string, perm os.FileMode) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	return os.Mkdir(path, perm)
}

func (l *localFileService) CreateSymlink(target, link string) error {
	if err := l.checkPaths(&target, &link); err != nil {
		return err
	}

	return os.Symlink(target, link)
}

func (l *localFileService) CreateDirectories(path string, perm os.FileMode) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	return os.MkdirAll(path, perm)
}

func (l *localFileService) OpenFile(path string, truncate bool) (*os.File, error) {
	if err := l.checkPaths(&path); err != nil {
		return nil, err
	}

	flag := os.O_RDWR | os.O_CREATE
	if truncate {
		flag |= os.O_TRUNC
	}

	return os.OpenFile(path, flag, 0664)
}

func (l *localFileService) OpenFileReadOnly(path string) (*os.File, error) {
	if err := l.checkPaths(&path); err != nil {
		return nil, err
	}

	return os.OpenFile(path, os.O_RDONLY, 0)
}

func (l *localFileService) WriteFile(path string, content []byte) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.Write(content); err != nil {
		return err
	}
	return nil
}

func (l *localFileService) DeleteFile(path string) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	if !l.isOperationOnFileOrDirectoryPermitted(path) {
		return os.ErrPermission
	}

	return os.Remove(path)
}

func (l *localFileService) RenameFile(src, dest string) error {
	if err := l.checkPaths(&src, &dest); err != nil {
		return err
	}

	return os.Rename(src, dest)
}

func (l *localFileService) DeleteDirectory(path string, recursively bool) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	if !recursively {
		return os.Remove(path)
	} else {
		return os.RemoveAll(path)
	}
}

func (l *localFileService) TruncateFile(path string, size int64) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	return os.Truncate(path, size)
}

func (l *localFileService) Chown(path string, uid, gid int) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	return os.Chown(path, uid, gid)
}

func (l *localFileService) Lchown(path string, uid, gid int) error {
	if err := l.checkPaths(&path); err != nil {
		return err
	}

	return os.Lchown(path, uid, gid)
}

func (l *localFileService) Stat(path string) (os.FileInfo, error) {
	if err := l.checkPaths(&path); err != nil {
		return nil, err
	}

	return os.Stat(path)
}

func (l *localFileService) Lstat(path string) (os.FileInfo, error) {
	if err := l.checkPaths(&path); err != nil {
		return nil, err
	}

	return os.Lstat(path)
}

func (l *localFileService) dirSize(path string) (uint64, error) {
	var size uint64 = 0
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		size += uint64(info.Size())
		return nil
	})
	return size, err
}

func (l *localFileService) GetDiskUsage(path string) (uint64, error) {
	if err := l.checkPaths(&path); err != nil {
		return 0, err
	}

	fi, err := l.Stat(path)
	if err != nil {
		return 0, err
	}

	if fi.IsDir() {
		return l.dirSize(path)
	} else {
		return uint64(fi.Size()), nil
	}
}

func (l *localFileService) GetDiskInfo(path string) (disk.Info, error) {
	di, err := disk.GetInfo(path)
	if err != nil {
		return di, err
	}

	// Remove trailing 0s in fs type.
	di.FSType = strings.TrimRight(di.FSType, "\x00")
	return di, nil
}

func newPathTrie(paths []string) (*trie.PathTrie, error) {
	t := trie.NewPathTrie()
	for _, p := range paths {
		// Put absolute path into the trie.
		if path.IsAbs(p) {
			t.Put(p, 1)
		} else {
			absPath, err := filepath.Abs(p)
			if err != nil {
				return nil, err
			}
			t.Put(absPath, 1)
		}
	}
	return t, nil
}

func newPathTestFunc(pathTrie *trie.PathTrie) func(s string) bool {
	var errTrieFastAbort = errors.New("trie: fast abort")

	return func(path string) bool {
		err := pathTrie.WalkPath(path, func(key string, value interface{}) error {
			if value == 1 {
				return errTrieFastAbort
			}
			return nil
		})
		return err == errTrieFastAbort
	}
}

func newLocalFileService(limitedPaths []string) (*localFileService, error) {
	// Set the default root path if not specified.
	if limitedPaths == nil || len(limitedPaths) == 0 {
		limitedPaths = []string{"/"}
	}
	pathTrie, err := newPathTrie(limitedPaths)
	if err != nil {
		return nil, err
	}

	lfs := &localFileService{
		pathTestFunc: newPathTestFunc(pathTrie),
	}
	if runtime.GOOS == "linux" {
		fmt.Println("Loading block devices...")
		if err := lfs.loadBlockDevices(); err != nil {
			return nil, err
		}
	}
	return lfs, err
}

func NewLocalFileService(limitedPaths []string) (LocalFileService, error) {
	return newLocalFileService(limitedPaths)
}
