// Package hsperfdata create data: 2018-06-12
// attention: The newest java HotSpot virtual machine performance data structures was V2 when I wrote this code, so these data structures may
// change from release to release, so this parser code only support JVM performance data V2. If there is new version, please open a issue
// or pull request, thx.
package hsperfdata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// type code http://openjdk.java.net/groups/serviceability/jvmstat/sun/jvmstat/perfdata/monitor/v2_0/TypeCode.html
// source code https://github.com/dmlloyd/openjdk/blob/jdk/jdk/src/jdk.internal.jvmstat/share/classes/sun/jvmstat/perfdata/monitor/v2_0/TypeCode.java
// actually, we use only 'B' and 'J' in HotSpot performance data V2
const (
	tBoolean = 'Z'
	tChar    = 'C'
	tFloat   = 'F'
	tDouble  = 'D'
	tByte    = 'B'
	tShort   = 'S'
	tInt     = 'I'
	tLong    = 'J'
	tObject  = 'L'
	tArray   = '['
	tVoid    = 'V'
)

// variability attribute https://github.com/dmlloyd/openjdk/blob/jdk/jdk/src/jdk.internal.jvmstat/share/classes/sun/jvmstat/monitor/Variability.java
const (
	vInvalid = iota
	vConstant
	vMonotonic
	vVariable
)

// unit of measure attribute https://github.com/dmlloyd/openjdk/blob/jdk/jdk/src/jdk.internal.jvmstat/share/classes/sun/jvmstat/monitor/Units.java
const (
	uInvalid = iota
	uNone
	uBytes
	uTicks
	uEvents
	uString
	uHertz
)

// perfdataHeader http://openjdk.java.net/groups/serviceability/jvmstat/sun/jvmstat/perfdata/monitor/AbstractPerfDataBufferPrologue.html
// source code https://github.com/dmlloyd/openjdk/blob/jdk/jdk/src/jdk.internal.jvmstat/share/classes/sun/jvmstat/perfdata/monitor/AbstractPerfDataBufferPrologue.java
type perfdataHeader struct {
	Magic     uint32 // magic number - 0xcafec0c0
	ByteOrder byte   // big_endian == 0, little_endian == 1
	Major     byte   // major version numbers
	Minor     byte   // minor version numbers
	// ReservedByte byte   // used as Accessible flag at performance data V2
}

// prologue http://openjdk.java.net/groups/serviceability/jvmstat/sun/jvmstat/perfdata/monitor/v2_0/PerfDataBufferPrologue.html
// source code https://github.com/dmlloyd/openjdk/blob/jdk/jdk/src/jdk.internal.jvmstat/share/classes/sun/jvmstat/perfdata/monitor/v2_0/PerfDataBufferPrologue.java
type bufferPrologueV2 struct {
	Accessible   byte  // Accessible flag at performance data V2
	Used         int32 // number of PerfData memory bytes used
	Overflow     int32 // number of bytes of overflow
	ModTimestamp int64 // time stamp of the last structural modification
	EntryOffset  int32 // offset of the first PerfDataEntry
	NumEntries   int32 // number of allocated PerfData entries
}

// entryHeader http://openjdk.java.net/groups/serviceability/jvmstat/sun/jvmstat/perfdata/monitor/v2_0/PerfDataBuffer.html
// source code https://github.com/dmlloyd/openjdk/blob/jdk/jdk/src/jdk.internal.jvmstat/share/classes/sun/jvmstat/perfdata/monitor/v2_0/PerfDataBuffer.java
type entryHeader struct {
	EntryLength  int32 // entry length in bytes
	NameOffset   int32 // offset to entry name, relative to start of entry
	VectorLength int32 // length of the vector. If 0, then scalar.
	DataType     byte  // JNI field descriptor type
	Flags        byte  // miscellaneous attribute flags 0x01 - supported
	DataUnits    byte  // unit of measure attribute
	DataVar      byte  // variability attribute
	DataOffset   int32 // offset to data item, relative to start of entry.
}

// PerfDataPath returns the path to the hsperfdata file for a given pid,
// it searches in all hsperfdata user directories (using a glob mattern),
// pid are assumed to be unique regardless of username.
func PerfDataPath(pid string) (string, error) {
	perfGlob := filepath.Join(os.TempDir(), "hsperfdata_*", pid)
	perfFiles, err := filepath.Glob(perfGlob)
	if err != nil {
		return "", err
	}

	if len(perfFiles) < 1 {
		return "", fmt.Errorf("No hsperfdata file found for pid: %s", pid)
	}

	if len(perfFiles) > 1 {
		return "", fmt.Errorf("More than one hsperfdata file found for pid: %s, this is not normal", pid)
	}

	filePath := perfFiles[0]

	return filePath, nil
}

// PerfDataPaths returns a map(pid: dataPath) by the given pids
func PerfDataPaths(pids []string) (map[string]string, error) {
	filePaths := make(map[string]string)
	for _, pid := range pids {
		filePath, err := PerfDataPath(pid)
		if err != nil {
			return nil, err
		}
		filePaths[pid] = filePath
	}

	return filePaths, nil
}

// UserPerfDataPaths returns all the java process hsperfdata path belongs to
// the given user
func UserPerfDataPaths(user string) (map[string]string, error) {
	dir := filepath.Join(os.TempDir(), "hsperfdata_"+user)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	filePaths := make(map[string]string)
	for _, f := range files {
		if _, err := strconv.Atoi(f.Name()); err == nil {
			filePaths[f.Name()] = filepath.Join(dir, f.Name())
		}
	}

	return filePaths, nil
}

// CurrentUserPerfDataPaths returns all the java process hsperfdata path belongs to
// the current user
func CurrentUserPerfDataPaths() (map[string]string, error) {
	var user string
	if runtime.GOOS == "windows" {
		user = os.Getenv("USERNAME")
	} else {
		user = os.Getenv("USER")
	}
	if user == "" {
		return nil, fmt.Errorf("error: Environment variable USER not set, can not find current user")
	}

	return UserPerfDataPaths(user)
}

// AllPerfDataPaths returns all users' hsperfdata path
func AllPerfDataPaths() (map[string]string, error) {
	dirsGlob := filepath.Join(os.TempDir(), "hsperfdata_*", "*")
	paths, err := filepath.Glob(dirsGlob)
	if err != nil {
		return nil, err
	}

	filePaths := make(map[string]string)
	for _, path := range paths {
		pid := filepath.Base(path)
		filePaths[pid] = path
	}

	return filePaths, nil
}

// DataPathsByProcessName get data paths by the given process name
func DataPathsByProcessName(processName string) (map[string]string, error) {
	var out []byte
	var err error
	if runtime.GOOS == "windows" {
		out, err = exec.Command("cmd", "/C", "tasklist /NH|findstr /i "+processName).Output()
	} else {
		out, err = exec.Command("sh", "-c", "ps -ef|grep -i "+processName+"|grep -v grep").Output()
	}
	if err != nil || len(out) == 0 {
		return nil, errors.New(processName + " is not running.")
	}

	filePaths := make(map[string]string)
	buffer := bytes.NewBuffer(out)
	for {
		line, err := buffer.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		pid := fields[1]
		filePath, err := PerfDataPath(pid)
		if err != nil {
			continue
		}
		filePaths[pid] = filePath
	}
	return filePaths, nil
}

// removeNull remove trailing '\x00' byte of s
func removeNull(s []byte) []byte {
	if i := bytes.IndexByte(s, '\x00'); i > 0 {
		return s[:i]
	}
	return s
}

// ReadPerfData parser hotspot performance data, and return a map represented entries' name and value,
// "ticks" are the unit of measurement of time in the Hotspot JVM.
// when the parserTime is true, tick time value will be parsered to a normal nanoseconds using
// the "sun.os.hrt.frequency" key in the hsperfdata.
func ReadPerfData(filepath string, parserTime bool) (map[string]interface{}, error) {
	// read a snapshot into memory
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		if runtime.GOOS == "windows" {
			// on windows, can not read the hsperfdata file when the java process is running, so we copy
			// to a new file first, and read the file, then delete it.
			_, err = exec.Command("powershell", "-c", "cp", filepath, filepath+"_").Output()
			if err != nil {
				return nil, err
			}
			data, err = ioutil.ReadFile(filepath + "_")
			if err != nil {
				return nil, err
			}
			_ = os.Remove(filepath + "_")
		} else {
			return nil, err
		}
	}
	buffer := bytes.NewReader(data)

	header := perfdataHeader{}
	{
		err = binary.Read(buffer, binary.BigEndian, &header)
		if err != nil {
			return nil, err
		}
		if header.Magic != 0xcafec0c0 {
			return nil, fmt.Errorf("illegal magic %v", header.Magic)
		}
		// only support 2.0 perf data buffers.
		if !(header.Major == 2 && header.Minor == 0) {
			return nil, fmt.Errorf("unsupported version %v.%v", header.Major, header.Minor)
		}
	}

	// file endian
	var endian binary.ByteOrder
	if header.ByteOrder == 0 {
		endian = binary.BigEndian
	} else {
		endian = binary.LittleEndian
	}

	prologue := bufferPrologueV2{}
	{
		err = binary.Read(buffer, endian, &prologue)
		if prologue.Accessible != 1 {
			return nil, fmt.Errorf("not accessible %v", prologue.Accessible)
		}
	}

	entryMap := make(map[string]interface{})
	startOffset := prologue.EntryOffset
	var frequency time.Duration // the length of a tick
	unconvertedTickFields := make(map[string]int64)

	for i := int32(0); i < prologue.NumEntries; i++ {
		entry := entryHeader{}
		buffer.Seek(int64(startOffset), 0)
		err = binary.Read(buffer, endian, &entry)
		if err != nil {
			return nil, fmt.Errorf("Cannot read binary: %v", err)
		}

		nameStart := int(startOffset) + int(entry.NameOffset)
		nameEnd := bytes.Index(data[nameStart:], []byte{'\x00'})
		if nameEnd < 0 {
			return nil, fmt.Errorf("invalid binary: %v", err)
		}
		name := string(data[nameStart : int(nameStart)+nameEnd])

		dataStart := startOffset + entry.DataOffset
		if entry.VectorLength == 0 {
			if entry.DataType != tLong {
				return nil, fmt.Errorf("Unexpected monitor type: %v", entry.DataType)
			}
			buffer.Seek(int64(dataStart), 0)
			value := int64(0)
			err = binary.Read(buffer, endian, &value)
			if err != nil {
				return nil, err
			}
			if parserTime && entry.DataUnits == uTicks {
				unconvertedTickFields[name] = value
			}
			if name == "sun.os.hrt.frequency" {
				frequency = time.Duration(time.Second.Nanoseconds() / value)
			}
			entryMap[name] = value
		} else {
			if entry.DataType != tByte || entry.DataUnits != uString || (entry.DataVar != vConstant && entry.DataVar != vVariable) {
				return nil, fmt.Errorf("Unexpected vector monitor: DataType:%c,DataUnits:%v,DataVar:%v", entry.DataType, entry.DataUnits, entry.DataVar)
			}

			value := string(removeNull(data[dataStart : dataStart+entry.VectorLength]))
			entryMap[name] = value
		}

		startOffset += entry.EntryLength
	}

	for name, value := range unconvertedTickFields {
		value *= frequency.Nanoseconds()
		entryMap[name] = value
	}
	return entryMap, nil
}
