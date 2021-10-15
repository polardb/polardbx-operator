# hsperfdata
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fxin053%2Fhsperfdata.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fxin053%2Fhsperfdata?ref=badge_shield)


This is a golang parser for the newest V2 java HotSpot virtual machine performance data, support all platform theoretically.

## What's this?

It is a log directory created by JVM while running your code. By default it is created inside the tmp folder of the operating system that you are using! This directory is a part of Java Performance counter. And the file in this directory is named by the pid number of java process.

You can disable creating this directory by using `-XX:-UsePerfData` or `-XX:+PerfDisableSharedMem` which is not recommended.

## Used as a library

You can just read the `cli.go` file at the root directory to figure out how to use.


```go
import (
    "fmt"
    "log"

    "github.com/xin053/hsperfdata"
)

// first, you should get the file path string of the hsperfdata file that you want to parser.
// there are several function to get the path, include PerfDataPath(pid string), PerfDataPaths(pids []string), UserPerfDataPaths(user string), CurrentUserPerfDataPaths(), AllPerfDataPaths(), DataPathsByProcessName(processName string)
// the source code is easy to read through, you can just read the hsperfdata.go to figure out how to use them.

entryMap, err := hsperfdata.ReadPerfData(filePath, true)
if err != nil {
    log.Fatal("open fail", err)
}

for _, key := range keys {
    fmt.Printf("%s=%v\n", key, entryMap[key])
}
```

## Used as a command

### build yourself

```shell
export GOPATH=`pwd`
go get -d github.com/xin053/hsperfdata
cd src/github.com/xin053/hsperfdata
go build cli.exe
```

### use the release package

download the executable from the release page, then here you go!

## Want deeper?

### java HotSpot virtual machine performance data structures

```go
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
```

**You can read the source code `hsperfdata.go` to get more information, have a good time!**

**You can open a issue if you have any questions.**

## Attention

The newest java HotSpot virtual machine performance data structures was V2 when I wrote this code, so these data structures may change from release to release, so this parser code only support JVM performance data V2. If there is new version, please open a issue or pull request, thx.

## Reference link

1. http://openjdk.java.net/groups/serviceability/jvmstat/index.html
2. https://github.com/tokuhirom/go-hsperfdata
3. https://github.com/njwhite/telegraf/blob/master/plugins/inputs/hsperfdata/hsperfdata.go
4. https://github.com/twitter/commons/blob/master/src/python/twitter/common/java/perfdata/bin/jammystat.py
5. https://github.com/YaSuenag/hsbeat/blob/master/module/hotspot/hsperfdata/parser.go

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fxin053%2Fhsperfdata.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fxin053%2Fhsperfdata?ref=badge_large)