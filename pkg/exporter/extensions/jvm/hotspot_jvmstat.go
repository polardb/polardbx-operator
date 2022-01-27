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

package jvm

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	hotspotHsPerfDataJavaThreadsPrefix = "java.threads"
	hotspotHsPerfDataGcPrefix          = "sun.gc"
	hotspotHsPerfDataCiPrefix          = "sun.ci"
)

type JvmStatsGcCollector struct {
	Invocations   int64  `jvm:"invocations"`
	LastEntryTime int64  `jvm:"lastEntryTime"`
	LastExitTime  int64  `jvm:"lastExitTime"`
	Name          string `jvm:"name"`
	Time          int64  `jvm:"time"`
}

func (c *JvmStatsGcCollector) Parse(p map[string]interface{}) error {
	return parse(p, c)
}

type JvmStatsGcCompressedClassSpace struct {
	Capacity    int64 `jvm:"capacity"`
	MaxCapacity int64 `jvm:"maxCapacity"`
	MinCapacity int64 `jvm:"minCapacity"`
	Used        int64 `jvm:"used"`
}

func (s *JvmStatsGcCompressedClassSpace) Parse(m map[string]interface{}) error {
	return parse(m, s)
}

type JvmStatsGcGenerationSpace struct {
	Capacity     int64  `jvm:"capacity"`
	InitCapacity int64  `jvm:"initCapacity"`
	MaxCapacity  int64  `jvm:"maxCapacity"`
	Name         string `jvm:"name"`
	Used         int64  `jvm:"used"`
}

func (s *JvmStatsGcGenerationSpace) Parse(m map[string]interface{}) error {
	return parse(m, s)
}

type JvmStatsGcGeneration struct {
	AgeTableSize  int32                       `jvm:"ageTableSize"`
	AgeTableBytes []int64                     `jvm:"ageTableBytes,omitempty"`
	Capacity      int64                       `jvm:"capacity"`
	MaxCapacity   int64                       `jvm:"maxCapacity"`
	MinCapacity   int64                       `jvm:"minCapacity"`
	Name          string                      `jvm:"name"`
	Spaces        []JvmStatsGcGenerationSpace `jvm:"spaces"`
}

func (g *JvmStatsGcGeneration) Parse(p map[string]interface{}) error {
	if _, ok := p["agetable.size"]; ok {
		g.AgeTableSize = int32(p["agetable.size"].(int64))
		g.AgeTableBytes = make([]int64, g.AgeTableSize)
		for i := int32(0); i < g.AgeTableSize; i++ {
			g.AgeTableBytes[int(i)] = p[fmt.Sprintf("agetable.bytes.%02d", i)].(int64)
		}
	}
	g.Capacity = p["capacity"].(int64)
	g.MinCapacity = p["minCapacity"].(int64)
	g.MaxCapacity = p["maxCapacity"].(int64)
	g.Name = p["name"].(string)

	spaceSize := int(p["spaces"].(int64))
	g.Spaces = make([]JvmStatsGcGenerationSpace, spaceSize)
	for i := 0; i < spaceSize; i++ {
		err := g.Spaces[i].Parse(filter(p, fmt.Sprintf("space.%d", i)))
		if err != nil {
			return err
		}
	}

	return nil
}

type JvmStatsGcMetaSpace struct {
	Capacity    int64 `jvm:"capacity"`
	MaxCapacity int64 `jvm:"maxCapacity"`
	MinCapacity int64 `jvm:"minCapacity"`
	Used        int64 `jvm:"used"`
}

func (s *JvmStatsGcMetaSpace) Parse(m map[string]interface{}) error {
	return parse(m, s)
}

type JvmStatsGcPolicy struct {
	Collectors           int    `jvm:"collectors"`
	DesiredSurvivorSize  int64  `jvm:"desiredSurvivorSize"`
	Generations          int    `jvm:"generations"`
	MaxTenuringThreshold int    `jvm:"maxTenuringThreshold"`
	Name                 string `jvm:"name"`
	TenuringThreshold    int    `jvm:"tenuringThreshold"`
}

func (p *JvmStatsGcPolicy) Parse(perf map[string]interface{}) error {
	return parse(perf, p)
}

type JvmStatsGcTlab struct {
	Alloc        int `jvm:"alloc"`
	AllocThreads int `jvm:"allocThreads"`
	FastWaste    int `jvm:"fastWaste"`
	Fills        int `jvm:"fills"`
	GcWaste      int `jvm:"gcWaste"`
	MaxFastWaste int `jvm:"maxFastWaste"`
	MaxFills     int `jvm:"maxFills"`
	MaxGcWaste   int `jvm:"maxGcWaste"`
	MaxSlowAlloc int `jvm:"maxSlowAllow"`
	SlowAlloc    int `jvm:"slowAlloc"`
	SlowWaste    int `jvm:"slowWaste"`
}

func (s *JvmStatsGcTlab) Parse(m map[string]interface{}) error {
	return parse(m, s)
}

type JvmStatsGc struct {
	Cause                string                         `jvm:"cause"`
	LastCause            string                         `jvm:"lastCause"`
	Generations          []JvmStatsGcGeneration         `jvm:"generations"`
	Collectors           []JvmStatsGcCollector          `jvm:"collectors"`
	CompressedClassSpace JvmStatsGcCompressedClassSpace `jvm:"compressedclassspace"`
	MetaSpace            JvmStatsGcMetaSpace            `jvm:"metaspace"`
	Tlab                 JvmStatsGcTlab                 `jvm:"tlab"`
	Policy               JvmStatsGcPolicy               `jvm:"policy"`
}

func filter(perf map[string]interface{}, prefix string) map[string]interface{} {
	r := make(map[string]interface{})
	for k, v := range perf {
		if strings.HasPrefix(k, prefix) {
			r[k[len(prefix)+1:]] = v
		}
	}
	return r
}

func (gc *JvmStatsGc) Parse(perf map[string]interface{}) error {
	gc.Cause = perf["cause"].(string)
	gc.LastCause = perf["lastCause"].(string)

	if err := gc.Policy.Parse(filter(perf, "policy")); err != nil {
		return err
	}

	if err := gc.CompressedClassSpace.Parse(filter(perf, "compressedclassspace")); err != nil {
		return err
	}

	if err := gc.MetaSpace.Parse(filter(perf, "metaspace")); err != nil {
		return err
	}

	if err := gc.Tlab.Parse(filter(perf, "tlab")); err != nil {
		return err
	}

	gc.Generations = make([]JvmStatsGcGeneration, 0)
	for i := 0; ; i++ {
		p := filter(perf, fmt.Sprintf("generation.%d", i))
		if len(p) == 0 {
			break
		}
		g := JvmStatsGcGeneration{}
		if err := g.Parse(p); err != nil {
			return err
		}
		gc.Generations = append(gc.Generations, g)
	}

	gc.Collectors = make([]JvmStatsGcCollector, 0)
	for i := 0; ; i++ {
		p := filter(perf, fmt.Sprintf("collector.%d", i))
		if len(p) == 0 {
			break
		}
		c := JvmStatsGcCollector{}
		if err := c.Parse(p); err != nil {
			return err
		}
		gc.Collectors = append(gc.Collectors, c)
	}

	return nil
}

type JvmStatsJavaThreads struct {
	Daemon   int `jvm:"daemon"`
	Live     int `jvm:"live"`
	LivePeak int `jvm:"livePeak"`
	Started  int `jvm:"started"`
}

func (t *JvmStatsJavaThreads) Parse(m map[string]interface{}) error {
	t.Daemon = int(m["daemon"].(int64))
	t.Live = int(m["live"].(int64))
	t.LivePeak = int(m["livePeak"].(int64))
	t.Started = int(m["started"].(int64))

	return nil
}

type JvmStatsCiCompilerThread struct {
	Compiles int    `jvm:"compiles"`
	Method   string `jvm:"method"`
	Time     int64  `jvm:"time"`
	Type     int    `jvm:"type"`
}

func (t *JvmStatsCiCompilerThread) Parse(m map[string]interface{}) error {
	t.Compiles = int(m["compiles"].(int64))
	t.Method = m["method"].(string)
	t.Time = m["time"].(int64)
	t.Type = int(m["type"].(int64))
	return nil
}

type JvmStatsCi struct {
	TotalTime             int64  `jvm:"totalTime"`
	LastFailedMethod      string `jvm:"lastFailedMethod"`
	LastFailedType        int    `jvm:"lastFailedType"`
	LastInvalidatedMethod string `jvm:"lastInvalidatedMethod"`
	LastInvalidatedType   int    `jvm:"lastInvalidatedType"`
	LastMethod            string `jvm:"lastMethod"`
	LastSize              int    `jvm:"lastSize"`
	LastType              int    `jvm:"lastType"`
	NmethodCodeSize       int64  `jvm:"nmethodCodeSize"`
	NmethodSize           int64  `jvm:"nmethodSize"`
	OsrBytes              int64  `jvm:"osrBytes"`
	OsrCompiles           int    `jvm:"osrCompiles"`
	OsrTime               int64  `jvm:"osrTime"`
	StandardBytes         int64  `jvm:"standardBytes"`
	StandardCompiles      int    `jvm:"standardCompiles"`
	StandardTime          int64  `jvm:"standardTime"`
	Threads               int    `jvm:"threads"`
	TotalBailouts         int    `jvm:"totalBailouts"`
	TotalCompiles         int    `jvm:"totalCompiles"`
	TotalInvalidates      int    `jvm:"totalInvalidates"`
	CompilerThreads       []JvmStatsCiCompilerThread
}

func (c *JvmStatsCi) Parse(m map[string]interface{}) error {
	if err := parse(m, c); err != nil {
		return err
	}

	// JDK 11 doesn't report the details of compiler threads.
	if len(filter(m, "compilerThread.0")) > 0 {
		c.CompilerThreads = make([]JvmStatsCiCompilerThread, c.Threads)
		for i := 0; i < c.Threads; i++ {
			err := c.CompilerThreads[i].Parse(filter(m, fmt.Sprintf("compilerThread.%d", i)))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type JvmStatsCls struct {
	LoadedClasses         int `jvm:"loadedClasses"`
	SharedLoadedClasses   int `jvm:"sharedLoadedClasses"`
	SharedUnloadedClasses int `jvm:"sharedUnLoadedClasses"`
	UnloadedClasses       int `jvm:"unloadedClasses"`

	AppClassBytes                     int64 `jvm:"appClassBytes"`
	AppClassLoadCount                 int   `jvm:"appClassLoadCount"`
	AppClassLoadTime                  int64 `jvm:"appClassLoadTime"`
	AppClassLoadTimeSelf              int64 `jvm:"appClassLoadTime.self"`
	ClassInitTime                     int64 `jvm:"classInitTime"`
	ClassInitTimeSelf                 int64 `jvm:"classInitTime.self"`
	ClassLinkedTime                   int64 `jvm:"classLinkedTime"`
	ClassLinkedTimeSelf               int64 `jvm:"classLinkedTime.self"`
	ClassVerifyTime                   int64 `jvm:"classVerifyTime"`
	ClassVerifyTimeSelf               int64 `jvm:"classVerifyTime.self"`
	DefineAppClassTime                int64 `jvm:"defineAppClassTime"`
	DefineAppClassTimeSelf            int64 `jvm:"defineAppClassTime.self"`
	DefineAppClasses                  int   `jvm:"defineAppClasses"`
	InitializedClasses                int   `jvm:"initializedClasses"`
	IsUnsyncloadClassSet              int   `jvm:"isUnsyncloadClassSet"`
	JniDefineClassNoLockCalls         int   `jvm:"jniDefineClassNoLockCalls"`
	JvmDefineClassNoLockCalls         int   `jvm:"jvmDefineClassNoLockCalls"`
	JvmFindLoadedClassNoLockCalls     int   `jvm:"jvmFindLoadedClassNoLockCalls"`
	LinkedClasses                     int   `jvm:"linkedClasses"`
	LoadInstanceClassFailRate         int   `jvm:"loadInstanceClassFailRate"`
	LoadedBytes                       int64 `jvm:"loadedBytes"`
	LookupSysClassTime                int64 `jvm:"lookupSysClassTime"`
	MethodBytes                       int64 `jvm:"methodBytes"`
	NonSystemLoaderLockContentionRate int   `jvm:"nonSystemLoaderLockContentionRate"`
	ParseClassTime                    int64 `jvm:"parseClassTime"`
	ParseClassTimeSelf                int64 `jvm:"parseClassTime.self"`
	SharedClassLoadTime               int64 `jvm:"sharedClassLoadTime"`
	SharedLoadedBytes                 int64 `jvm:"sharedLoadedBytes"`
	SharedUnloadedBytes               int64 `jvm:"sharedUnloadedBytes"`
	SysClassBytes                     int64 `jvm:"sysClassBytes"`
	SysClassLoadTime                  int64 `jvm:"sysClassLoadTime"`
	SystemLoaderLockContentionRate    int   `jvm:"systemLoaderLockContentionRate"`
	Time                              int64 `jvm:"time"`
	UnloadedBytes                     int64 `jvm:"unloadedBytes"`
	UnsafeDefineClassCalls            int   `jvm:"unsafeDefineClassCalls"`
	VerifiedClasses                   int   `jvm:"verifiedClasses"`
}

func (c *JvmStatsCls) ParseJava(m map[string]interface{}) error {
	return parse(m, c)
}

func (c *JvmStatsCls) ParseSun(m map[string]interface{}) error {
	return parse(m, c)
}

type JvmStatsClassLoader struct {
	FindClassTime         int64 `jvm:"findClassTime"`
	FindClasses           int   `jvm:"findClasses"`
	ParentDelegationTime  int64 `jvm:"parentDelegationTime"`
	UrlReadClassBytesTime int64 `jvm:"readClassBytesTime"`
}

func (l *JvmStatsClassLoader) Parse(m map[string]interface{}) error {
	return parse(m, l)
}

type JvmStatsRt struct {
	SyncContentedLockAttempts int    `jvm:"_sync_ContendedLockAttempts"`
	SyncDeflations            int    `jvm:"_sync_Deflations"`
	SyncEmptyNotifications    int    `jvm:"_sync_emptyNotifications"`
	SyncFailedSpins           int    `jvm:"_sync_FailedSpins"`
	SyncFutileWakeups         int    `jvm:"_sync_FutileWakeups"`
	SyncInflations            int    `jvm:"_sync_Inflations"`
	SyncMonExtant             int    `jvm:"_sync_MonExtant"`
	SyncMonInCirculation      int    `jvm:"_sync_MonInCirculation"`
	SyncMonScavenged          int    `jvm:"_sync_MonScavenged"`
	SyncNotifications         int    `jvm:"_sync_Notifications"`
	SyncParks                 int    `jvm:"_sync_Parks"`
	SyncPrivateA              int    `jvm:"_sync_PrivateA"`
	SyncPrivateB              int    `jvm:"_sync_PrivateB"`
	SyncSlowEnter             int    `jvm:"_sync_SlowEnter"`
	SyncSlowExit              int    `jvm:"_sync_SlowExit"`
	SyncSlowNotify            int    `jvm:"_sync_SlowNotify"`
	SyncSlowNotifyAll         int    `jvm:"_sync_SlowNotifyAll"`
	SyncSuccessfulSpins       int    `jvm:"_sync_SuccessfulSpins"`
	ApplicationTime           int64  `jvm:"applicationTime"`
	CreateVmBeginTime         int64  `jvm:"CreateVmBeginTime"`
	CreateVmEndTime           int64  `jvm:"CreateVmEndTime"`
	InternalVersion           string `jvm:"internalVersion"`
	InterruptedBeforeIO       int    `jvm:"interruptedBeforeIO"`
	InterruptedDuringIO       int    `jvm:"interruptedDuringIO"`
	JavaCommand               string `jvm:"javaCommand"`
	JvmCapabilities           string `jvm:"jvmCapabilities"`
	JvmVersion                int    `jvm:"jvmVersion"`
	SafePointSyncTime         int64  `jvm:"safepointSyncTime"`
	SafePointTime             int64  `jvm:"safepointTime"`
	SafePoints                int64  `jvm:"safepoints"`
	ThreadInterruptSignaled   int    `jvm:"threadInterruptSignaled"`
	VmInitDoneTime            int64  `jvm:"vmInitDoneTime"`
}

func (r *JvmStatsRt) Parse(m map[string]interface{}) error {
	return parse(m, r)
}

type JvmStatsOsHrt struct {
	Frequency int64 `jvm:"frequency"`
	Ticks     int64 `jvm:"ticks"`
}

type JvmStatsOs struct {
	Hrt JvmStatsOsHrt `jvm:"hrt"`
}

func (o *JvmStatsOs) Parse(m map[string]interface{}) error {
	return parse(m, o)
}

// JVM stats from hsperfdata, only java.threads and sun.gc, others are left unparsed
type JvmStats struct {
	JavaThreads JvmStatsJavaThreads `jvm:"java.threads"`
	Gc          JvmStatsGc          `jvm:"sun.gc"`
	Ci          JvmStatsCi          `jvm:"sun.ci"`
	Cls         JvmStatsCls         `jvm:"java.cls,sun.cls"`
	ClsLoader   JvmStatsClassLoader `jvm:"sun.classloader"`
	Rt          JvmStatsRt          `jvm:"sun.rt"`
	Os          JvmStatsOs          `jvm:"sun.os"`
}

func (s *JvmStats) Parse(perf map[string]interface{}) error {
	var err error

	err = s.Rt.Parse(filter(perf, "sun.rt"))
	if err != nil {
		return err
	}

	err = s.Os.Parse(filter(perf, "sun.os"))
	if err != nil {
		return err
	}

	err = s.JavaThreads.Parse(filter(perf, hotspotHsPerfDataJavaThreadsPrefix))
	if err != nil {
		return err
	}

	err = s.Gc.Parse(filter(perf, hotspotHsPerfDataGcPrefix))
	if err != nil {
		return err
	}

	s.Ci.TotalTime = perf["java.ci.totalTime"].(int64)
	err = s.Ci.Parse(filter(perf, hotspotHsPerfDataCiPrefix))
	if err != nil {
		return err
	}

	err = s.Cls.ParseJava(filter(perf, "java.cls"))
	if err != nil {
		return err
	}
	err = s.Cls.ParseSun(filter(perf, "sun.cls"))
	if err != nil {
		return err
	}

	err = parse(filter(perf, "sun.urlClassLoader"), &s.ClsLoader)
	if err != nil {
		return err
	}
	err = s.ClsLoader.Parse(filter(perf, "sun.classloader"))
	if err != nil {
		return err
	}

	return nil
}

func parse(m map[string]interface{}, x interface{}) error {
	t := reflect.TypeOf(x).Elem()
	v := reflect.ValueOf(x).Elem()

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		vf := v.Field(i)
		if tag, ok := f.Tag.Lookup("jvm"); ok {
			switch f.Type.Kind() {
			case reflect.Struct:
				if err := parse(filter(m, tag), vf.Addr().Interface()); err != nil {
					return err
				}
			case reflect.String:
				if s, ok := m[tag]; ok {
					v.Field(i).SetString(strings.Trim(s.(string), "\x00"))
				}
			case reflect.Int:
				fallthrough
			case reflect.Int8:
				fallthrough
			case reflect.Int16:
				fallthrough
			case reflect.Int32:
				fallthrough
			case reflect.Int64:
				if s, ok := m[tag]; ok {
					v.Field(i).SetInt(s.(int64))
				}
			}
		}
	}

	return nil
}
