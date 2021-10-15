package hsperfdata

import (
	"reflect"
	"testing"
)

func TestPerfDataPath(t *testing.T) {
	type args struct {
		pid string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"1",
			args{"18877"},
			"C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18877",
			false,
		},
		{
			"2",
			args{"123"},
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PerfDataPath(tt.args.pid)
			if (err != nil) != tt.wantErr {
				t.Errorf("PerfDataPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PerfDataPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPerfDataPaths(t *testing.T) {
	type args struct {
		pids []string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			"1",
			args{[]string{"18877", "18878"}},
			map[string]string{
				"18877": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18877",
				"18878": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18878",
			},
			false,
		},
		{
			"2",
			args{[]string{"18877", "123"}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PerfDataPaths(tt.args.pids)
			if (err != nil) != tt.wantErr {
				t.Errorf("PerfDataPaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PerfDataPaths() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserPerfDataPaths(t *testing.T) {
	type args struct {
		user string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			"1",
			args{"0686"},
			map[string]string{
				"18877": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18877",
				"18878": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18878",
			},
			false,
		},
		{
			"2",
			args{"root"},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UserPerfDataPaths(tt.args.user)
			if (err != nil) != tt.wantErr {
				t.Errorf("UserPerfDataPaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UserPerfDataPaths() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurrentUserPerfDataPaths(t *testing.T) {
	tests := []struct {
		name    string
		want    map[string]string
		wantErr bool
	}{
		{
			"1",
			map[string]string{
				"18877": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18877",
				"18878": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18878",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CurrentUserPerfDataPaths()
			if (err != nil) != tt.wantErr {
				t.Errorf("CurrentUserPerfDataPaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CurrentUserPerfDataPaths() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllPerfDataPaths(t *testing.T) {
	tests := []struct {
		name    string
		want    map[string]string
		wantErr bool
	}{
		{
			"1",
			map[string]string{
				"18877": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18877",
				"18878": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\18878",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AllPerfDataPaths()
			if (err != nil) != tt.wantErr {
				t.Errorf("AllPerfDataPaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AllPerfDataPaths() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_removeNull(t *testing.T) {
	type args struct {
		s []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			"1",
			args{[]byte{1, 2, 3, 0}},
			[]byte{1, 2, 3},
		},
		{
			"2",
			args{[]byte{1, 2, 3}},
			[]byte{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removeNull(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("removeNull() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataPathsByProcessName(t *testing.T) {
	type args struct {
		processName string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			"1",
			args{"java"},
			map[string]string{"53516": "C:\\Users\\0686\\AppData\\Local\\Temp\\hsperfdata_0686\\53516"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DataPathsByProcessName(tt.args.processName)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataPathsByProcessName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataPathsByProcessName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadPerfData(t *testing.T) {
	type args struct {
		filepath   string
		parserTime bool
	}
	tests := []struct {
		name    string
		args    args
		want    [4]interface{}
		wantErr bool
	}{
		{
			"2036",
			args{"../test-data/2036", false},
			[4]interface{}{"1.6.0_45", "1.0", int64(20358139), 245},
			false,
		},
		{
			"2956",
			args{"../test-data/2956", false},
			[4]interface{}{"1.7.0-u40-rel_19", "1.7", int64(0), 218},
			false,
		},
		{
			"13223",
			args{"../test-data/13223", true},
			[4]interface{}{"1.6.0_65", "1.0", int64(11955673569), 212},
			false,
		},
		{
			"13984",
			args{"../test-data/13984", true},
			[4]interface{}{"1.8.0_172", "1.8", int64(85266870), 254},
			false,
		},
		{
			"15192",
			args{"../test-data/15192", true},
			[4]interface{}{"1.8.0_92", "1.8", int64(34831378), 253},
			false,
		},
		{
			"21916",
			args{"../test-data/21916", false},
			[4]interface{}{"1.8.0_31", "1.8", int64(0), 253},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadPerfData(tt.args.filepath, tt.args.parserTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadPerfData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got["java.property.java.version"], tt.want[0]) {
				t.Errorf("ReadPerfData() java.property.java.version = %v, want %v", got["java.property.java.version"], tt.want[0])
			}
			if !reflect.DeepEqual(got["java.property.java.vm.specification.version"], tt.want[1]) {
				t.Errorf("ReadPerfData() java.property.java.vm.specification.version = %v, want %v", got["java.property.java.vm.specification.version"], tt.want[1])
			}
			if !reflect.DeepEqual(got["sun.gc.collector.1.time"], tt.want[2]) {
				t.Errorf("ReadPerfData() sun.gc.collector.1.time = %v, want %v", got["sun.gc.collector.1.time"], tt.want[2])
			}
			if !reflect.DeepEqual(len(got), tt.want[3]) {
				t.Errorf("ReadPerfData() entries number = %v, want %v", len(got), tt.want[3])
			}
		})
	}
}
