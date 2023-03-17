package backupbinlog

import (
	"testing"
	"time"
)

func TestWatcher(t *testing.T) {
	NewWatcher("/Users/busu/tmp/litewatchertest", BeforeUpload, FetchStartIndex, Upload, RecordUpload, AfterUpload).Start()
	time.Sleep(1 * time.Hour)
}
