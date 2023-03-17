package systemtask

type StBalanceResourceStatus struct {
	RebuildTaskName     string `json:"rebuildTaskName,omitempty"`
	RebuildFinish       bool   `json:"rebuildFinish,omitempty"`
	BalanceLeaderFinish bool   `json:"balanceLeaderFinish,omitempty"`
}
