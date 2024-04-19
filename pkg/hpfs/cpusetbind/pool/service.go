package pool

type CpuSetPool interface {
	MarkAllocated(cpus []int)
	TestAvailable(cpus []int) bool
	AllocateProcessors(processors int) ([]int, error)
	SetPreferredNumaNode(numaNode int)
	ConsiderNuma() bool
	Name() string
}
