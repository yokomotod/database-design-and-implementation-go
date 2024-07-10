package concurrency

import "simpledb/file"

type Manager struct {
	lockTable *LockTable
	locks     map[file.BlockID]string
}

func New() *Manager {
	return &Manager{
		lockTable: newLockTable(),
		locks:     make(map[file.BlockID]string),
	}
}

func (m *Manager) SLock(blockID file.BlockID) {
	if m.locks[blockID] != "" {
		return
	}

	m.lockTable.SLock(blockID)
	m.locks[blockID] = "S"
}

func (m *Manager) XLock(blockID file.BlockID) {
	if m.locks[blockID] != "" {
		return
	}

	m.lockTable.XLock(blockID)
	m.locks[blockID] = "X"
}

func (m *Manager) Release() {
	for blockID := range m.locks {
		m.lockTable.Unlock(blockID)
	}
	clear(m.locks)
}

func (m *Manager) HasXLock(blockID file.BlockID) bool {
	locktype := m.locks[blockID]
	return locktype == "X"
}
