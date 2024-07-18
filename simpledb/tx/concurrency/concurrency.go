package concurrency

import "simpledb/file"

var lockTable = newLockTable()

type Manager struct {
	locks map[file.BlockID]string
}

func New() *Manager {
	return &Manager{
		locks: make(map[file.BlockID]string),
	}
}

func (m *Manager) SLock(blockID file.BlockID) error {
	if m.locks[blockID] != "" {
		return nil
	}

	err := lockTable.SLock(blockID)
	if err != nil {
		return err
	}
	m.locks[blockID] = "S"
	return nil
}

func (m *Manager) XLock(blockID file.BlockID) error {
	if m.HasXLock(blockID) {
		return nil
	}

	// XLockを取る前にSlockを取る
	err := m.SLock(blockID)
	if err != nil {
		return err
	}

	err = lockTable.XLock(blockID)
	if err != nil {
		return err
	}
	m.locks[blockID] = "X"
	return nil
}

func (m *Manager) Release() {
	for blockID := range m.locks {
		lockTable.Unlock(blockID)
	}
	clear(m.locks)
}

func (m *Manager) HasXLock(blockID file.BlockID) bool {
	locktype := m.locks[blockID]
	return locktype == "X"
}
