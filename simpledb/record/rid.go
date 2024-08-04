package record

import "fmt"

type RID struct {
	blockNum int32
	slot     int32
}

func NewRID(blockNum, slot int32) *RID {
	return &RID{blockNum, slot}
}

func (r *RID) BlockNumber() int32 {
	return r.blockNum
}

func (r *RID) Slot() int32 {
	return r.slot
}

func (r *RID) Equals(other *RID) bool {
	return r.blockNum == other.blockNum && r.slot == other.slot
}

func (r *RID) String() string {
	return fmt.Sprintf("[%d, %d]", r.blockNum, r.slot)
}
