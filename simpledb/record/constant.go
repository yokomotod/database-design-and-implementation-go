package record

import "fmt"

type Constant struct {
	ival *int32
	sval *string
}

func NewConstantWithInt(ival int32) *Constant {
	return &Constant{ival: &ival}
}

func NewConstantWithString(sval string) *Constant {
	return &Constant{sval: &sval}
}

func (c *Constant) AsInt() int32 {
	return *c.ival
}

func (c *Constant) AsString() string {
	return *c.sval
}

func (c *Constant) Equals(other *Constant) bool {
	if c.ival != nil {
		return *c.ival == *other.ival
	} else {
		return *c.sval == *other.sval
	}
}

func (c *Constant) String() string {
	if c.ival != nil {
		return fmt.Sprintf("%d", *c.ival)
	}
	return *c.sval
}
