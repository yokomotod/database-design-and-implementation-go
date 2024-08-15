package query

import "fmt"

var ErrInvalidConstantType = fmt.Errorf("invalid constant type")

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

func (c *Constant) AsInt() (int32, error) {
	if c.ival == nil {
		return 0, ErrInvalidConstantType
	}
	return *c.ival, nil
}

func (c *Constant) AsString() (string, error) {
	if c.sval == nil {
		return "", ErrInvalidConstantType
	}
	return *c.sval, nil
}

func (c *Constant) Equals(other *Constant) bool {
	if c.ival != nil {
		if other.ival == nil {
			return false
		}
		return *c.ival == *other.ival
	} else {
		if other.sval == nil {
			return false
		}
		return *c.sval == *other.sval
	}
}

func (c *Constant) String() string {
	if c.ival != nil {
		return fmt.Sprint(*c.ival)
	}
	return fmt.Sprintf("'%s'", *c.sval)
}
