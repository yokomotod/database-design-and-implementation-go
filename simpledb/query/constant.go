package query

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

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

func (c *Constant) CompareTo(other *Constant) (int, error) {
	if c.ival != nil && other.ival != nil {
		if *c.ival > *other.ival {
			return 1, nil
		} else if *c.ival < *other.ival {
			return -1, nil
		}
		return 0, nil
	}
	if c.sval != nil && other.sval != nil {
		return strings.Compare(*c.sval, *other.sval), nil
	}
	return 0, ErrInvalidConstantType
}

func (c *Constant) HashCode() int32 {
	h := fnv.New32()
	if c.ival != nil {
		h.Write([]byte(strconv.Itoa(int(*c.ival))))
		return int32(h.Sum32())
	}
	if c.sval != nil {
		h.Write([]byte(*c.sval))
		return int32(h.Sum32())
	}
	return 0
}

func (c *Constant) String() string {
	if c.ival != nil {
		return fmt.Sprint(*c.ival)
	}
	return fmt.Sprintf("'%s'", *c.sval)
}

func (c *Constant) AnyValue() any {
	if c.ival != nil {
		return *c.ival
	}
	if c.sval != nil {
		return *c.sval
	}
	return nil
}
