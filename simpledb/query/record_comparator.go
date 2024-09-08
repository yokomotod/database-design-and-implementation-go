package query

import (
	"fmt"
)

type RecordComparator struct {
	Fields []string
}

func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{Fields: fields}
}

func (rc *RecordComparator) Compare(scan1, scan2 Scan) (int, error) {
	for _, fieldName := range rc.Fields {
		val1, err := scan1.GetVal(fieldName)
		if err != nil {
			return 0, fmt.Errorf("scan1.GetVal(%s): %w", fieldName, err)
		}
		val2, err := scan2.GetVal(fieldName)
		if err != nil {
			return 0, fmt.Errorf("scan2.GetVal(%s): %w", fieldName, err)
		}

		if !val1.Equals(val2) {
			return val1.CompareTo(val2)
		}
	}

	return 0, nil
}

func (rc *RecordComparator) CompareMap(vals1, vals2 map[string]*Constant) (int, error) {
	for _, fieldName := range rc.Fields {
		val1, ok := vals1[fieldName]
		if !ok {
			return 0, fmt.Errorf("vals1 has no key %s", fieldName)
		}
		val2, ok := vals2[fieldName]
		if !ok {
			return 0, fmt.Errorf("vals2 has no key %s", fieldName)
		}

		if !val1.Equals(val2) {
			return strings.Compare(val1.String(), val2.String()), nil
		}
	}

	return 0, nil
}
