package query

import (
	"fmt"
	"simpledb/record"
	"simpledb/util/logger"
)

var _ Scan = (*MultibufferSortScan)(nil)

type MultibufferSortScan struct {
	logger *logger.Logger

	scans         []UpdateScan
	hasMores      []bool
	currentScan   UpdateScan
	comp          *RecordComparator
	savedPosition []*record.RID
}

func NewMultibufferSortScan(runs []*TempTable, comp *RecordComparator) (*MultibufferSortScan, error) {
	scans := make([]UpdateScan, 0, len(runs))
	hasMores := make([]bool, 0, len(runs))
	for i, run := range runs {
		s, err := run.Open()
		if err != nil {
			return nil, fmt.Errorf("runs[%d].Open: %w", i, err)
		}
		scans = append(scans, s)

		hasMore, err := s.Next()
		if err != nil {
			return nil, fmt.Errorf("scans[%d].Next: %w", i, err)
		}
		hasMores = append(hasMores, hasMore)
	}

	return &MultibufferSortScan{
		logger: logger.New("query.MultibufferSortScan", logger.Trace),

		scans:       scans,
		hasMores:    hasMores,
		currentScan: nil,
		comp:        comp,
	}, nil
}

func (s *MultibufferSortScan) BeforeFirst() error {
	for i, scan := range s.scans {
		if err := scan.BeforeFirst(); err != nil {
			return fmt.Errorf("scans[%d].BeforeFirst: %w", i, err)
		}
		hasMore, err := scan.Next()
		if err != nil {
			return fmt.Errorf("scans[%d].Next: %w", i, err)
		}
		s.hasMores[i] = hasMore
	}

	return nil
}

func (s *MultibufferSortScan) Next() (bool, error) {
	scanMap := make(map[int]Scan)
	for i, scan := range s.scans {
		if s.currentScan == scan {
			hasMore, err := scan.Next()
			if err != nil {
				return false, fmt.Errorf("scans[%d].Next: %w", i, err)
			}

			s.hasMores[i] = hasMore
		}

		s.logger.Tracef("Next(): scans[%d]: hasMore=%t", i, s.hasMores[i])

		if !s.hasMores[i] {
			break
		}

		scanMap[i] = scan
	}

	s.logger.Tracef("Next(): scanMap=%+v", scanMap)
	if len(scanMap) == 0 {
		return false, nil
	}

	minIdx := -1
	for i, scan := range scanMap {
		if minIdx == -1 {
			// first loop
			minIdx = i
			continue
		}

		cmp, err := s.comp.Compare(scan, scanMap[minIdx])
		if err != nil {
			return false, fmt.Errorf("s.comp.Compare: %w", err)
		}
		s.logger.Tracef("Next(): Compare(%+v, %+v) = %d", scan, scanMap[minIdx], cmp)
		if cmp < 0 {
			minIdx = i
		}
	}

	s.logger.Tracef("Next(): s.currentScan = s.scans[%d]", minIdx)
	s.currentScan = s.scans[minIdx]

	return true, nil
}

func (s *MultibufferSortScan) Close() {
	for _, scan := range s.scans {
		scan.Close()
	}
}

func (s *MultibufferSortScan) GetVal(fieldName string) (*Constant, error) {
	return s.currentScan.GetVal(fieldName)
}

func (s *MultibufferSortScan) GetInt(fieldName string) (int32, error) {
	return s.currentScan.GetInt(fieldName)
}

func (s *MultibufferSortScan) GetString(fieldName string) (string, error) {
	return s.currentScan.GetString(fieldName)
}

func (s *MultibufferSortScan) HasField(fieldName string) bool {
	return s.currentScan.HasField(fieldName)
}

func (s *MultibufferSortScan) SavePosition() error {
	savedPosition := make([]*record.RID, 0, len(s.scans))
	for i, scan := range s.scans {
		rid, err := scan.GetRID()
		if err != nil {
			return fmt.Errorf("scans[%d].GetRID: %w", i, err)
		}
		savedPosition = append(savedPosition, rid)
	}
	s.savedPosition = savedPosition

	return nil
}

func (s *MultibufferSortScan) RestorePosition() error {
	for i, scan := range s.scans {
		if err := scan.MoveToRID(s.savedPosition[i]); err != nil {
			return fmt.Errorf("scans[%d].MoveToRID: %w", i, err)
		}
	}

	return nil
}
