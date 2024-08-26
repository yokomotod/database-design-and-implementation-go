package query

type IndexSelectScan struct {
	tableScan *TableScan
	idx       Index
	val       *Constant
}

func NewIndexSelectScan(tableScan *TableScan, idx Index, val *Constant) *IndexSelectScan {
	return &IndexSelectScan{tableScan, idx, val}
}

func (p *IndexSelectScan) BeforeFirst() error {
	return p.idx.BeforeFirst(p.val)
}

func (p *IndexSelectScan) Next() (bool, error) {
	ok, err := p.idx.Next()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	rid, err := p.idx.GetDataRID()
	if err != nil {
		return false, err
	}
	if err := p.tableScan.MoveToRID(rid); err != nil {
		return false, err
	}
	return true, nil
}

func (p *IndexSelectScan) GetInt(fieldName string) (int32, error) {
	return p.tableScan.GetInt(fieldName)
}

func (p *IndexSelectScan) GetString(fieldName string) (string, error) {
	return p.tableScan.GetString(fieldName)
}

func (p *IndexSelectScan) GetVal(fieldName string) (*Constant, error) {
	return p.tableScan.GetVal(fieldName)
}

func (p *IndexSelectScan) HasField(fieldName string) bool {
	return p.tableScan.HasField(fieldName)
}

func (p *IndexSelectScan) Close() {
	p.idx.Close()
	p.tableScan.Close()
}
