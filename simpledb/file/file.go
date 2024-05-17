package file

type BlockID struct{}

func NewBlockID(filename string, blockNum int) *BlockID {
	return &BlockID{}
}

type Page struct{}

func NewPage(blockSize int) *Page {
	return &Page{}
}

func (p *Page) GetInt(offset int) int {
	return 0
}

func (p *Page) SetInt(offset int, val int) {}

func (p *Page) GetString(offset int) string {
	return ""
}

func (p *Page) SetString(offset int, val string) {}

func MaxLength(length int) int {
	return 0
}

type Manager struct {
	blockSize int
}

func NewManager(dbDir string, blockSize int) *Manager {
	return &Manager{
		blockSize: blockSize,
	}
}

func (fm *Manager) BlockSize() int {
	return fm.blockSize
}

func (fm *Manager) Write(blk *BlockID, p *Page) {}

func (fm *Manager) Read(blk *BlockID, p *Page) {}
