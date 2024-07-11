package file

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strings"
	"unicode/utf16"
)

type BlockID struct {
	filename string
	blockNum int64
}

func NewBlockID(filename string, blockNum int64) *BlockID {
	return &BlockID{
		filename: filename,
		blockNum: blockNum,
	}
}

func (blk *BlockID) FileName() string {
	return blk.filename
}

func (blk *BlockID) Number() int64 {
	return blk.blockNum
}

type Page struct {
	buffer []byte
}

const (
	Int32Bytes = 4
	utf16Size  = 2
)

func NewPage(blockSize int64) *Page {
	return &Page{
		buffer: make([]byte, blockSize),
	}
}

func NewPageWith(buffer []byte) *Page {
	return &Page{
		buffer: buffer,
	}
}

func (p *Page) GetInt(offset int) int32 {
	return int32(binary.LittleEndian.Uint32(p.buffer[offset : offset+Int32Bytes]))
}

func (p *Page) SetInt(offset int, val int32) {
	binary.LittleEndian.PutUint32(p.buffer[offset:offset+4], uint32(val))
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.buffer[offset+Int32Bytes : offset+Int32Bytes+int(length)]
}

func (p *Page) SetBytes(offset int, val []byte) {
	p.SetInt(offset, int32(len(val)))
	copy(p.buffer[offset+Int32Bytes:], val)
}

func (p *Page) GetString(offset int) string {
	length := int(p.GetInt(offset)) / utf16Size

	runes := make([]uint16, length)
	for i := range length {
		runes[i] = p.getUint16(offset + Int32Bytes + i*utf16Size)
	}

	return string(utf16.Decode(runes))
}

func (p *Page) SetString(offset int, val string) {
	runes := utf16.Encode([]rune(val))

	p.SetInt(offset, int32(len(runes)*utf16Size))

	for i, r := range runes {
		p.setUint16(offset+Int32Bytes+i*utf16Size, r)
	}
}

func (p *Page) getUint16(offset int) uint16 {
	return binary.LittleEndian.Uint16(p.buffer[offset : offset+utf16Size])
}

func (p *Page) setUint16(offset int, val uint16) {
	binary.LittleEndian.PutUint16(p.buffer[offset:offset+utf16Size], val)
}

func MaxLength(length int) int {
	return Int32Bytes + length*utf16Size
}

type Manager struct {
	dbDir     string
	blockSize int64
	files     map[string]*os.File
}

func NewManager(dbDir string, blockSize int64) (*Manager, error) {
	// if not exists, create dbDir recursively
	if _, err := os.Stat(dbDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("os.Stat: %w", err)
		}

		err = os.MkdirAll(dbDir, 0o700)
		if err != nil {
			return nil, fmt.Errorf("os.MkdirAll: %w", err)
		}
	}

	// remove any leftover temporary tables
	files, err := os.ReadDir(dbDir)
	if err != nil {
		return nil, fmt.Errorf("os.ReadDir: %w", err)
	}

	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "temp") {
			continue
		}

		if err = os.Remove(file.Name()); err != nil {
			return nil, fmt.Errorf("os.Remove: %w", err)
		}
	}

	return &Manager{
		dbDir:     dbDir,
		blockSize: blockSize,
		files:     make(map[string]*os.File),
	}, nil
}

func (fm *Manager) BlockSize() int64 {
	return fm.blockSize
}

func (fm *Manager) Read(blk *BlockID, p *Page) error {
	f, err := fm.openFile(blk.filename)
	if err != nil {
		return fmt.Errorf("fm.openFile: %w", err)
	}

	_, err = f.Seek(blk.blockNum*fm.blockSize, 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Read(p.buffer)
	if err != nil {
		return fmt.Errorf("f.Read: %w", err)
	}

	return nil
}

func (fm *Manager) Write(blk *BlockID, p *Page) error {
	f, err := fm.openFile(blk.filename)
	if err != nil {
		return fmt.Errorf("fm.openFile: %w", err)
	}

	_, err = f.Seek(blk.blockNum*fm.blockSize, 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Write(p.buffer)
	if err != nil {
		return fmt.Errorf("f.Write: %w", err)
	}

	return nil
}

func (fm *Manager) Append(filename string) (*BlockID, error) {
	newBlockNum, err := fm.Length(filename)
	if err != nil {
		return nil, fmt.Errorf("fm.Length: %w", err)
	}

	blk := NewBlockID(filename, newBlockNum)
	b := make([]byte, fm.blockSize)

	f, err := fm.openFile(blk.filename)
	if err != nil {
		return nil, fmt.Errorf("fm.openFile: %w", err)
	}

	f.Seek(blk.blockNum*fm.blockSize, 0)
	f.Write(b)

	return blk, nil
}

func (fm *Manager) Length(filename string) (int64, error) {
	f, err := fm.openFile(filename)
	if err != nil {
		return 0, fmt.Errorf("fm.openFile: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("f.Stat: %w", err)
	}

	return fi.Size() / fm.blockSize, nil
}

func (fm *Manager) openFile(filename string) (*os.File, error) {
	if f, ok := fm.files[filename]; ok {
		return f, nil
	}

	f, err := os.OpenFile(path.Join(fm.dbDir, filename), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}

	fm.files[filename] = f

	return f, nil
}
