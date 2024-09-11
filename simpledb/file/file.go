package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"simpledb/util/logger"
	"strings"
	"sync"
	"unicode/utf16"
)

type BlockID struct {
	FileName string
	Number   int32
}

func NewBlockID(filename string, blockNum int32) BlockID {
	return BlockID{
		FileName: filename,
		Number:   blockNum,
	}
}

type Page struct {
	buffer []byte
}

const (
	Int32Bytes int32 = 4
	utf16Size  int32 = 2
)

func NewPage(blockSize int32) *Page {
	return &Page{
		buffer: make([]byte, blockSize),
	}
}

func NewPageWith(buffer []byte) *Page {
	return &Page{
		buffer: buffer,
	}
}

func (p *Page) GetInt(offset int32) int32 {
	return int32(binary.LittleEndian.Uint32(p.buffer[offset : offset+Int32Bytes]))
}

func (p *Page) SetInt(offset int32, val int32) {
	binary.LittleEndian.PutUint32(p.buffer[offset:offset+4], uint32(val))
}

func (p *Page) GetBytes(offset int32) []byte {
	length := p.GetInt(offset)
	return p.buffer[offset+Int32Bytes : offset+Int32Bytes+length]
}

func (p *Page) SetBytes(offset int32, val []byte) {
	p.SetInt(offset, int32(len(val)))
	copy(p.buffer[offset+Int32Bytes:], val)
}

func (p *Page) GetString(offset int32) string {
	length := p.GetInt(offset) / utf16Size

	runes := make([]uint16, length)
	for i := range length {
		runes[i] = p.getUint16(offset + Int32Bytes + int32(i)*utf16Size)
	}

	return string(utf16.Decode(runes))
}

func (p *Page) SetString(offset int32, val string) {
	runes := utf16.Encode([]rune(val))

	p.SetInt(offset, int32(int32(len(runes))*utf16Size))

	for i, r := range runes {
		p.setUint16(offset+Int32Bytes+int32(i)*utf16Size, r)
	}
}

func (p *Page) getUint16(offset int32) uint16 {
	return binary.LittleEndian.Uint16(p.buffer[offset : offset+utf16Size])
}

func (p *Page) setUint16(offset int32, val uint16) {
	binary.LittleEndian.PutUint16(p.buffer[offset:offset+utf16Size], val)
}

func MaxLength(length int32) int32 {
	return Int32Bytes + length*utf16Size
}

type Manager struct {
	Logger *logger.FileManagerLogger

	dbDir     string
	blockSize int32
	isNew     bool
	files     map[string]*os.File
	mux       *sync.Mutex
}

func NewManager(dbDir string, blockSize int32) (*Manager, error) {
	isNew := false
	// if not exists, create dbDir recursively
	if _, err := os.Stat(dbDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("os.Stat: %w", err)
		}
		isNew = true

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
		Logger: logger.NewFileManagerLogger("file.Manager", logger.Debug, logger.Trace),

		dbDir:     dbDir,
		blockSize: blockSize,
		isNew:     isNew,
		files:     make(map[string]*os.File),
		mux:       &sync.Mutex{},
	}, nil
}

func (fm *Manager) IsNew() bool {
	return fm.isNew
}

func (fm *Manager) BlockSize() int32 {
	return fm.blockSize
}

func (fm *Manager) Read(blk BlockID, p *Page) error {
	fm.mux.Lock()
	defer fm.mux.Unlock()

	fm.Logger.Get(blk.FileName).Tracef("Read(%+v)", blk)

	f, err := fm.openFile(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.openFile: %w", err)
	}

	_, err = f.Seek(int64(blk.Number)*int64(fm.blockSize), 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Read(p.buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("f.Read: %w", err)
	}

	return nil
}

func (fm *Manager) Write(blk BlockID, p *Page) error {
	fm.mux.Lock()
	defer fm.mux.Unlock()

	fm.Logger.Get(blk.FileName).Tracef("Write(%+v)", blk)

	f, err := fm.openFile(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.openFile: %w", err)
	}

	_, err = f.Seek(int64(blk.Number)*int64(fm.blockSize), 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Write(p.buffer)
	if err != nil {
		return fmt.Errorf("f.Write: %w", err)
	}

	return nil
}

func (fm *Manager) Append(filename string) (BlockID, error) {
	fm.mux.Lock()
	defer fm.mux.Unlock()

	fm.Logger.Get(filename).Tracef("Append(%q)", filename)

	newBlockNum, err := fm.Length(filename)
	if err != nil {
		return BlockID{}, fmt.Errorf("fm.Length: %w", err)
	}

	blk := NewBlockID(filename, newBlockNum)
	b := make([]byte, fm.blockSize)

	f, err := fm.openFile(blk.FileName)
	if err != nil {
		return BlockID{}, fmt.Errorf("fm.openFile: %w", err)
	}

	_, err = f.Seek(int64(blk.Number)*int64(fm.blockSize), 0)
	if err != nil {
		return BlockID{}, fmt.Errorf("f.Seek: %w", err)
	}
	_, err = f.Write(b)
	if err != nil {
		return BlockID{}, fmt.Errorf("f.Write: %w", err)
	}

	return blk, nil
}

func (fm *Manager) Length(filename string) (int32, error) {
	fm.Logger.Get(filename).Tracef("Length(%q)", path.Join(fm.dbDir, filename))
	f, err := fm.openFile(filename)
	if err != nil {
		return 0, fmt.Errorf("fm.openFile: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("f.Stat: %w", err)
	}

	var length int32 = int32(fi.Size() / int64(fm.blockSize))
	return length, nil
}

func (fm *Manager) openFile(filename string) (*os.File, error) {
	if f, ok := fm.files[filename]; ok {
		return f, nil
	}

	fm.Logger.Get(filename).Tracef("openFile(%q)", filename)
	f, err := os.OpenFile(path.Join(fm.dbDir, filename), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}

	fm.files[filename] = f

	return f, nil
}
