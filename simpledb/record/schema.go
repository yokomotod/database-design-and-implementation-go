package record

type FieldType int32

const (
	INT FieldType = iota
	VARCHAR
)

type fieldInfo struct {
	FieldType FieldType
	Length    int32
}

type Schema struct {
	fields []string
	info   map[string]*fieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]*fieldInfo),
	}
}

// AddField フィールドを追加する。lengthはSTRINGの場合のみ有効
func (s *Schema) AddField(fieldName string, fieldType FieldType, length int32) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = &fieldInfo{FieldType: fieldType, Length: length}
}

func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, INT, 0)
}

func (s *Schema) AddStringField(fieldName string, length int32) {
	s.AddField(fieldName, VARCHAR, length)
}

func (s *Schema) Add(fieldName string, schema *Schema) {
	fieldType := schema.Type(fieldName)
	length := schema.Length(fieldName)
	s.AddField(fieldName, fieldType, length)
}

func (s *Schema) AddAll(other *Schema) {
	for _, fieldName := range other.fields {
		s.Add(fieldName, other)
	}
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) HasField(fieldName string) bool {
	_, ok := s.info[fieldName]
	return ok
}

func (s *Schema) Type(fieldName string) FieldType {
	return s.info[fieldName].FieldType
}

func (s *Schema) Length(fieldName string) int32 {
	return s.info[fieldName].Length
}
