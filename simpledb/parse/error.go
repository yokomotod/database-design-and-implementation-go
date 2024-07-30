package parse

type BadSyntaxError struct {
	message string
}

func NewBadSyntaxError(message string) error {
	return &BadSyntaxError{
		message: message,
	}
}

func (e *BadSyntaxError) Error() string {
	return "bad syntax: " + e.message
}
