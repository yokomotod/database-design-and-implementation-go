package parse_test

import (
	"simpledb/parse"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPredParser(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		input     string
		wantError bool
	}{
		{
			input:     "age = 20",
			wantError: false,
		},
		{
			input:     "age = 20 AND name = 'Alice'",
			wantError: false,
		},
		{
			input:     "age = 20 AND name = 'Alice' and 1 = 2",
			wantError: false,
		},
		{
			input:     "is_expired", // `=` を使用しない式はサポートされていない
			wantError: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			p, err := parse.NewPredParser(tt.input)
			require.NoError(t, err)

			err = p.Predicate()

			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
