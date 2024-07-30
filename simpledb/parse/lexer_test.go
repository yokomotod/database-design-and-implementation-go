package parse_test

import (
	"simpledb/parse"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexer(t *testing.T) {
	t.Parallel()

	lex, err := parse.NewLexer("SELECT * from Table_1 where age = 20 and country = 'United States';")
	require.NoError(t, err)

	{
		assert.True(t, lex.MatchKeyword("select"))
		err := lex.EatKeyword("select")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lex.MatchDelim('*'))
		err := lex.EatDelim('*')
		assert.NoError(t, err)
	}
	{
		assert.True(t, lex.MatchKeyword("from"))
		err := lex.EatKeyword("from")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lex.MatchIdentifier())
		v, err := lex.EatIdentifier()
		assert.NoError(t, err)
		assert.Equal(t, "table_1", v)
	}
	{
		assert.True(t, lex.MatchKeyword("where"))
		err := lex.EatKeyword("where")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lex.MatchIdentifier())
		v, err := lex.EatIdentifier()
		assert.NoError(t, err)
		assert.Equal(t, "age", v)
	}
	{
		assert.True(t, lex.MatchDelim('='))
		err := lex.EatDelim('=')
		assert.NoError(t, err)
	}
	{
		assert.True(t, lex.MatchIntConstant())
		v, err := lex.EatIntConstant()
		assert.NoError(t, err)
		assert.Equal(t, int32(20), v)
	}
	{
		assert.True(t, lex.MatchKeyword("and"))
		err := lex.EatKeyword("and")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lex.MatchIdentifier())
		v, err := lex.EatIdentifier()
		assert.NoError(t, err)
		assert.Equal(t, "country", v)
	}
	{
		assert.True(t, lex.MatchDelim('='))
		err := lex.EatDelim('=')
		assert.NoError(t, err)
	}
	{
		assert.True(t, lex.MatchStringConstant())
		v, err := lex.EatStringConstant()
		assert.NoError(t, err)
		assert.Equal(t, "United States", v)
	}
	{
		assert.True(t, lex.MatchDelim(';'))
		err := lex.EatDelim(';')
		assert.NoError(t, err)
	}
	{
		_, err := lex.EatIdentifier()
		var errBadSyntax *parse.BadSyntaxError
		assert.ErrorAs(t, err, &errBadSyntax)
	}
}
