package token

import (
	"strings"
	"testing"
)

func TestLookupKeywordCaseInsensitive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		want      Keyword
		wantFound bool
	}{
		{
			name:  "reserved lowercase",
			input: "select",
			want: Keyword{
				Word:  "SELECT",
				Class: KeywordReserved,
			},
			wantFound: true,
		},
		{
			name:  "reserved mixed case",
			input: "eNd-ExEc",
			want: Keyword{
				Word:  "END-EXEC",
				Class: KeywordReserved,
			},
			wantFound: true,
		},
		{
			name:  "non reserved lowercase",
			input: "serializable",
			want: Keyword{
				Word:  "SERIALIZABLE",
				Class: KeywordNonReserved,
			},
			wantFound: true,
		},
		{
			name:  "non reserved underscore form",
			input: "table_name",
			want: Keyword{
				Word:  "TABLE_NAME",
				Class: KeywordNonReserved,
			},
			wantFound: true,
		},
		{
			name:  "sql99 recursive",
			input: "recursive",
			want: Keyword{
				Word:  "RECURSIVE",
				Class: KeywordNonReserved,
			},
			wantFound: true,
		},
		{
			name:  "sql99 grouping",
			input: "gRoUpInG",
			want: Keyword{
				Word:  "GROUPING",
				Class: KeywordNonReserved,
			},
			wantFound: true,
		},
		{
			name:      "not a keyword",
			input:     "tucotuco",
			wantFound: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, found := LookupKeyword(tc.input)
			if found != tc.wantFound {
				t.Fatalf("LookupKeyword(%q) found = %v, want %v", tc.input, found, tc.wantFound)
			}

			if !tc.wantFound {
				return
			}

			if got != tc.want {
				t.Fatalf("LookupKeyword(%q) = %#v, want %#v", tc.input, got, tc.want)
			}
		})
	}
}

func TestKeywordHelpers(t *testing.T) {
	t.Parallel()

	if !IsKeyword("Ada") {
		t.Fatalf("IsKeyword(%q) = false, want true", "Ada")
	}

	if IsReservedKeyword("Ada") {
		t.Fatalf("IsReservedKeyword(%q) = true, want false", "Ada")
	}

	if !IsReservedKeyword("order") {
		t.Fatalf("IsReservedKeyword(%q) = false, want true", "order")
	}

	if !IsKeyword("recursive") {
		t.Fatalf("IsKeyword(%q) = false, want true", "recursive")
	}

	if IsReservedKeyword("recursive") {
		t.Fatalf("IsReservedKeyword(%q) = true, want false", "recursive")
	}

	if IsKeyword("not_a_keyword") {
		t.Fatalf("IsKeyword(%q) = true, want false", "not_a_keyword")
	}
}

func TestLookupSQL92KeywordExcludesSQL99Additions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     string
		want      Keyword
		wantFound bool
	}{
		{
			input: "select",
			want: Keyword{
				Word:  "SELECT",
				Class: KeywordReserved,
			},
			wantFound: true,
		},
		{
			input: "serializable",
			want: Keyword{
				Word:  "SERIALIZABLE",
				Class: KeywordNonReserved,
			},
			wantFound: true,
		},
		{
			input:     "recursive",
			wantFound: false,
		},
		{
			input:     "row",
			wantFound: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			got, found := LookupSQL92Keyword(tc.input)
			if found != tc.wantFound {
				t.Fatalf("LookupSQL92Keyword(%q) found = %v, want %v", tc.input, found, tc.wantFound)
			}

			if !tc.wantFound {
				return
			}

			if got != tc.want {
				t.Fatalf("LookupSQL92Keyword(%q) = %#v, want %#v", tc.input, got, tc.want)
			}
		})
	}
}

func TestSQL99KeywordsSourceOfTruth(t *testing.T) {
	t.Parallel()

	want := []Keyword{
		{Word: "AFTER", Class: KeywordNonReserved},
		{Word: "BEFORE", Class: KeywordNonReserved},
		{Word: "BREADTH", Class: KeywordNonReserved},
		{Word: "CUBE", Class: KeywordNonReserved},
		{Word: "CYCLE", Class: KeywordNonReserved},
		{Word: "DEPTH", Class: KeywordNonReserved},
		{Word: "EACH", Class: KeywordNonReserved},
		{Word: "ELSEIF", Class: KeywordNonReserved},
		{Word: "EXIT", Class: KeywordNonReserved},
		{Word: "GROUPING", Class: KeywordNonReserved},
		{Word: "HANDLER", Class: KeywordNonReserved},
		{Word: "IF", Class: KeywordNonReserved},
		{Word: "INSTEAD", Class: KeywordNonReserved},
		{Word: "ITERATE", Class: KeywordNonReserved},
		{Word: "LEAVE", Class: KeywordNonReserved},
		{Word: "LOOP", Class: KeywordNonReserved},
		{Word: "NEW", Class: KeywordNonReserved},
		{Word: "OLD", Class: KeywordNonReserved},
		{Word: "RECURSIVE", Class: KeywordNonReserved},
		{Word: "REFERENCING", Class: KeywordNonReserved},
		{Word: "REPEAT", Class: KeywordNonReserved},
		{Word: "RESIGNAL", Class: KeywordNonReserved},
		{Word: "RETURN", Class: KeywordNonReserved},
		{Word: "ROLLUP", Class: KeywordNonReserved},
		{Word: "ROW", Class: KeywordNonReserved},
		{Word: "SEARCH", Class: KeywordNonReserved},
		{Word: "SETS", Class: KeywordNonReserved},
		{Word: "SIGNAL", Class: KeywordNonReserved},
		{Word: "STATEMENT", Class: KeywordNonReserved},
		{Word: "UNTIL", Class: KeywordNonReserved},
		{Word: "WHILE", Class: KeywordNonReserved},
		{Word: "UNDO", Class: KeywordNonReserved},
	}

	got := SQL99Keywords()
	if len(got) != len(want) {
		t.Fatalf("SQL99Keywords() length = %d, want %d", len(got), len(want))
	}

	for index, keyword := range got {
		if keyword != want[index] {
			t.Fatalf("SQL99Keywords()[%d] = %#v, want %#v", index, keyword, want[index])
		}

		if keyword.Word != foldKeyword(keyword.Word) {
			t.Fatalf("SQL99Keywords()[%d].Word = %q, want canonical uppercase spelling", index, keyword.Word)
		}

		lookedUp, found := LookupKeyword(strings.ToLower(keyword.Word))
		if !found {
			t.Fatalf("LookupKeyword(%q) = not found, want keyword", keyword.Word)
		}

		if lookedUp != keyword {
			t.Fatalf("LookupKeyword(%q) = %#v, want %#v", keyword.Word, lookedUp, keyword)
		}

		if _, found := LookupSQL92Keyword(keyword.Word); found {
			t.Fatalf("LookupSQL92Keyword(%q) = found, want not found", keyword.Word)
		}
	}
}

func TestSQL92KeywordSourceOfTruth(t *testing.T) {
	t.Parallel()

	sql92Keywords := SQL92Keywords()
	if len(sql92Keywords) != len(sql92KeywordSpecs) {
		t.Fatalf("SQL92Keywords() length = %d, want %d", len(sql92Keywords), len(sql92KeywordSpecs))
	}

	for index, keyword := range sql92Keywords {
		if keyword != sql92KeywordSpecs[index] {
			t.Fatalf("SQL92Keywords()[%d] = %#v, want %#v", index, keyword, sql92KeywordSpecs[index])
		}
	}

	seen := make(map[string]KeywordClass, len(sql92KeywordSpecs)+len(sql99KeywordSpecs))
	for index, keyword := range sql92KeywordSpecs {
		if keyword.Word != foldKeyword(keyword.Word) {
			t.Fatalf("keyword %d = %q, want canonical uppercase spelling", index, keyword.Word)
		}

		if keyword.Class != KeywordReserved && keyword.Class != KeywordNonReserved {
			t.Fatalf("keyword %q has unexpected class %v", keyword.Word, keyword.Class)
		}

		if previous, exists := seen[keyword.Word]; exists {
			t.Fatalf("duplicate keyword %q with classes %v and %v", keyword.Word, previous, keyword.Class)
		}

		seen[keyword.Word] = keyword.Class
	}

	for index, keyword := range sql99KeywordSpecs {
		if keyword.Word != foldKeyword(keyword.Word) {
			t.Fatalf("sql99 keyword %d = %q, want canonical uppercase spelling", index, keyword.Word)
		}

		if keyword.Class != KeywordReserved && keyword.Class != KeywordNonReserved {
			t.Fatalf("sql99 keyword %q has unexpected class %v", keyword.Word, keyword.Class)
		}

		if previous, exists := seen[keyword.Word]; exists {
			t.Fatalf("duplicate SQL keyword %q with classes %v and %v", keyword.Word, previous, keyword.Class)
		}

		seen[keyword.Word] = keyword.Class
	}

	if len(seen) != len(keywordTable) {
		t.Fatalf("keywordTable size = %d, want %d", len(keywordTable), len(seen))
	}
}
