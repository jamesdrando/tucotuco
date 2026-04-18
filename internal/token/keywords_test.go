package token

import "testing"

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

	if IsKeyword("not_a_keyword") {
		t.Fatalf("IsKeyword(%q) = true, want false", "not_a_keyword")
	}
}

func TestSQL92KeywordSourceOfTruth(t *testing.T) {
	t.Parallel()

	seen := make(map[string]KeywordClass, len(sql92KeywordSpecs))
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

	if len(seen) != len(keywordTable) {
		t.Fatalf("keywordTable size = %d, want %d", len(keywordTable), len(seen))
	}
}
