package trevrpc

import (
	"net/http"
	"slices"
	"strings"
	"testing"
)

func TestParseStringList(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"empty", "", nil},
		{"blank", "   ", nil},
		{"one", `"trevrpc-1"`, []string{"trevrpc-1"}},
		{"two", `"trevrpc-2", "trevrpc-1"`, []string{"trevrpc-2", "trevrpc-1"}},
		{"no space after comma", `"a","b"`, []string{"a", "b"}},
		{"tab and spaces around comma", "\"a\" \t,\t \"b\"", []string{"a", "b"}},
		{"surrounding spaces", `  "a"  `, []string{"a"}},
		{"empty string member", `""`, []string{""}},
		{"escaped quote", `"a\"b"`, []string{`a"b`}},
		{"escaped backslash", `"a\\b"`, []string{`a\b`}},
		{"escaped backslash before closing quote", `"a\\"`, []string{`a\`}},
		{"escaped quote then backslash", `"\"\\"`, []string{`"\`}},
		{"comma inside string", `"a,b", "c"`, []string{"a,b", "c"}},
		{"semicolon inside string", `"a;b"`, []string{"a;b"}},
		{"printable ASCII", ` "!#~ "`, []string{"!#~ "}},
		{"bare parameter", `"a";x`, []string{"a"}},
		{"integer parameter", `"a";q=1, "b"`, []string{"a", "b"}},
		{"negative decimal parameter", `"a";q=-1.5`, []string{"a"}},
		{"string parameter", `"a";x="y,z\"", "b"`, []string{"a", "b"}},
		{"token parameter", `"a";x=tok/en:1`, []string{"a"}},
		{"boolean parameters", `"a";x=?1;y=?0`, []string{"a"}},
		{"byte sequence parameter", `"a";x=:aGVsbG8=:`, []string{"a"}},
		{"date parameter", `"a";x=@1659578233`, []string{"a"}},
		{"display string parameter", `"a";x=%"f%c3%bc"`, []string{"a"}},
		{"star key and space after semicolon", `"a"; *k=1;  k.2-x_=2`, []string{"a"}},
		{"max digits", `"a";x=123456789012345`, []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseStringList(tt.input)
			if !ok || !slices.Equal(got, tt.want) {
				t.Fatalf("parseStringList(%q) = %q, %v, want %q, true", tt.input, got, ok, tt.want)
			}
		})
	}
}

func TestParseStringListRejectsInvalidLists(t *testing.T) {
	for _, input := range []string{
		`trevrpc-1`,                // token, not a string
		`1`,                        // integer
		`?1`,                       // boolean
		`:aGk=:`,                   // byte sequence
		`("a" "b")`,                // inner list
		`("a")`,                    // inner list of one
		`"a" "b"`,                  // missing comma
		`"a",`,                     // trailing comma
		`"a", `,                    // trailing comma and space
		`,"a"`,                     // leading comma
		`"a",,"b"`,                 // empty member
		`"a", ,"b"`,                // empty member
		`"a`,                       // unterminated
		`"a\"`,                     // unterminated after an escaped quote
		`"a\`,                      // dangling backslash
		`"a\n"`,                    // unknown escape
		`"a\x"`,                    // unknown escape
		"\"a\x01\"",                // control character
		"\"a\x7f\"",                // DEL
		"\"café\"",                 // non-ASCII
		"\"a\nb\"",                 // newline
		`"a";`,                     // empty parameter
		`"a";=1`,                   // empty key
		`"a";X=1`,                  // uppercase key
		`"a";1=1`,                  // key starting with a digit
		`"a";x=`,                   // missing value
		`"a";x="b`,                 // unterminated string value
		`"a";x=?2`,                 // bad boolean
		`"a";x=:abc`,               // unterminated byte sequence
		`"a";x=:a b:`,              // bad base64 character
		`"a";x=@`,                  // empty date
		`"a";x=%"f%C3"`,            // uppercase hex in display string
		`"a";x=%"f%c`,              // truncated escape
		`"a";x=%f`,                 // missing quote
		`"a";x=1234567890123456`,   // 16 digits
		`"a";x=1234567890123.5`,    // 13 integer digits in a decimal
		`"a";x=1.`,                 // empty fraction
		`"a";x=1.2345`,             // 4 fraction digits
		`"a";x=-`,                  // sign only
		`"a";x=#`,                  // not a bare item
		`"a" ; x=1`,                // space before semicolon
		`"a"x`,                     // trailing garbage
		"\"a\"\x00",                // NUL
		strings.Repeat(`"a", `, 3), // trailing comma after valid members
	} {
		if got, ok := parseStringList(input); ok {
			t.Errorf("parseStringList(%q) = %q, true, want failure", input, got)
		}
	}
}

func TestOffersOnlyOtherWebTransportProtocols(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   bool
	}{
		{"missing field is version 1", nil, false},
		{"offers trevrpc-1", []string{`"trevrpc-1"`}, false},
		{"offers trevrpc-1 among others", []string{`"trevrpc-2", "trevrpc-1"`}, false},
		{"offers trevrpc-1 with parameters", []string{`"trevrpc-2";q=1, "trevrpc-1";q=0`}, false},
		{"only another version", []string{`"trevrpc-2"`}, true},
		{"only other protocols", []string{`"chat", "trevrpc-2"`}, true},
		{"prefix is not a match", []string{`"trevrpc-10", "trevrpc-"`}, true},
		{"case matters", []string{`"TrevRPC-1"`}, true},
		{"empty value is absent", []string{``}, false},
		{"whitespace only is absent", []string{"  \t "}, false},
		{"empty field lines are absent", []string{``, ``}, false},
		{"empty field line beside another list", []string{``, `"trevrpc-2"`}, true},
		{"several field lines are one list", []string{`"trevrpc-2"`, `"trevrpc-1"`}, false},
		{"several field lines without trevrpc-1", []string{`"trevrpc-2"`, `"trevrpc-3"`}, true},
		{"malformed is ignored", []string{`trevrpc-2`}, false},
		{"trailing comma is ignored", []string{`"trevrpc-2",`}, false},
		{"inner list is ignored", []string{`("trevrpc-2")`}, false},
		{"one malformed field line spoils the list", []string{`"trevrpc-2"`, `trevrpc-3`}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			for _, value := range tt.values {
				header.Add(wtAvailableProtocolsHeader, value)
			}
			if got := offersOnlyOtherWebTransportProtocols(header); got != tt.want {
				t.Fatalf("offersOnlyOtherWebTransportProtocols(%q) = %v, want %v", tt.values, got, tt.want)
			}
		})
	}
}

func FuzzParseStringList(f *testing.F) {
	for _, seed := range []string{``, `"a"`, `"a", "b";q=1`, `"a\"b"`, `("a")`, `"a";x=:aGk=:`, `"a",`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		list, ok := parseStringList(input)
		if !ok && list != nil {
			t.Fatalf("parseStringList(%q) = %q with ok=false", input, list)
		}
		for _, member := range list {
			for i := 0; i < len(member); i++ {
				if member[i] < 0x20 || member[i] > 0x7e {
					t.Fatalf("parseStringList(%q) member %q has a byte outside printable ASCII", input, member)
				}
			}
		}
	})
}
