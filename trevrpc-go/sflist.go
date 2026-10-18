package trevrpc

import (
	"strings"
)

// parseStringList parses value as an RFC 9651 Structured Field list whose
// members are all sf-strings, such as the WT-Available-Protocols field
// `"trevrpc-2", "trevrpc-1";q=1`. Parameters on a member are validated and
// dropped. It reports false if value is not such a list: bad syntax, a
// trailing comma, an inner list, or a member that is not a string. An empty
// value is an empty list.
func parseStringList(value string) ([]string, bool) {
	p := sfParser{input: value}
	p.skipSP()
	if p.done() {
		return nil, true
	}
	var list []string
	for {
		member, ok := p.stringItem()
		if !ok {
			return nil, false
		}
		list = append(list, member)
		p.skipOWS()
		if p.done() {
			return list, true
		}
		if !p.consume(',') {
			return nil, false
		}
		p.skipOWS()
		if p.done() {
			return nil, false // trailing comma
		}
	}
}

type sfParser struct {
	input string
	pos   int
}

func (p *sfParser) done() bool { return p.pos >= len(p.input) }

func (p *sfParser) peek() byte {
	if p.done() {
		return 0
	}
	return p.input[p.pos]
}

func (p *sfParser) consume(c byte) bool {
	if p.peek() == c && !p.done() {
		p.pos++
		return true
	}
	return false
}

func (p *sfParser) skipSP() {
	for !p.done() && p.input[p.pos] == ' ' {
		p.pos++
	}
}

// skipOWS skips the optional whitespace (SP or HTAB) around list commas.
func (p *sfParser) skipOWS() {
	for !p.done() && (p.input[p.pos] == ' ' || p.input[p.pos] == '\t') {
		p.pos++
	}
}

// stringItem parses an sf-string followed by its parameters.
func (p *sfParser) stringItem() (string, bool) {
	if p.peek() != '"' {
		return "", false
	}
	s, ok := p.quoted()
	if !ok {
		return "", false
	}
	return s, p.parameters()
}

// quoted parses an sf-string: a double-quoted run of printable ASCII in which
// only `\"` and `\\` are escapes.
func (p *sfParser) quoted() (string, bool) {
	p.pos++ // opening quote
	start := p.pos
	escaped := false
	for i := start; i < len(p.input); i++ {
		c := p.input[i]
		switch {
		case c == '\\':
			escaped = true
			i++
			if i >= len(p.input) || (p.input[i] != '"' && p.input[i] != '\\') {
				return "", false
			}
		case c == '"':
			p.pos = i + 1
			raw := p.input[start:i]
			if !escaped {
				return raw, true
			}
			return unescapeSFString(raw), true
		case c < 0x20 || c > 0x7e:
			return "", false
		}
	}
	return "", false // unterminated
}

func unescapeSFString(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' {
			i++
		}
		b.WriteByte(raw[i])
	}
	return b.String()
}

// parameters parses and drops `*( ";" *SP key [ "=" bare-item ] )`.
func (p *sfParser) parameters() bool {
	for p.consume(';') {
		p.skipSP()
		if !p.key() {
			return false
		}
		if p.consume('=') && !p.bareItem() {
			return false
		}
	}
	return true
}

// key parses a parameter key: lcalpha or "*", then lcalpha, DIGIT, "_", "-",
// "." or "*".
func (p *sfParser) key() bool {
	if p.done() || !isSFKeyStart(p.input[p.pos]) {
		return false
	}
	p.pos++
	for !p.done() && isSFKeyByte(p.input[p.pos]) {
		p.pos++
	}
	return true
}

func isSFKeyStart(c byte) bool { return c >= 'a' && c <= 'z' || c == '*' }

func isSFKeyByte(c byte) bool {
	return isSFKeyStart(c) || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.'
}

// bareItem parses and drops a parameter value of any bare item type.
func (p *sfParser) bareItem() bool {
	c := p.peek()
	switch {
	case p.done():
		return false
	case c == '"':
		_, ok := p.quoted()
		return ok
	case c == '-' || c >= '0' && c <= '9':
		return p.number()
	case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '*':
		return p.token()
	case c == '?':
		p.pos++
		if p.peek() == '0' || p.peek() == '1' {
			p.pos++
			return true
		}
		return false
	case c == ':':
		return p.byteSequence()
	case c == '@':
		p.pos++
		return p.integerOnly()
	case c == '%':
		return p.displayString()
	default:
		return false
	}
}

func (p *sfParser) token() bool {
	p.pos++
	for !p.done() && isSFTokenByte(p.input[p.pos]) {
		p.pos++
	}
	return true
}

func isSFTokenByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~:/", c) >= 0
}

// number parses an sf-integer (at most 15 digits) or sf-decimal (at most 12
// integer digits and 3 fractional digits).
func (p *sfParser) number() bool {
	p.consume('-')
	intDigits := p.digits()
	if intDigits == 0 || intDigits > 15 {
		return false
	}
	if !p.consume('.') {
		return true
	}
	if intDigits > 12 {
		return false
	}
	fractionDigits := p.digits()
	return fractionDigits >= 1 && fractionDigits <= 3
}

// integerOnly parses an sf-integer, the payload of a date.
func (p *sfParser) integerOnly() bool {
	p.consume('-')
	n := p.digits()
	return n >= 1 && n <= 15
}

func (p *sfParser) digits() int {
	start := p.pos
	for !p.done() && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
		p.pos++
	}
	return p.pos - start
}

// byteSequence parses `":" *(base64) ":"`.
func (p *sfParser) byteSequence() bool {
	p.pos++
	for !p.done() {
		c := p.input[p.pos]
		p.pos++
		switch {
		case c == ':':
			return true
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '+', c == '/', c == '=':
		default:
			return false
		}
	}
	return false
}

// displayString parses `%"` *(unreserved / %xx) `"`, with lowercase hex
// escapes only.
func (p *sfParser) displayString() bool {
	p.pos++
	if !p.consume('"') {
		return false
	}
	for !p.done() {
		c := p.input[p.pos]
		p.pos++
		switch {
		case c == '"':
			return true
		case c == '%':
			if p.pos+2 > len(p.input) || !isLowerHex(p.input[p.pos]) || !isLowerHex(p.input[p.pos+1]) {
				return false
			}
			p.pos += 2
		case c < 0x20 || c > 0x7e:
			return false
		}
	}
	return false
}

func isLowerHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' }
