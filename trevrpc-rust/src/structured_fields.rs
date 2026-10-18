//! The subset of [RFC 9651] Structured Field Values that `TrevRPC` reads: a list of strings, as in
//! the `WT-Available-Protocols` header of draft-ietf-webtrans-http3.
//!
//! [RFC 9651]: https://www.rfc-editor.org/rfc/rfc9651

/// Parses a field value as an `sf-list` whose members are all `sf-string` items.
///
/// Returns the member strings, unescaped and in order, with each member's parameters validated and
/// dropped. Returns `None` when the value is not a valid `sf-list` or when any member is not a
/// string (a token, number, boolean, inner list, ...), which is how the protocol negotiation
/// treats a malformed header. An empty or all-whitespace value is the valid empty list.
///
/// The value is the field's combined line: callers join repeated field lines with `,`.
pub fn parse_string_list(input: &str) -> Option<Vec<String>> {
    let mut parser = Parser {
        rest: input.as_bytes(),
    };
    let mut members = Vec::new();

    parser.skip_sp();
    if parser.rest.is_empty() {
        return Some(members);
    }
    loop {
        members.push(parser.string_member()?);
        parser.skip_ows();
        if parser.rest.is_empty() {
            return Some(members);
        }
        parser.expect(b',')?;
        parser.skip_ows();
        if parser.rest.is_empty() {
            // A trailing comma leaves a missing member.
            return None;
        }
    }
}

/// A cursor over the unparsed remainder of the field value.
struct Parser<'a> {
    rest: &'a [u8],
}

impl Parser<'_> {
    const fn peek(&self) -> Option<u8> {
        self.rest.first().copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let (&byte, rest) = self.rest.split_first()?;
        self.rest = rest;
        Some(byte)
    }

    fn eat(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.rest = self.rest.get(1..).unwrap_or_default();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: u8) -> Option<()> {
        self.eat(expected).then_some(())
    }

    /// Consumes the longest prefix whose bytes satisfy `predicate` and returns its length.
    fn take_while(&mut self, predicate: impl Fn(u8) -> bool) -> usize {
        let len = self
            .rest
            .iter()
            .take_while(|&&byte| predicate(byte))
            .count();
        self.rest = self.rest.get(len..).unwrap_or_default();
        len
    }

    fn skip_sp(&mut self) {
        self.take_while(|byte| byte == b' ');
    }

    fn skip_ows(&mut self) {
        self.take_while(|byte| byte == b' ' || byte == b'\t');
    }

    /// `list-member` restricted to a string `sf-item`: a string and its parameters.
    fn string_member(&mut self) -> Option<String> {
        let value = self.string()?;
        self.parameters()?;
        Some(value)
    }

    /// `parameters = *( ";" *SP parameter )`, validated and discarded.
    fn parameters(&mut self) -> Option<()> {
        while self.eat(b';') {
            self.skip_sp();
            self.key()?;
            if self.eat(b'=') {
                self.bare_item()?;
            }
        }
        Some(())
    }

    /// `key = ( lcalpha / "*" ) *( lcalpha / DIGIT / "_" / "-" / "." / "*" )`.
    fn key(&mut self) -> Option<()> {
        let first = self.bump()?;
        if !(first.is_ascii_lowercase() || first == b'*') {
            return None;
        }
        self.take_while(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'-' | b'.' | b'*')
        });
        Some(())
    }

    /// Any `bare-item`, for a parameter value.
    fn bare_item(&mut self) -> Option<()> {
        match self.peek()? {
            b'-' | b'0'..=b'9' => self.number(),
            b'"' => self.string().map(drop),
            b'A'..=b'Z' | b'a'..=b'z' | b'*' => self.token(),
            b':' => self.binary(),
            b'?' => self.boolean(),
            b'@' => self.date(),
            b'%' => self.display_string(),
            _ => None,
        }
    }

    /// `sf-string = DQUOTE *chr DQUOTE`, where `chr` is printable ASCII except that `"` and `\` are
    /// escaped with `\`.
    fn string(&mut self) -> Option<String> {
        self.expect(b'"')?;
        let mut value = Vec::new();
        loop {
            match self.bump()? {
                b'"' => return String::from_utf8(value).ok(),
                b'\\' => match self.bump()? {
                    escaped @ (b'"' | b'\\') => value.push(escaped),
                    _ => return None,
                },
                byte @ 0x20..=0x7e => value.push(byte),
                _ => return None,
            }
        }
    }

    /// `sf-integer` or `sf-decimal`.
    fn number(&mut self) -> Option<()> {
        self.eat(b'-');
        let integer_digits = self.take_while(|byte| byte.is_ascii_digit());
        if integer_digits == 0 {
            return None;
        }
        if self.eat(b'.') {
            let fraction_digits = self.take_while(|byte| byte.is_ascii_digit());
            ((1..=3).contains(&fraction_digits) && integer_digits <= 12).then_some(())
        } else {
            (integer_digits <= 15).then_some(())
        }
    }

    /// `sf-token = ( ALPHA / "*" ) *( tchar / ":" / "/" )`.
    fn token(&mut self) -> Option<()> {
        let first = self.bump()?;
        if !(first.is_ascii_alphabetic() || first == b'*') {
            return None;
        }
        self.take_while(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                        | b':'
                        | b'/'
                )
        });
        Some(())
    }

    /// `sf-binary = ":" *(base64) ":"`. Padding is not checked, as RFC 9651 allows parsers to be
    /// lenient about it.
    fn binary(&mut self) -> Option<()> {
        self.expect(b':')?;
        self.take_while(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='));
        self.expect(b':')
    }

    /// `sf-boolean = "?" ( "0" / "1" )`.
    fn boolean(&mut self) -> Option<()> {
        self.expect(b'?')?;
        matches!(self.bump()?, b'0' | b'1').then_some(())
    }

    /// `sf-date = "@" sf-integer`.
    fn date(&mut self) -> Option<()> {
        self.expect(b'@')?;
        self.eat(b'-');
        let digits = self.take_while(|byte| byte.is_ascii_digit());
        (1..=15).contains(&digits).then_some(())
    }

    /// `sf-displaystring = "%" DQUOTE *( unescaped / "\" / pct-encoded ) DQUOTE`, whose
    /// percent-encoded bytes must form UTF-8.
    fn display_string(&mut self) -> Option<()> {
        self.expect(b'%')?;
        self.expect(b'"')?;
        let mut decoded = Vec::new();
        loop {
            match self.bump()? {
                b'"' => return std::str::from_utf8(&decoded).is_ok().then_some(()),
                b'%' => {
                    let high = lower_hex_value(self.bump()?)?;
                    let low = lower_hex_value(self.bump()?)?;
                    decoded.push(high.checked_mul(16)?.checked_add(low)?);
                }
                byte @ 0x20..=0x7e => decoded.push(byte),
                _ => return None,
            }
        }
    }
}

const fn lower_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte.wrapping_sub(b'0')),
        b'a'..=b'f' => Some(byte.wrapping_sub(b'a').wrapping_add(10)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_string_list;

    fn list(input: &str) -> Vec<String> {
        parse_string_list(input).expect("value should parse")
    }

    #[test]
    fn parses_a_single_string() {
        assert_eq!(list(r#""trevrpc-1""#), ["trevrpc-1"]);
    }

    #[test]
    fn parses_several_strings_with_optional_whitespace() {
        let expected = ["a", "trevrpc-1", "b"];
        assert_eq!(list(r#""a","trevrpc-1","b""#), expected);
        assert_eq!(list(r#""a", "trevrpc-1", "b""#), expected);
        assert_eq!(list("\"a\" ,\t\"trevrpc-1\"\t,   \"b\""), expected);
    }

    #[test]
    fn strips_leading_spaces_and_trailing_whitespace() {
        assert_eq!(list(r#"   "a"  "#), ["a"]);
        assert_eq!(list("\"a\"\t"), ["a"]);
    }

    #[test]
    fn empty_and_blank_values_are_the_empty_list() {
        assert!(list("").is_empty());
        assert!(list("   ").is_empty());
    }

    #[test]
    fn unescapes_quotes_and_backslashes() {
        assert_eq!(list(r#""a\"b""#), ["a\"b"]);
        assert_eq!(list(r#""a\\b""#), ["a\\b"]);
        assert_eq!(list(r#""\\\"""#), ["\\\""]);
        assert_eq!(list(r#""""#), [""]);
        assert_eq!(list(r#""a, b""#), ["a, b"]);
    }

    #[test]
    fn rejects_bad_strings() {
        for input in [
            r#""unterminated"#,
            r#""bad \n escape""#,
            r#""trailing backslash\"#,
            r#""\"#,
            "\"tab\there\"",
            "\"new\nline\"",
            "\"caf\u{e9}\"",
            "\"\u{7f}\"",
            r"'single'",
        ] {
            assert_eq!(parse_string_list(input), None, "{input:?}");
        }
    }

    #[test]
    fn rejects_bad_separators() {
        for input in [
            r#""a" "b""#,
            r#""a""b""#,
            r#""a","#,
            r#""a", "#,
            r",",
            r#","a""#,
            r#""a",,"b""#,
            r#""a";"#,
            r#""a" x"#,
        ] {
            assert_eq!(parse_string_list(input), None, "{input:?}");
        }
    }

    #[test]
    fn rejects_members_that_are_not_strings() {
        for input in [
            "trevrpc-1",
            "1",
            "?1",
            ":aGk=:",
            r#"("a" "b")"#,
            r#""a", trevrpc-1"#,
            r#""a", 12"#,
        ] {
            assert_eq!(parse_string_list(input), None, "{input:?}");
        }
    }

    #[test]
    fn skips_parameters_of_every_type() {
        assert_eq!(list(r#""a";x"#), ["a"]);
        assert_eq!(
            list(r#""a";x=1;y=-2.5;z="s";t=tok;b=?0;c=?1;d=:aGk=:;e=@12"#),
            ["a"]
        );
        assert_eq!(list(r#""a";  x=1 ,"b";*k-.9_=%"h%c3%a9""#), ["a", "b"]);
        assert_eq!(list(r#""a";x=1;y=2,"b";z"#), ["a", "b"]);
        assert_eq!(list(r#""a";s="semi;colon,comma""#), ["a"]);
        assert_eq!(list(r#""a";t=a/b:c;u=*x"#), ["a"]);
    }

    #[test]
    fn rejects_bad_parameters() {
        for input in [
            r#""a";"#,
            r#""a";=1"#,
            r#""a";X=1"#,
            r#""a";1x"#,
            r#""a";x="#,
            r#""a";x=;y"#,
            r#""a";x=1.1234"#,
            r#""a";x=1."#,
            r#""a";x=1234567890123456"#,
            r#""a";x=1234567890123.5"#,
            r#""a";x=?2"#,
            r#""a";x=:a$:"#,
            r#""a";x=:abc"#,
            r#""a";x=@"#,
            "\"a\";x=%\"\u{e9}\"",
            r#""a";x=%"%C3%A9""#,
            r#""a";x=%"%ff""#,
            r#""a";x=%"%c3""#,
            r#""a";x=%"unterminated"#,
            r#""a";x="unterminated"#,
            r#""a";x=(1)"#,
            "\"a\"\t;x",
        ] {
            assert_eq!(parse_string_list(input), None, "{input:?}");
        }
    }

    #[test]
    fn accepts_numbers_at_their_limits() {
        assert_eq!(list(r#""a";x=999999999999999"#), ["a"]);
        assert_eq!(list(r#""a";x=-999999999999.999"#), ["a"]);
        assert_eq!(list(r#""a";x=@-999999999999999"#), ["a"]);
    }

    #[test]
    fn accepts_percent_encoded_utf8_in_display_strings() {
        assert_eq!(list(r#""a";x=%"caf%c3%a9 \ ok""#), ["a"]);
    }
}
