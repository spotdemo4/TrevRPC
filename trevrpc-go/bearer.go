package trevrpc

// BearerMetadata returns the request metadata that carries token as bearer
// credentials, for WithMetadataEntries, DialOptions.ConnectionMetadata and
// Channel.SetConnectionMetadata: the authorization key with the value "Bearer "
// and the token. It is the counterpart of BearerToken, and accepts the tokens
// that parses, the RFC 6750 b64token syntax: one or more ASCII letters, digits
// and the characters -._~+/, followed by any number of =. It returns an
// InvalidArgument status for any other token, such as an empty one or one with
// whitespace, which a server would reject as Unauthenticated, and which a
// client could not tell from an expired session.
func BearerMetadata(token string) (Metadata, error) {
	const prefix = "Bearer "
	value := []byte(prefix + token)
	if !isB64Token(value[len(prefix):]) {
		return nil, InvalidArgument("bearer token is not a b64token")
	}
	return Metadata{"authorization": value}, nil
}

// BearerToken returns the token of the request's authorization metadata, which
// must hold bearer credentials as RFC 6750 defines them: the case-insensitive
// scheme "Bearer", one or more spaces, and the token. It returns an
// Unauthenticated status when the metadata is missing or holds anything else,
// with a message that tells the two apart.
func BearerToken(metadata Metadata) (string, error) {
	value, ok := metadata["authorization"]
	if !ok {
		return "", Unauthenticated("missing bearer token")
	}
	const scheme = "bearer"
	if len(value) <= len(scheme) || !asciiEqualFold(value[:len(scheme)], scheme) || value[len(scheme)] != ' ' {
		return "", Unauthenticated("malformed bearer token")
	}
	token := value[len(scheme)+1:]
	for len(token) > 0 && token[0] == ' ' {
		token = token[1:]
	}
	if !isB64Token(token) {
		return "", Unauthenticated("malformed bearer token")
	}
	return string(token), nil
}

// asciiEqualFold reports whether value equals the lower-case ASCII lower,
// ignoring ASCII case.
func asciiEqualFold(value []byte, lower string) bool {
	for i := range len(lower) {
		c := value[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

// isB64Token reports whether token matches RFC 6750's b64token:
// 1*( ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/" ) *"=".
func isB64Token(token []byte) bool {
	end := len(token)
	for end > 0 && token[end-1] == '=' {
		end--
	}
	if end == 0 {
		return false
	}
	for _, c := range token[:end] {
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case c == '-', c == '.', c == '_', c == '~', c == '+', c == '/':
		default:
			return false
		}
	}
	return true
}
