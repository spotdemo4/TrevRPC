package trevrpc

import (
	"strconv"
	"strings"
)

// SameOriginAdmission returns an Admission for a server that serves its own
// web app. It admits:
//
//   - requests without an Origin header, which do not come from a web page,
//     so cross-site WebSocket hijacking does not apply to them;
//   - pages from the server's own origin: an Origin whose scheme is https
//     (http when the request is not Secure) and whose host and port are the
//     request's Authority;
//   - pages from allowedOrigins, such as "http://localhost:3000" for a
//     development server.
//
// Origins compare as browsers serialize them: the scheme and host ignore
// case, and a default port (443 for https, 80 for http) equals no port. The
// opaque origin "null" is never admitted. Behind a proxy that terminates TLS
// the request is not Secure, so the server's own https origin is admitted only
// when listed in allowedOrigins.
//
// It panics if an allowed origin is not of the form scheme://host[:port].
func SameOriginAdmission(allowedOrigins ...string) Admission {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, origin := range allowedOrigins {
		normalized, ok := parseOrigin(origin)
		if !ok {
			panic("trevrpc: SameOriginAdmission: invalid origin " + strconv.Quote(origin))
		}
		allowed[normalized] = struct{}{}
	}

	return func(request AdmissionRequest) bool {
		if request.Origin == "" {
			return true
		}
		origin, ok := parseOrigin(request.Origin)
		if !ok {
			return false
		}
		scheme := "http"
		if request.Secure {
			scheme = "https"
		}
		if own, ok := normalizeOrigin(scheme, request.Authority); ok && origin == own {
			return true
		}
		_, ok = allowed[origin]
		return ok
	}
}

// parseOrigin returns the normalized form of an origin serialized as
// scheme://host[:port] (see normalizeOrigin), or false when origin is not one,
// such as the opaque origin "null".
func parseOrigin(origin string) (string, bool) {
	scheme, hostport, ok := strings.Cut(origin, "://")
	if !ok {
		return "", false
	}
	return normalizeOrigin(scheme, hostport)
}

// normalizeOrigin returns scheme://host[:port] with the scheme and host in
// lower case and the port in decimal, omitted when it is the scheme's default,
// or false when scheme or hostport is malformed.
func normalizeOrigin(scheme, hostport string) (string, bool) {
	if scheme == "" || strings.ContainsFunc(scheme, invalidSchemeRune) {
		return "", false
	}
	scheme = strings.ToLower(scheme)
	if hostport == "" || strings.ContainsAny(hostport, "/?#@\\ ") {
		return "", false
	}

	host, port := hostport, ""
	if rest, ok := strings.CutPrefix(hostport, "["); ok {
		// An IPv6 address, which must be in brackets.
		var after string
		var closed bool
		host, after, closed = strings.Cut(rest, "]")
		if !closed || !strings.Contains(host, ":") {
			return "", false
		}
		if after != "" {
			if port, ok = strings.CutPrefix(after, ":"); !ok || port == "" {
				return "", false
			}
		}
	} else if host, port, ok = strings.Cut(hostport, ":"); ok && (port == "" || strings.Contains(port, ":")) {
		return "", false
	}
	if host == "" {
		return "", false
	}

	if port != "" {
		number, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return "", false
		}
		if number == defaultPort(scheme) {
			port = ""
		} else {
			port = strconv.FormatUint(number, 10)
		}
	}

	host = strings.ToLower(host)
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port == "" {
		return scheme + "://" + host, true
	}
	return scheme + "://" + host + ":" + port, true
}

// invalidSchemeRune reports whether r may not appear in a URL scheme.
func invalidSchemeRune(r rune) bool {
	switch {
	case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9', r == '+', r == '-', r == '.':
		return false
	default:
		return true
	}
}

// defaultPort returns the port a URL of scheme has when it names none, or 0.
func defaultPort(scheme string) uint64 {
	switch scheme {
	case "https", "wss":
		return 443
	case "http", "ws":
		return 80
	default:
		return 0
	}
}
