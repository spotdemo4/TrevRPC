#include "trevrpc_http3_headers_internal.h"

#include <stdint.h>
#include <string.h>

typedef enum trevrpc_http3_pseudo_id {
    TREV_HTTP3_PSEUDO_METHOD = 0,
    TREV_HTTP3_PSEUDO_SCHEME,
    TREV_HTTP3_PSEUDO_AUTHORITY,
    TREV_HTTP3_PSEUDO_PATH,
    TREV_HTTP3_PSEUDO_STATUS,
    TREV_HTTP3_PSEUDO_PROTOCOL,
    TREV_HTTP3_PSEUDO_COUNT,
    TREV_HTTP3_PSEUDO_UNKNOWN,
} trevrpc_http3_pseudo_id;

typedef struct trevrpc_http3_content_length {
    const uint8_t* significant;
    size_t significant_len;
    int present;
} trevrpc_http3_content_length;

typedef enum trevrpc_http3_host_kind {
    TREV_HTTP3_HOST_REG_NAME = 0,
    TREV_HTTP3_HOST_IPV4,
    TREV_HTTP3_HOST_IPV6,
    TREV_HTTP3_HOST_IPVFUTURE,
} trevrpc_http3_host_kind;

typedef struct trevrpc_http3_authority {
    trevrpc_http3_host_kind host_kind;
    trevrpc_qpack_bytes host;
    uint8_t ipv4[4];
    uint16_t ipv6[8];
    uint16_t port;
    int userinfo_present;
    int port_present;
    int port_has_value;
} trevrpc_http3_authority;

static int trevrpc_http3_ascii_alpha(uint8_t byte) {
    return (byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z');
}

static int trevrpc_http3_ascii_hex(uint8_t byte) {
    return (byte >= '0' && byte <= '9') || (byte >= 'A' && byte <= 'F') || (byte >= 'a' && byte <= 'f');
}

static uint8_t trevrpc_http3_ascii_lower(uint8_t byte) {
    if (byte >= 'A' && byte <= 'Z') {
        return (uint8_t)(byte + ('a' - 'A'));
    }
    return byte;
}

static int trevrpc_http3_token_byte(uint8_t byte, int lowercase_only) {
    if (byte >= '0' && byte <= '9') {
        return 1;
    }
    if (byte >= 'a' && byte <= 'z') {
        return 1;
    }
    if (!lowercase_only && byte >= 'A' && byte <= 'Z') {
        return 1;
    }
    switch (byte) {
    case '!':
    case '#':
    case '$':
    case '%':
    case '&':
    case '\'':
    case '*':
    case '+':
    case '-':
    case '.':
    case '^':
    case '_':
    case '`':
    case '|':
    case '~':
        return 1;
    default:
        return 0;
    }
}

static int trevrpc_http3_token(trevrpc_qpack_bytes bytes, int lowercase_only) {
    if (bytes.len == 0 || bytes.data == NULL) {
        return 0;
    }
    for (size_t i = 0; i < bytes.len; i++) {
        if (!trevrpc_http3_token_byte(bytes.data[i], lowercase_only)) {
            return 0;
        }
    }
    return 1;
}

static int trevrpc_http3_scheme_valid(trevrpc_qpack_bytes scheme) {
    if (scheme.len == 0 || scheme.data == NULL || !trevrpc_http3_ascii_alpha(scheme.data[0])) {
        return 0;
    }
    for (size_t i = 1; i < scheme.len; i++) {
        uint8_t byte = scheme.data[i];
        if (!(trevrpc_http3_ascii_alpha(byte) || (byte >= '0' && byte <= '9') || byte == '+' || byte == '-' ||
                byte == '.')) {
            return 0;
        }
    }
    return 1;
}

static int trevrpc_http3_name_valid(trevrpc_qpack_bytes name, int* out_pseudo) {
    if (name.len == 0 || name.data == NULL) {
        return 0;
    }
    *out_pseudo = name.data[0] == ':';
    if (*out_pseudo) {
        trevrpc_qpack_bytes suffix = {.data = name.data + 1, .len = name.len - 1};
        return trevrpc_http3_token(suffix, 1);
    }
    return trevrpc_http3_token(name, 1);
}

static int trevrpc_http3_value_valid(trevrpc_qpack_bytes value) {
    if (value.data == NULL && value.len != 0) {
        return 0;
    }
    if (value.len != 0 && (value.data[0] == ' ' || value.data[0] == '\t' || value.data[value.len - 1] == ' ' ||
                              value.data[value.len - 1] == '\t')) {
        return 0;
    }
    for (size_t i = 0; i < value.len; i++) {
        uint8_t byte = value.data[i];
        if ((byte < 0x20u && byte != '\t') || byte == 0x7fu) {
            return 0;
        }
    }
    return 1;
}

static int trevrpc_http3_bytes_literal(trevrpc_qpack_bytes bytes, const char* literal) {
    size_t literal_len = strlen(literal);
    return bytes.len == literal_len && (literal_len == 0 || memcmp(bytes.data, literal, literal_len) == 0);
}

static int trevrpc_http3_bytes_ascii_case_equal(trevrpc_qpack_bytes left, trevrpc_qpack_bytes right) {
    if (left.len != right.len) {
        return 0;
    }
    for (size_t i = 0; i < left.len; i++) {
        if (trevrpc_http3_ascii_lower(left.data[i]) != trevrpc_http3_ascii_lower(right.data[i])) {
            return 0;
        }
    }
    return 1;
}

static int trevrpc_http3_bytes_ascii_case_literal(trevrpc_qpack_bytes bytes, const char* lowercase_literal) {
    trevrpc_qpack_bytes literal = {
        .data = (const uint8_t*)lowercase_literal,
        .len = strlen(lowercase_literal),
    };
    return trevrpc_http3_bytes_ascii_case_equal(bytes, literal);
}

static trevrpc_http3_pseudo_id trevrpc_http3_pseudo_name(trevrpc_qpack_bytes name) {
    static const char* const names[TREV_HTTP3_PSEUDO_COUNT] = {
        ":method",
        ":scheme",
        ":authority",
        ":path",
        ":status",
        ":protocol",
    };
    for (size_t i = 0; i < TREV_HTTP3_PSEUDO_COUNT; i++) {
        if (trevrpc_http3_bytes_literal(name, names[i])) {
            return (trevrpc_http3_pseudo_id)i;
        }
    }
    return TREV_HTTP3_PSEUDO_UNKNOWN;
}

static int trevrpc_http3_connection_specific(trevrpc_qpack_bytes name) {
    return trevrpc_http3_bytes_literal(name, "connection") || trevrpc_http3_bytes_literal(name, "proxy-connection") ||
           trevrpc_http3_bytes_literal(name, "keep-alive") || trevrpc_http3_bytes_literal(name, "transfer-encoding") ||
           trevrpc_http3_bytes_literal(name, "upgrade");
}

static int trevrpc_http3_trailer_forbidden(trevrpc_qpack_bytes name) {
    return trevrpc_http3_bytes_literal(name, "host") || trevrpc_http3_bytes_literal(name, "content-length") ||
           trevrpc_http3_bytes_literal(name, "trailer");
}

static int trevrpc_http3_content_length_value(
    trevrpc_qpack_bytes value, const uint8_t** out_significant, size_t* out_significant_len) {
    if (value.len == 0) {
        return 0;
    }
    for (size_t i = 0; i < value.len; i++) {
        if (value.data[i] < '0' || value.data[i] > '9') {
            return 0;
        }
    }
    size_t first = 0;
    while (first + 1 < value.len && value.data[first] == '0') {
        first++;
    }
    *out_significant = value.data + first;
    *out_significant_len = value.len - first;
    return 1;
}

static int trevrpc_http3_validate_content_length(
    trevrpc_qpack_bytes value, trevrpc_http3_content_length* content_length) {
    const uint8_t* significant = NULL;
    size_t significant_len = 0;
    if (!trevrpc_http3_content_length_value(value, &significant, &significant_len)) {
        return 0;
    }
    if (!content_length->present) {
        content_length->present = 1;
        content_length->significant = significant;
        content_length->significant_len = significant_len;
        return 1;
    }
    return content_length->significant_len == significant_len &&
           memcmp(content_length->significant, significant, significant_len) == 0;
}

static int trevrpc_http3_te_value(trevrpc_qpack_bytes value) {
    enum { TREV_HTTP3_MAX_IGNORED_EMPTY_TE_MEMBERS = 16 };
    size_t offset = 0;
    size_t empty_members = 0;
    for (;;) {
        while (offset < value.len && (value.data[offset] == ' ' || value.data[offset] == '\t')) {
            offset++;
        }
        size_t begin = offset;
        while (offset < value.len && value.data[offset] != ',') {
            offset++;
        }
        size_t end = offset;
        while (end > begin && (value.data[end - 1] == ' ' || value.data[end - 1] == '\t')) {
            end--;
        }

        if (begin == end) {
            empty_members++;
            if (empty_members > TREV_HTTP3_MAX_IGNORED_EMPTY_TE_MEMBERS) {
                return 0;
            }
        } else {
            trevrpc_qpack_bytes token = {.data = value.data + begin, .len = end - begin};
            if (!trevrpc_http3_bytes_ascii_case_literal(token, "trailers")) {
                return 0;
            }
        }

        if (offset == value.len) {
            return 1;
        }
        offset++;
    }
}

static int trevrpc_http3_unreserved(uint8_t byte) {
    return trevrpc_http3_ascii_alpha(byte) || (byte >= '0' && byte <= '9') || byte == '-' || byte == '.' ||
           byte == '_' || byte == '~';
}

static int trevrpc_http3_sub_delim(uint8_t byte) {
    switch (byte) {
    case '!':
    case '$':
    case '&':
    case '\'':
    case '(':
    case ')':
    case '*':
    case '+':
    case ',':
    case ';':
    case '=':
        return 1;
    default:
        return 0;
    }
}

static int trevrpc_http3_userinfo(trevrpc_qpack_bytes userinfo) {
    if (userinfo.data == NULL && userinfo.len != 0) {
        return 0;
    }
    for (size_t i = 0; i < userinfo.len; i++) {
        uint8_t byte = userinfo.data[i];
        if (trevrpc_http3_unreserved(byte) || trevrpc_http3_sub_delim(byte) || byte == ':') {
            continue;
        }
        if (byte == '%' && i + 2 < userinfo.len && trevrpc_http3_ascii_hex(userinfo.data[i + 1]) &&
            trevrpc_http3_ascii_hex(userinfo.data[i + 2])) {
            i += 2;
            continue;
        }
        return 0;
    }
    return 1;
}

static int trevrpc_http3_reg_name(trevrpc_qpack_bytes host) {
    if (host.len == 0 || host.data == NULL) {
        return 0;
    }
    for (size_t i = 0; i < host.len; i++) {
        uint8_t byte = host.data[i];
        if (trevrpc_http3_unreserved(byte) || trevrpc_http3_sub_delim(byte)) {
            continue;
        }
        if (byte == '%' && i + 2 < host.len && trevrpc_http3_ascii_hex(host.data[i + 1]) &&
            trevrpc_http3_ascii_hex(host.data[i + 2])) {
            i += 2;
            continue;
        }
        return 0;
    }
    return 1;
}

static int trevrpc_http3_ipv4(trevrpc_qpack_bytes bytes, uint8_t out[4]) {
    size_t offset = 0;
    for (size_t part = 0; part < 4; part++) {
        size_t begin = offset;
        unsigned value = 0;
        while (offset < bytes.len && bytes.data[offset] >= '0' && bytes.data[offset] <= '9') {
            value = value * 10u + (unsigned)(bytes.data[offset] - '0');
            if (value > 255u) {
                return 0;
            }
            offset++;
        }
        if (offset == begin || (offset - begin > 1 && bytes.data[begin] == '0')) {
            return 0;
        }
        out[part] = (uint8_t)value;
        if (part != 3) {
            if (offset == bytes.len || bytes.data[offset] != '.') {
                return 0;
            }
            offset++;
        }
    }
    return offset == bytes.len;
}

static int trevrpc_http3_hextet(trevrpc_qpack_bytes bytes, uint16_t* out) {
    if (bytes.len == 0 || bytes.len > 4) {
        return 0;
    }
    uint16_t value = 0;
    for (size_t i = 0; i < bytes.len; i++) {
        uint8_t byte = bytes.data[i];
        unsigned digit = 0;
        if (byte >= '0' && byte <= '9') {
            digit = (unsigned)(byte - '0');
        } else if (byte >= 'A' && byte <= 'F') {
            digit = (unsigned)(byte - 'A' + 10);
        } else if (byte >= 'a' && byte <= 'f') {
            digit = (unsigned)(byte - 'a' + 10);
        } else {
            return 0;
        }
        value = (uint16_t)(((unsigned)value << 4u) | digit);
    }
    *out = value;
    return 1;
}

static int trevrpc_http3_ipv6_part(trevrpc_qpack_bytes part, int allow_ipv4, uint16_t out[8], size_t* out_count) {
    *out_count = 0;
    if (part.len == 0) {
        return 1;
    }
    size_t offset = 0;
    while (offset < part.len) {
        size_t end = offset;
        while (end < part.len && part.data[end] != ':') {
            end++;
        }
        if (end == offset) {
            return 0;
        }
        trevrpc_qpack_bytes item = {.data = part.data + offset, .len = end - offset};
        int dotted = memchr(item.data, '.', item.len) != NULL;
        if (dotted) {
            uint8_t ipv4[4] = {0};
            if (!allow_ipv4 || end != part.len || *out_count > 6 || !trevrpc_http3_ipv4(item, ipv4)) {
                return 0;
            }
            out[(*out_count)++] = (uint16_t)(((uint16_t)ipv4[0] << 8u) | ipv4[1]);
            out[(*out_count)++] = (uint16_t)(((uint16_t)ipv4[2] << 8u) | ipv4[3]);
        } else {
            if (*out_count == 8 || !trevrpc_http3_hextet(item, &out[*out_count])) {
                return 0;
            }
            (*out_count)++;
        }
        if (end == part.len) {
            break;
        }
        offset = end + 1;
        if (offset == part.len) {
            return 0;
        }
    }
    return 1;
}

static int trevrpc_http3_ipv6(trevrpc_qpack_bytes bytes, uint16_t out[8]) {
    if (bytes.len == 0 || bytes.data == NULL) {
        return 0;
    }
    size_t compression = bytes.len;
    for (size_t i = 0; i + 1 < bytes.len; i++) {
        if (bytes.data[i] == ':' && bytes.data[i + 1] == ':') {
            if (compression != bytes.len) {
                return 0;
            }
            compression = i;
            i++;
        }
    }

    uint16_t left[8] = {0};
    uint16_t right[8] = {0};
    size_t left_count = 0;
    size_t right_count = 0;
    if (compression == bytes.len) {
        if (!trevrpc_http3_ipv6_part(bytes, 1, left, &left_count) || left_count != 8) {
            return 0;
        }
        memcpy(out, left, sizeof(left));
        return 1;
    }

    trevrpc_qpack_bytes left_part = {.data = bytes.data, .len = compression};
    trevrpc_qpack_bytes right_part = {
        .data = bytes.data + compression + 2,
        .len = bytes.len - compression - 2,
    };
    if (!trevrpc_http3_ipv6_part(left_part, 0, left, &left_count) ||
        !trevrpc_http3_ipv6_part(right_part, 1, right, &right_count) || left_count + right_count >= 8) {
        return 0;
    }
    size_t zero_count = 8 - left_count - right_count;
    memcpy(out, left, left_count * sizeof(left[0]));
    memset(out + left_count, 0, zero_count * sizeof(out[0]));
    memcpy(out + left_count + zero_count, right, right_count * sizeof(right[0]));
    return 1;
}

static int trevrpc_http3_ipvfuture(trevrpc_qpack_bytes bytes) {
    if (bytes.len < 4 || (bytes.data[0] != 'v' && bytes.data[0] != 'V')) {
        return 0;
    }
    size_t offset = 1;
    size_t version_begin = offset;
    while (offset < bytes.len && trevrpc_http3_ascii_hex(bytes.data[offset])) {
        offset++;
    }
    if (offset == version_begin || offset == bytes.len || bytes.data[offset] != '.') {
        return 0;
    }
    offset++;
    size_t address_begin = offset;
    while (offset < bytes.len) {
        uint8_t byte = bytes.data[offset];
        if (!(trevrpc_http3_unreserved(byte) || trevrpc_http3_sub_delim(byte) || byte == ':')) {
            return 0;
        }
        offset++;
    }
    return offset != address_begin;
}

static int trevrpc_http3_port(trevrpc_qpack_bytes bytes, uint16_t* out) {
    if (bytes.len == 0 || bytes.data == NULL) {
        return 0;
    }
    unsigned value = 0;
    for (size_t i = 0; i < bytes.len; i++) {
        if (bytes.data[i] < '0' || bytes.data[i] > '9') {
            return 0;
        }
        value = value * 10u + (unsigned)(bytes.data[i] - '0');
        if (value > 65535u) {
            return 0;
        }
    }
    *out = (uint16_t)value;
    return 1;
}

static int trevrpc_http3_authority_parse(trevrpc_qpack_bytes value, int allow_userinfo, trevrpc_http3_authority* out) {
    *out = (trevrpc_http3_authority){0};
    if (value.len == 0 || value.data == NULL) {
        return 0;
    }

    size_t at = value.len;
    for (size_t i = 0; i < value.len; i++) {
        if (value.data[i] == '@') {
            if (!allow_userinfo || at != value.len) {
                return 0;
            }
            at = i;
        }
    }
    if (at != value.len) {
        trevrpc_qpack_bytes userinfo = {.data = value.data, .len = at};
        if (!trevrpc_http3_userinfo(userinfo)) {
            return 0;
        }
        out->userinfo_present = 1;
        value.data += at + 1;
        value.len -= at + 1;
        if (value.len == 0) {
            return 0;
        }
    }

    trevrpc_qpack_bytes host = {0};
    trevrpc_qpack_bytes port = {0};
    if (value.data[0] == '[') {
        size_t close = 1;
        while (close < value.len && value.data[close] != ']') {
            close++;
        }
        if (close == value.len) {
            return 0;
        }
        host = (trevrpc_qpack_bytes){.data = value.data + 1, .len = close - 1};
        if (close + 1 < value.len) {
            if (value.data[close + 1] != ':') {
                return 0;
            }
            port = (trevrpc_qpack_bytes){.data = value.data + close + 2, .len = value.len - close - 2};
            out->port_present = 1;
        }
        if (trevrpc_http3_ipv6(host, out->ipv6)) {
            out->host_kind = TREV_HTTP3_HOST_IPV6;
        } else if (trevrpc_http3_ipvfuture(host)) {
            out->host_kind = TREV_HTTP3_HOST_IPVFUTURE;
        } else {
            return 0;
        }
    } else {
        size_t colon = value.len;
        for (size_t i = 0; i < value.len; i++) {
            if (value.data[i] == '[' || value.data[i] == ']' || value.data[i] == '/' || value.data[i] == '?' ||
                value.data[i] == '#') {
                return 0;
            }
            if (value.data[i] == ':') {
                if (colon != value.len) {
                    return 0;
                }
                colon = i;
            }
        }
        host = (trevrpc_qpack_bytes){.data = value.data, .len = colon};
        if (colon != value.len) {
            port = (trevrpc_qpack_bytes){.data = value.data + colon + 1, .len = value.len - colon - 1};
            out->port_present = 1;
        }
        if (!trevrpc_http3_reg_name(host)) {
            return 0;
        }
        if (trevrpc_http3_ipv4(host, out->ipv4)) {
            out->host_kind = TREV_HTTP3_HOST_IPV4;
        } else {
            out->host_kind = TREV_HTTP3_HOST_REG_NAME;
        }
    }

    if (out->port_present && port.len != 0) {
        if (!trevrpc_http3_port(port, &out->port)) {
            return 0;
        }
        out->port_has_value = 1;
    }
    out->host = host;
    return 1;
}

static int trevrpc_http3_authority_host_equal(
    const trevrpc_http3_authority* left, const trevrpc_http3_authority* right) {
    if (left->host_kind != right->host_kind) {
        return 0;
    }
    switch (left->host_kind) {
    case TREV_HTTP3_HOST_IPV4:
        return memcmp(left->ipv4, right->ipv4, sizeof(left->ipv4)) == 0;
    case TREV_HTTP3_HOST_IPV6:
        return memcmp(left->ipv6, right->ipv6, sizeof(left->ipv6)) == 0;
    case TREV_HTTP3_HOST_REG_NAME:
    case TREV_HTTP3_HOST_IPVFUTURE:
        return trevrpc_http3_bytes_ascii_case_equal(left->host, right->host);
    default:
        return 0;
    }
}

static int trevrpc_http3_default_port(trevrpc_qpack_bytes scheme, uint16_t* out) {
    if (trevrpc_http3_bytes_ascii_case_literal(scheme, "http")) {
        *out = 80;
        return 1;
    }
    if (trevrpc_http3_bytes_ascii_case_literal(scheme, "https")) {
        *out = 443;
        return 1;
    }
    return 0;
}

static int trevrpc_http3_authority_equal(
    const trevrpc_http3_authority* left, const trevrpc_http3_authority* right, trevrpc_qpack_bytes scheme) {
    if (!trevrpc_http3_authority_host_equal(left, right)) {
        return 0;
    }

    uint16_t default_port = 0;
    if (trevrpc_http3_default_port(scheme, &default_port)) {
        uint16_t left_port = left->port_has_value ? left->port : default_port;
        uint16_t right_port = right->port_has_value ? right->port : default_port;
        return left_port == right_port;
    }

    if (left->port_present != right->port_present) {
        return 0;
    }
    if (!left->port_present) {
        return 1;
    }
    if (left->port_has_value != right->port_has_value) {
        return 0;
    }
    return !left->port_has_value || left->port == right->port;
}

static int trevrpc_http3_path_char(uint8_t byte) {
    return trevrpc_http3_unreserved(byte) || trevrpc_http3_sub_delim(byte) || byte == ':' || byte == '@';
}

static int trevrpc_http3_request_path(trevrpc_qpack_bytes path, trevrpc_qpack_bytes method) {
    if (trevrpc_http3_bytes_literal(path, "*")) {
        return trevrpc_http3_bytes_literal(method, "OPTIONS");
    }
    if (path.len == 0 || path.data == NULL || path.data[0] != '/') {
        return 0;
    }
    int query = 0;
    for (size_t i = 0; i < path.len; i++) {
        uint8_t byte = path.data[i];
        if (byte == '#') {
            return 0;
        }
        if (byte == '?') {
            if (!query) {
                query = 1;
            }
            continue;
        }
        if (byte == '/') {
            continue;
        }
        if (trevrpc_http3_path_char(byte)) {
            continue;
        }
        if (byte == '%' && i + 2 < path.len && trevrpc_http3_ascii_hex(path.data[i + 1]) &&
            trevrpc_http3_ascii_hex(path.data[i + 2])) {
            i += 2;
            continue;
        }
        return 0;
    }
    return 1;
}

static int trevrpc_http3_request_pseudo_allowed(trevrpc_http3_pseudo_id pseudo) {
    return pseudo == TREV_HTTP3_PSEUDO_METHOD || pseudo == TREV_HTTP3_PSEUDO_SCHEME ||
           pseudo == TREV_HTTP3_PSEUDO_AUTHORITY || pseudo == TREV_HTTP3_PSEUDO_PATH ||
           pseudo == TREV_HTTP3_PSEUDO_PROTOCOL;
}

static int trevrpc_http3_validate_request_semantics(const trevrpc_qpack_bytes pseudo_values[TREV_HTTP3_PSEUDO_COUNT],
    uint32_t seen,
    int host_present,
    trevrpc_qpack_bytes host_value) {
    uint32_t method_bit = 1u << TREV_HTTP3_PSEUDO_METHOD;
    uint32_t scheme_bit = 1u << TREV_HTTP3_PSEUDO_SCHEME;
    uint32_t authority_bit = 1u << TREV_HTTP3_PSEUDO_AUTHORITY;
    uint32_t path_bit = 1u << TREV_HTTP3_PSEUDO_PATH;
    uint32_t protocol_bit = 1u << TREV_HTTP3_PSEUDO_PROTOCOL;
    trevrpc_qpack_bytes method = pseudo_values[TREV_HTTP3_PSEUDO_METHOD];
    trevrpc_qpack_bytes scheme = pseudo_values[TREV_HTTP3_PSEUDO_SCHEME];
    trevrpc_qpack_bytes authority_value = pseudo_values[TREV_HTTP3_PSEUDO_AUTHORITY];
    trevrpc_qpack_bytes path = pseudo_values[TREV_HTTP3_PSEUDO_PATH];

    if ((seen & method_bit) == 0 || !trevrpc_http3_token(method, 0)) {
        return 0;
    }

    int connect = trevrpc_http3_bytes_literal(method, "CONNECT");
    int scheme_present = (seen & scheme_bit) != 0;
    int scheme_valid = scheme_present && trevrpc_http3_scheme_valid(scheme);
    int http_scheme = trevrpc_http3_bytes_ascii_case_literal(scheme, "http") ||
                      trevrpc_http3_bytes_ascii_case_literal(scheme, "https");
    int allow_authority_userinfo = scheme_valid && !http_scheme;

    trevrpc_http3_authority authority = {0};
    trevrpc_http3_authority host = {0};
    int authority_present = (seen & authority_bit) != 0;
    if (authority_present && !trevrpc_http3_authority_parse(authority_value, allow_authority_userinfo, &authority)) {
        return 0;
    }
    if (host_present && !trevrpc_http3_authority_parse(host_value, 0, &host)) {
        return 0;
    }
    if (host_present && authority.userinfo_present) {
        /* Host has no userinfo production; do not compare it to a stripped authority. */
        return 0;
    }
    if ((seen & protocol_bit) != 0) {
        if (!connect || (seen & (scheme_bit | authority_bit | path_bit)) != (scheme_bit | authority_bit | path_bit) ||
            !scheme_valid || !trevrpc_http3_token(pseudo_values[TREV_HTTP3_PSEUDO_PROTOCOL], 0) || path.len == 0) {
            return 0;
        }
        if (host_present && !trevrpc_http3_authority_equal(&authority, &host, scheme)) {
            return 0;
        }
        if (http_scheme && !trevrpc_http3_request_path(path, method)) {
            return 0;
        }
        return 1;
    }

    if (connect) {
        if (!authority_present || (seen & (scheme_bit | path_bit)) != 0 || !authority.port_has_value) {
            return 0;
        }
        return !host_present || trevrpc_http3_authority_equal(&authority, &host, scheme);
    }

    if ((seen & (scheme_bit | path_bit)) != (scheme_bit | path_bit) || !scheme_valid || path.len == 0) {
        return 0;
    }
    if (authority_present && host_present && !trevrpc_http3_authority_equal(&authority, &host, scheme)) {
        return 0;
    }
    if (http_scheme) {
        return (authority_present || host_present) && trevrpc_http3_request_path(path, method);
    }
    return 1;
}

trevrpc_http3_headers_status trevrpc_http3_headers_validate(const trevrpc_qpack_field_section* section,
    trevrpc_http3_header_block_kind kind,
    trevrpc_http3_headers_report* out_report) {
    if (out_report == NULL) {
        return TREV_HTTP3_HEADERS_INVALID_ARGUMENT;
    }
    *out_report = (trevrpc_http3_headers_report){0};
    if (section == NULL || (section->fields == NULL && section->field_count != 0) ||
        (kind != TREV_HTTP3_HEADERS_REQUEST && kind != TREV_HTTP3_HEADERS_RESPONSE &&
            kind != TREV_HTTP3_HEADERS_TRAILERS)) {
        return TREV_HTTP3_HEADERS_INVALID_ARGUMENT;
    }

    uint32_t seen_pseudo = 0;
    int regular_seen = 0;
    int host_present = 0;
    trevrpc_qpack_bytes host_value = {0};
    trevrpc_qpack_bytes pseudo_values[TREV_HTTP3_PSEUDO_COUNT] = {{0}};
    trevrpc_http3_content_length content_length = {0};
    trevrpc_http3_headers_report report = {0};

    for (size_t i = 0; i < section->field_count; i++) {
        const trevrpc_qpack_field* field = &section->fields[i];
        int pseudo = 0;
        if (!trevrpc_http3_name_valid(field->name, &pseudo) || !trevrpc_http3_value_valid(field->value)) {
            return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
        }
        if (pseudo) {
            if (regular_seen || kind == TREV_HTTP3_HEADERS_TRAILERS) {
                return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
            }
            trevrpc_http3_pseudo_id id = trevrpc_http3_pseudo_name(field->name);
            if (id == TREV_HTTP3_PSEUDO_UNKNOWN || (seen_pseudo & (1u << id)) != 0 ||
                (kind == TREV_HTTP3_HEADERS_REQUEST && !trevrpc_http3_request_pseudo_allowed(id)) ||
                (kind == TREV_HTTP3_HEADERS_RESPONSE && id != TREV_HTTP3_PSEUDO_STATUS)) {
                return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
            }
            seen_pseudo |= 1u << id;
            pseudo_values[id] = field->value;
            report.pseudo_field_count++;
            continue;
        }

        regular_seen = 1;
        report.regular_field_count++;
        if (trevrpc_http3_connection_specific(field->name) ||
            (kind == TREV_HTTP3_HEADERS_TRAILERS && trevrpc_http3_trailer_forbidden(field->name))) {
            return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
        }
        if (trevrpc_http3_bytes_literal(field->name, "host")) {
            if (kind == TREV_HTTP3_HEADERS_REQUEST) {
                if (host_present) {
                    return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
                }
                host_present = 1;
                host_value = field->value;
            }
        }
        if (trevrpc_http3_bytes_literal(field->name, "te")) {
            if (kind != TREV_HTTP3_HEADERS_REQUEST || !trevrpc_http3_te_value(field->value)) {
                return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
            }
        }
        if (trevrpc_http3_bytes_literal(field->name, "content-length")) {
            if (kind == TREV_HTTP3_HEADERS_TRAILERS ||
                !trevrpc_http3_validate_content_length(field->value, &content_length)) {
                return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
            }
        }
    }

    if (kind == TREV_HTTP3_HEADERS_REQUEST) {
        if (!trevrpc_http3_validate_request_semantics(pseudo_values, seen_pseudo, host_present, host_value)) {
            return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
        }
    } else if (kind == TREV_HTTP3_HEADERS_RESPONSE) {
        uint32_t status_bit = 1u << TREV_HTTP3_PSEUDO_STATUS;
        trevrpc_qpack_bytes status = pseudo_values[TREV_HTTP3_PSEUDO_STATUS];
        if ((seen_pseudo & status_bit) == 0 || status.len != 3 || status.data[0] < '0' || status.data[0] > '9' ||
            status.data[1] < '0' || status.data[1] > '9' || status.data[2] < '0' || status.data[2] > '9') {
            return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
        }
        report.response_status = (status.data[0] - '0') * 100 + (status.data[1] - '0') * 10 + (status.data[2] - '0');
        if (report.response_status < 100 || report.response_status > 599 || report.response_status == 101) {
            /* HTTP/3 has no protocol-switch mechanism and does not support 101. */
            return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
        }
        if (content_length.present && (report.response_status < 200 || report.response_status == 204)) {
            return TREV_HTTP3_HEADERS_MESSAGE_ERROR;
        }
    }
    report.has_content_length = content_length.present;
    *out_report = report;
    return TREV_HTTP3_HEADERS_OK;
}

uint64_t trevrpc_http3_headers_status_error_code(trevrpc_http3_headers_status status) {
    switch (status) {
    case TREV_HTTP3_HEADERS_MESSAGE_ERROR:
        return TREV_H3_ERROR_MESSAGE_ERROR;
    case TREV_HTTP3_HEADERS_OK:
    case TREV_HTTP3_HEADERS_INVALID_ARGUMENT:
        return 0;
    }
    return 0;
}
