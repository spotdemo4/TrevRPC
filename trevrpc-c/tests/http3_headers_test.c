#include "trevrpc_http3_headers_internal.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

#define FIELD(name_literal, value_literal)                                                                             \
    (trevrpc_qpack_field) {                                                                                            \
        .name = {(const uint8_t*)(name_literal), sizeof(name_literal) - 1},                                            \
        .value = {(const uint8_t*)(value_literal), sizeof(value_literal) - 1},                                         \
    }

static trevrpc_qpack_field byte_field(const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len) {
    return (trevrpc_qpack_field){
        .name = {.data = name, .len = name_len},
        .value = {.data = value, .len = value_len},
    };
}

static trevrpc_http3_headers_status validate(const trevrpc_qpack_field* fields,
    size_t field_count,
    trevrpc_http3_header_block_kind kind,
    trevrpc_http3_headers_report* report) {
    trevrpc_qpack_field_section section = {
        .fields = (trevrpc_qpack_field*)fields,
        .field_count = field_count,
    };
    return trevrpc_http3_headers_validate(&section, kind, report);
}

static trevrpc_http3_headers_status validate_authority(
    const char* authority, const char* scheme, trevrpc_http3_headers_report* report) {
    trevrpc_qpack_field request[] = {
        FIELD(":method", "GET"),
        byte_field((const uint8_t*)":scheme", 7, (const uint8_t*)scheme, strlen(scheme)),
        byte_field((const uint8_t*)":authority", 10, (const uint8_t*)authority, strlen(authority)),
        FIELD(":path", "/"),
    };
    return validate(request, sizeof(request) / sizeof(request[0]), TREV_HTTP3_HEADERS_REQUEST, report);
}

static trevrpc_http3_headers_status validate_path(
    const char* method, const char* path, trevrpc_http3_headers_report* report) {
    trevrpc_qpack_field request[] = {
        byte_field((const uint8_t*)":method", 7, (const uint8_t*)method, strlen(method)),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        byte_field((const uint8_t*)":path", 5, (const uint8_t*)path, strlen(path)),
    };
    return validate(request, sizeof(request) / sizeof(request[0]), TREV_HTTP3_HEADERS_REQUEST, report);
}

static int test_valid_request_response_and_trailers(void) {
    const trevrpc_qpack_field request[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
        FIELD("accept", "*/*"),
    };
    trevrpc_http3_headers_report report = {0};
    CHECK(validate(request, sizeof(request) / sizeof(request[0]), TREV_HTTP3_HEADERS_REQUEST, &report) ==
          TREV_HTTP3_HEADERS_OK);
    CHECK(report.pseudo_field_count == 4 && report.regular_field_count == 1);
    CHECK(report.response_status == 0 && !report.has_content_length);

    const trevrpc_qpack_field response[] = {
        FIELD(":status", "204"),
        FIELD("server", "trevrpc"),
    };
    CHECK(validate(response, sizeof(response) / sizeof(response[0]), TREV_HTTP3_HEADERS_RESPONSE, &report) ==
          TREV_HTTP3_HEADERS_OK);
    CHECK(report.response_status == 204);

    const trevrpc_qpack_field trailers[] = {
        FIELD("checksum", "abc123"),
        FIELD("x-note", "ok\twith internal tab"),
    };
    CHECK(validate(trailers, sizeof(trailers) / sizeof(trailers[0]), TREV_HTTP3_HEADERS_TRAILERS, &report) ==
          TREV_HTTP3_HEADERS_OK);
    CHECK(report.pseudo_field_count == 0 && report.regular_field_count == 2);
    return 0;
}

static int test_name_validation(void) {
    static const uint8_t nul_name[] = {'x', 0, 'y'};
    static const uint8_t non_ascii_name[] = {'x', 0x80};
    const struct {
        const uint8_t* name;
        size_t len;
    } invalid[] = {
        {(const uint8_t*)"", 0},
        {(const uint8_t*)"Content-Type", 12},
        {(const uint8_t*)"bad name", 8},
        {(const uint8_t*)"bad/name", 8},
        {(const uint8_t*)"bad:name", 8},
        {(const uint8_t*)"::method", 8},
        {nul_name, sizeof(nul_name)},
        {non_ascii_name, sizeof(non_ascii_name)},
    };
    trevrpc_http3_headers_report report = {0};
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        trevrpc_qpack_field field = byte_field(invalid[i].name, invalid[i].len, (const uint8_t*)"ok", 2);
        CHECK(validate(&field, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
        CHECK(report.pseudo_field_count == 0 && report.regular_field_count == 0);
    }

    const trevrpc_qpack_field valid[] = {
        FIELD("x!#$%&'*+-.^_`|~09az", "ok"),
    };
    CHECK(validate(valid, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_OK);
    return 0;
}

static int test_value_validation_and_whitespace_boundaries(void) {
    static const uint8_t invalid_values[][3] = {
        {'a', 0, 'b'},
        {'a', '\r', 'b'},
        {'a', '\n', 'b'},
        {'a', 0x01, 'b'},
        {'a', 0x7f, 'b'},
    };
    trevrpc_http3_headers_report report = {0};
    for (size_t i = 0; i < sizeof(invalid_values) / sizeof(invalid_values[0]); i++) {
        trevrpc_qpack_field field = byte_field((const uint8_t*)"x", 1, invalid_values[i], sizeof(invalid_values[i]));
        CHECK(validate(&field, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }

    const char* const invalid_whitespace[] = {" value", "\tvalue", "value ", "value\t", " ", "\t", " \t "};
    for (size_t i = 0; i < sizeof(invalid_whitespace) / sizeof(invalid_whitespace[0]); i++) {
        trevrpc_qpack_field field =
            byte_field((const uint8_t*)"x", 1, (const uint8_t*)invalid_whitespace[i], strlen(invalid_whitespace[i]));
        CHECK(validate(&field, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }

    const trevrpc_qpack_field valid[] = {
        FIELD("x-empty", ""),
        FIELD("x-space", "a value with internal spaces"),
        FIELD("x-tab", "a\tvalue\twith\ttabs"),
    };
    CHECK(validate(valid, sizeof(valid) / sizeof(valid[0]), TREV_HTTP3_HEADERS_TRAILERS, &report) ==
          TREV_HTTP3_HEADERS_OK);

    trevrpc_qpack_field null_empty = byte_field((const uint8_t*)"x", 1, NULL, 0);
    CHECK(validate(&null_empty, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_OK);
    trevrpc_qpack_field invalid_null = byte_field((const uint8_t*)"x", 1, NULL, 1);
    CHECK(validate(&invalid_null, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    return 0;
}

static int test_http_authority_or_host_requirements(void) {
    trevrpc_http3_headers_report report = {0};
    const trevrpc_qpack_field authority_only[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
    };
    CHECK(validate(authority_only, 4, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field host_only[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "http"),
        FIELD(":path", "/"),
        FIELD("host", "example.com"),
    };
    CHECK(validate(host_only, 4, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field matching_both[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "EXAMPLE.com"),
        FIELD(":path", "/"),
        FIELD("host", "example.COM:443"),
    };
    CHECK(validate(matching_both, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field matching_ipv6[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "[2001:db8::1]:443"),
        FIELD(":path", "/"),
        FIELD("host", "[2001:0DB8:0:0:0:0:0:1]"),
    };
    CHECK(validate(matching_ipv6, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field matching_empty_default_port[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com:"),
        FIELD(":path", "/"),
        FIELD("host", "example.com:443"),
    };
    CHECK(validate(matching_empty_default_port, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field empty_default_port_host_only[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "http"),
        FIELD(":path", "/"),
        FIELD("host", "example.com:"),
    };
    CHECK(validate(empty_default_port_host_only, 4, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field custom_userinfo[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "custom+rpc"),
        FIELD(":authority", "user:pass@example.com:7443"),
        FIELD(":path", "/resource"),
    };
    CHECK(validate(custom_userinfo, 4, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field custom_userinfo_with_host[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "custom+rpc"),
        FIELD(":authority", "user:pass@example.com:7443"),
        FIELD(":path", "/resource"),
        FIELD("host", "EXAMPLE.COM:7443"),
    };
    CHECK(validate(custom_userinfo_with_host, 5, TREV_HTTP3_HEADERS_REQUEST, &report) ==
          TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field userinfo_in_host[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "custom+rpc"),
        FIELD(":path", "/resource"),
        FIELD("host", "user@example.com"),
    };
    CHECK(validate(userinfo_in_host, 4, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field missing_both[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":path", "/"),
    };
    CHECK(validate(missing_both, 3, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field mismatched_host[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
        FIELD("host", "other.example"),
    };
    CHECK(validate(mismatched_host, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field mismatched_port[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com:444"),
        FIELD(":path", "/"),
        FIELD("host", "example.com"),
    };
    CHECK(validate(mismatched_port, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field duplicate_host[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":path", "/"),
        FIELD("host", "example.com"),
        FIELD("host", "example.com"),
    };
    CHECK(validate(duplicate_host, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field duplicate_authority[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
    };
    CHECK(validate(duplicate_authority, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field empty_authority_with_host[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", ""),
        FIELD(":path", "/"),
        FIELD("host", "example.com"),
    };
    CHECK(validate(empty_authority_with_host, 5, TREV_HTTP3_HEADERS_REQUEST, &report) ==
          TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field empty_host[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":path", "/"),
        FIELD("host", ""),
    };
    CHECK(validate(empty_host, 4, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field non_http_without_authority[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "custom+rpc"),
        FIELD(":path", "relative-target"),
    };
    CHECK(validate(non_http_without_authority, 3, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);
    return 0;
}

static int test_authority_syntax_and_port_boundaries(void) {
    trevrpc_http3_headers_report report = {0};
    const char* const accepted[] = {
        "example.com",
        "sub_domain.example.",
        "exa!$&'()*+,;=mple.example",
        "percent-%41.example",
        "127.0.0.1",
        "127.0.0.1:0",
        "example.com:",
        "example.com:65535",
        "[::1]",
        "[::1]:",
        "[2001:db8::1]:443",
        "[::ffff:192.0.2.1]:80",
        "[v1.fe80::abcd]:8443",
        "[VF.a-._~!$&'()*+,;=:]",
    };
    for (size_t i = 0; i < sizeof(accepted) / sizeof(accepted[0]); i++) {
        CHECK(validate_authority(accepted[i], "https", &report) == TREV_HTTP3_HEADERS_OK);
    }

    const char* const rejected[] = {
        "",
        ":443",
        "user@example.com",
        "example.com/path",
        "example.com?query",
        "example.com#fragment",
        "example.com:http",
        "example.com:65536",
        "example.com:-1",
        "example.com:1:2",
        "2001:db8::1",
        "[127.0.0.1]",
        "[::1",
        "::1]",
        "[::1]extra",
        "[::1]:65536",
        "[2001:::1]",
        "[2001:db8::1::2]",
        "[192.0.2.1::]",
        "[v.fe80]",
        "[v1.]",
        "[v1.bad%20host]",
        "bad%host",
        "bad%2host",
        "bad[host]",
    };
    for (size_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); i++) {
        CHECK(validate_authority(rejected[i], "https", &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }

    const char* const custom_userinfo_accepted[] = {
        "user:pass@example.com:7443",
        "user%40name@example.com",
        "@example.com:",
        "user@[2001:db8::1]:",
    };
    for (size_t i = 0; i < sizeof(custom_userinfo_accepted) / sizeof(custom_userinfo_accepted[0]); i++) {
        CHECK(validate_authority(custom_userinfo_accepted[i], "custom+rpc", &report) == TREV_HTTP3_HEADERS_OK);
    }

    const char* const custom_userinfo_rejected[] = {
        "user@@example.com",
        "user%2@example.com",
        "user@example.com:invalid",
        "user@",
    };
    for (size_t i = 0; i < sizeof(custom_userinfo_rejected) / sizeof(custom_userinfo_rejected[0]); i++) {
        CHECK(
            validate_authority(custom_userinfo_rejected[i], "custom+rpc", &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }
    return 0;
}

static int test_path_validation(void) {
    trevrpc_http3_headers_report report = {0};
    const char* const accepted_get[] = {
        "/",
        "//",
        "/resource",
        "/a/b;c?x=y",
        "/?",
        "/?x=/a?b",
        "/percent%20encoded",
        "/:@!$&'()*+,;=-._~",
    };
    for (size_t i = 0; i < sizeof(accepted_get) / sizeof(accepted_get[0]); i++) {
        CHECK(validate_path("GET", accepted_get[i], &report) == TREV_HTTP3_HEADERS_OK);
    }
    CHECK(validate_path("OPTIONS", "*", &report) == TREV_HTTP3_HEADERS_OK);

    const char* const rejected_get[] = {
        "",
        "relative",
        "?query",
        "#fragment",
        "/path#fragment",
        "/path?query#fragment",
        "*",
        "*?query",
        "/bad percent",
        "/bad%escape",
        "/bad%2",
        "/tab\there",
    };
    for (size_t i = 0; i < sizeof(rejected_get) / sizeof(rejected_get[0]); i++) {
        CHECK(validate_path("GET", rejected_get[i], &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }
    CHECK(validate_path("POST", "*", &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field extended_connect[] = {
        FIELD(":method", "CONNECT"),
        FIELD(":protocol", "arbitrary-v2"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/session?version=2"),
    };
    CHECK(validate(extended_connect, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    trevrpc_qpack_field bad_extended_path[5];
    memcpy(bad_extended_path, extended_connect, sizeof(extended_connect));
    bad_extended_path[4] = FIELD(":path", "relative");
    CHECK(validate(bad_extended_path, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    bad_extended_path[4] = FIELD(":path", "*");
    CHECK(validate(bad_extended_path, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    return 0;
}

static int test_te_list_policy(void) {
    const char* const accepted[] = {
        "",
        "trailers",
        "TRAILERS",
        "TrAiLeRs",
        "trailers,trailers",
        "trailers, trailers",
        "TRAILERS\t,\tTrAiLeRs, trailers",
        "trailers,",
        ",trailers",
        "trailers,,trailers",
        "trailers, ,trailers",
    };
    trevrpc_http3_headers_report report = {0};
    for (size_t i = 0; i < sizeof(accepted) / sizeof(accepted[0]); i++) {
        trevrpc_qpack_field request[] = {
            FIELD(":method", "GET"),
            FIELD(":scheme", "https"),
            FIELD(":authority", "example.com"),
            FIELD(":path", "/"),
            byte_field((const uint8_t*)"te", 2, (const uint8_t*)accepted[i], strlen(accepted[i])),
        };
        CHECK(validate(request, sizeof(request) / sizeof(request[0]), TREV_HTTP3_HEADERS_REQUEST, &report) ==
              TREV_HTTP3_HEADERS_OK);
    }

    char reasonable_empty_members[16];
    memset(reasonable_empty_members, ',', sizeof(reasonable_empty_members) - 1);
    reasonable_empty_members[sizeof(reasonable_empty_members) - 1] = '\0';
    trevrpc_qpack_field reasonable_empty_request[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
        byte_field((const uint8_t*)"te", 2, (const uint8_t*)reasonable_empty_members, strlen(reasonable_empty_members)),
    };
    CHECK(validate(reasonable_empty_request,
              sizeof(reasonable_empty_request) / sizeof(reasonable_empty_request[0]),
              TREV_HTTP3_HEADERS_REQUEST,
              &report) == TREV_HTTP3_HEADERS_OK);

    char excessive_empty_members[17];
    memset(excessive_empty_members, ',', sizeof(excessive_empty_members) - 1);
    excessive_empty_members[sizeof(excessive_empty_members) - 1] = '\0';
    trevrpc_qpack_field excessive_empty_request[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
        byte_field((const uint8_t*)"te", 2, (const uint8_t*)excessive_empty_members, strlen(excessive_empty_members)),
    };
    CHECK(validate(excessive_empty_request,
              sizeof(excessive_empty_request) / sizeof(excessive_empty_request[0]),
              TREV_HTTP3_HEADERS_REQUEST,
              &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const char* const rejected[] = {
        " trailers",
        "trailers ",
        "gzip",
        "trailers,gzip",
        "gzip,trailers",
        "trail ers",
        "trailers gzip",
        "trailers;q=1",
        "trailers; q=1",
    };
    for (size_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); i++) {
        trevrpc_qpack_field request[] = {
            FIELD(":method", "GET"),
            FIELD(":scheme", "https"),
            FIELD(":authority", "example.com"),
            FIELD(":path", "/"),
            byte_field((const uint8_t*)"te", 2, (const uint8_t*)rejected[i], strlen(rejected[i])),
        };
        CHECK(validate(request, sizeof(request) / sizeof(request[0]), TREV_HTTP3_HEADERS_REQUEST, &report) ==
              TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }

    const trevrpc_qpack_field response[] = {FIELD(":status", "200"), FIELD("te", "trailers")};
    CHECK(validate(response, 2, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    const trevrpc_qpack_field trailers[] = {FIELD("te", "trailers")};
    CHECK(validate(trailers, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    return 0;
}

static int test_generic_protocol_and_classic_connect_rules(void) {
    trevrpc_http3_headers_report report = {0};
    const trevrpc_qpack_field extended_connect[] = {
        FIELD(":method", "CONNECT"),
        FIELD(":protocol", "arbitrary-v2"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/session"),
    };
    CHECK(validate(extended_connect, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    trevrpc_qpack_field other_protocol[5];
    memcpy(other_protocol, extended_connect, sizeof(extended_connect));
    other_protocol[1] = FIELD(":protocol", "WebTransport+EXPERIMENT");
    CHECK(validate(other_protocol, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const trevrpc_qpack_field custom_extended_connect[] = {
        FIELD(":method", "CONNECT"),
        FIELD(":protocol", "arbitrary-v2"),
        FIELD(":scheme", "custom+rpc"),
        FIELD(":authority", "user@example.com:7443"),
        FIELD(":path", "/session"),
    };
    CHECK(validate(custom_extended_connect, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);

    const char* const valid_authorities[] = {
        "example.com:443",
        "example.com:0",
        "example.com:65535",
        "[::1]:443",
        "[v1.connect]:7",
    };
    for (size_t i = 0; i < sizeof(valid_authorities) / sizeof(valid_authorities[0]); i++) {
        trevrpc_qpack_field request[] = {
            FIELD(":method", "CONNECT"),
            byte_field(
                (const uint8_t*)":authority", 10, (const uint8_t*)valid_authorities[i], strlen(valid_authorities[i])),
        };
        CHECK(validate(request, 2, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);
    }

    const char* const invalid_authorities[] = {
        "example.com",
        "example.com:",
        "example.com:https",
        "example.com:65536",
        ":443",
        "user@example.com:443",
        "2001:db8::1:443",
        "[::1]",
        "[::1]:",
    };
    for (size_t i = 0; i < sizeof(invalid_authorities) / sizeof(invalid_authorities[0]); i++) {
        trevrpc_qpack_field request[] = {
            FIELD(":method", "CONNECT"),
            byte_field((const uint8_t*)":authority",
                10,
                (const uint8_t*)invalid_authorities[i],
                strlen(invalid_authorities[i])),
        };
        CHECK(validate(request, 2, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }

    const trevrpc_qpack_field connect_with_matching_host[] = {
        FIELD(":method", "CONNECT"),
        FIELD(":authority", "example.com:443"),
        FIELD("host", "EXAMPLE.COM:0443"),
    };
    CHECK(validate(connect_with_matching_host, 3, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_OK);
    const trevrpc_qpack_field connect_with_mismatched_host[] = {
        FIELD(":method", "CONNECT"),
        FIELD(":authority", "example.com:443"),
        FIELD("host", "example.com:80"),
    };
    CHECK(validate(connect_with_mismatched_host, 3, TREV_HTTP3_HEADERS_REQUEST, &report) ==
          TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field protocol_on_get[] = {
        FIELD(":method", "GET"),
        FIELD(":protocol", "anything"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
    };
    CHECK(validate(protocol_on_get, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field connect_with_scheme[] = {
        FIELD(":method", "CONNECT"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com:443"),
    };
    CHECK(validate(connect_with_scheme, 3, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field invalid_scheme[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "1https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
    };
    CHECK(validate(invalid_scheme, 4, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    return 0;
}

static int test_pseudo_order_duplicates_unknown_and_mixing(void) {
    trevrpc_http3_headers_report report = {0};
    const trevrpc_qpack_field late_pseudo[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":path", "/"),
        FIELD("host", "example.com"),
        FIELD(":authority", "example.com"),
    };
    CHECK(validate(late_pseudo, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field duplicate[] = {
        FIELD(":method", "GET"),
        FIELD(":method", "POST"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
    };
    CHECK(validate(duplicate, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field unknown[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
        FIELD(":future", "x"),
    };
    CHECK(validate(unknown, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field request_status[] = {
        FIELD(":method", "GET"),
        FIELD(":scheme", "https"),
        FIELD(":authority", "example.com"),
        FIELD(":path", "/"),
        FIELD(":status", "200"),
    };
    CHECK(validate(request_status, 5, TREV_HTTP3_HEADERS_REQUEST, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field response_method[] = {FIELD(":status", "200"), FIELD(":method", "GET")};
    CHECK(validate(response_method, 2, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    return 0;
}

static int test_trailer_and_response_content_rules(void) {
    trevrpc_http3_headers_report report = {0};
    const trevrpc_qpack_field pseudo[] = {FIELD(":status", "200")};
    CHECK(validate(pseudo, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    const trevrpc_qpack_field content_length[] = {FIELD("content-length", "0")};
    CHECK(validate(content_length, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    const trevrpc_qpack_field host[] = {FIELD("host", "example.com")};
    CHECK(validate(host, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    const trevrpc_qpack_field trailer[] = {FIELD("trailer", "digest")};
    CHECK(validate(trailer, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const trevrpc_qpack_field eligible[] = {
        FIELD("digest", "sha-256=:YWJjZA==:"),
        FIELD("etag", "\"final\""),
        FIELD("content-type", "application/json"),
        FIELD("content-encoding", "gzip"),
        FIELD("content-range", "bytes 0-9/10"),
        FIELD("authorization", "Bearer sender-policy"),
        FIELD("vary", "accept-encoding"),
        FIELD("x-final-metadata", "available after payload"),
    };
    CHECK(validate(eligible, sizeof(eligible) / sizeof(eligible[0]), TREV_HTTP3_HEADERS_TRAILERS, &report) ==
          TREV_HTTP3_HEADERS_OK);

    const char* const connection_specific[] = {
        "connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade"};
    for (size_t i = 0; i < sizeof(connection_specific) / sizeof(connection_specific[0]); i++) {
        trevrpc_qpack_field field =
            byte_field((const uint8_t*)connection_specific[i], strlen(connection_specific[i]), (const uint8_t*)"x", 1);
        CHECK(validate(&field, 1, TREV_HTTP3_HEADERS_TRAILERS, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }

    const char* const no_content_length_statuses[] = {"100", "102", "199", "204"};
    for (size_t i = 0; i < sizeof(no_content_length_statuses) / sizeof(no_content_length_statuses[0]); i++) {
        trevrpc_qpack_field response[] = {
            byte_field((const uint8_t*)":status",
                7,
                (const uint8_t*)no_content_length_statuses[i],
                strlen(no_content_length_statuses[i])),
            FIELD("content-length", "0"),
        };
        CHECK(validate(response, 2, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }

    const char* const allowed_content_length_statuses[] = {"200", "201", "205", "304"};
    for (size_t i = 0; i < sizeof(allowed_content_length_statuses) / sizeof(allowed_content_length_statuses[0]); i++) {
        trevrpc_qpack_field response[] = {
            byte_field((const uint8_t*)":status",
                7,
                (const uint8_t*)allowed_content_length_statuses[i],
                strlen(allowed_content_length_statuses[i])),
            FIELD("content-length", "0"),
        };
        CHECK(validate(response, 2, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_OK);
        CHECK(report.has_content_length);
    }
    return 0;
}

static int test_status_validation(void) {
    const char* const accepted[] = {"100", "200", "599"};
    const char* const rejected[] = {"099", "101", "600", "20", "2000", "2a0", ""};
    trevrpc_http3_headers_report report = {0};
    for (size_t i = 0; i < sizeof(accepted) / sizeof(accepted[0]); i++) {
        trevrpc_qpack_field field =
            byte_field((const uint8_t*)":status", 7, (const uint8_t*)accepted[i], strlen(accepted[i]));
        CHECK(validate(&field, 1, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_OK);
    }
    for (size_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); i++) {
        trevrpc_qpack_field field =
            byte_field((const uint8_t*)":status", 7, (const uint8_t*)rejected[i], strlen(rejected[i]));
        CHECK(validate(&field, 1, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }
    CHECK(validate(NULL, 0, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    return 0;
}

static int test_content_length_consistency(void) {
    trevrpc_http3_headers_report report = {0};
    const trevrpc_qpack_field consistent[] = {
        FIELD(":status", "200"),
        FIELD("content-length", "42"),
        FIELD("content-length", "0042"),
    };
    CHECK(validate(consistent, 3, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_OK);
    CHECK(report.has_content_length);

    const trevrpc_qpack_field conflicting[] = {
        FIELD(":status", "200"),
        FIELD("content-length", "42"),
        FIELD("content-length", "43"),
    };
    CHECK(validate(conflicting, 3, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);

    const char* const invalid[] = {"", "-1", "+1", "1,1", "1 2", " 1", "1\t"};
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        trevrpc_qpack_field fields[] = {
            FIELD(":status", "200"),
            byte_field((const uint8_t*)"content-length", 14, (const uint8_t*)invalid[i], strlen(invalid[i])),
        };
        CHECK(validate(fields, 2, TREV_HTTP3_HEADERS_RESPONSE, &report) == TREV_HTTP3_HEADERS_MESSAGE_ERROR);
    }
    return 0;
}

static int test_status_error_mapping(void) {
    static const struct {
        trevrpc_http3_headers_status status;
        uint64_t error_code;
    } cases[] = {
        {TREV_HTTP3_HEADERS_OK, 0},
        {TREV_HTTP3_HEADERS_INVALID_ARGUMENT, 0},
        {TREV_HTTP3_HEADERS_MESSAGE_ERROR, UINT64_C(0x10e)},
        {(trevrpc_http3_headers_status)-1, 0},
        {(trevrpc_http3_headers_status)99, 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CHECK(trevrpc_http3_headers_status_error_code(cases[i].status) == cases[i].error_code);
    }
    return 0;
}

static int test_invalid_arguments_and_atomic_report(void) {
    trevrpc_http3_headers_report report = {
        .pseudo_field_count = 11,
        .regular_field_count = 22,
        .response_status = 333,
        .has_content_length = 1,
    };
    CHECK(trevrpc_http3_headers_validate(NULL, TREV_HTTP3_HEADERS_REQUEST, &report) ==
          TREV_HTTP3_HEADERS_INVALID_ARGUMENT);
    CHECK(report.pseudo_field_count == 0 && report.regular_field_count == 0 && report.response_status == 0 &&
          !report.has_content_length);
    trevrpc_qpack_field_section section = {0};
    CHECK(trevrpc_http3_headers_validate(&section, (trevrpc_http3_header_block_kind)99, &report) ==
          TREV_HTTP3_HEADERS_INVALID_ARGUMENT);
    CHECK(trevrpc_http3_headers_validate(&section, TREV_HTTP3_HEADERS_TRAILERS, NULL) ==
          TREV_HTTP3_HEADERS_INVALID_ARGUMENT);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_valid_request_response_and_trailers,
        test_name_validation,
        test_value_validation_and_whitespace_boundaries,
        test_http_authority_or_host_requirements,
        test_authority_syntax_and_port_boundaries,
        test_path_validation,
        test_te_list_policy,
        test_generic_protocol_and_classic_connect_rules,
        test_pseudo_order_duplicates_unknown_and_mixing,
        test_trailer_and_response_content_rules,
        test_status_validation,
        test_content_length_consistency,
        test_status_error_mapping,
        test_invalid_arguments_and_atomic_report,
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        int result = tests[i]();
        if (result != 0) {
            return result;
        }
    }
    return 0;
}
