#include "trevrpc_qpack_static_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define TREV_QPACK_STATIC_TABLE_LEN 99u
#define TREV_QPACK_MAX_INTEGER UINT64_C(0x3fffffffffffffff)
#define TREV_HPACK_EOS 256u

typedef struct trevrpc_qpack_static_entry {
    const char* name;
    size_t name_len;
    const char* value;
    size_t value_len;
} trevrpc_qpack_static_entry;

typedef struct trevrpc_huffman_entry {
    uint32_t code;
    uint16_t symbol;
    uint8_t bit_len;
} trevrpc_huffman_entry;

typedef struct trevrpc_qpack_string {
    const uint8_t* encoded;
    size_t encoded_len;
    size_t decoded_len;
    int huffman;
} trevrpc_qpack_string;

typedef struct trevrpc_qpack_decoded_field {
    trevrpc_qpack_string name;
    trevrpc_qpack_string value;
} trevrpc_qpack_decoded_field;

typedef struct trevrpc_qpack_parse_state {
    const uint8_t* encoded;
    size_t encoded_len;
    size_t offset;
    size_t field_count;
    size_t decoded_size;
    const trevrpc_qpack_limits* limits;
    trevrpc_qpack_field* fields;
    uint8_t* bytes;
    size_t field_index;
} trevrpc_qpack_parse_state;

static const trevrpc_qpack_static_entry trevrpc_qpack_static_table[TREV_QPACK_STATIC_TABLE_LEN] = {
    {":authority", sizeof(":authority") - 1, "", sizeof("") - 1},
    {":path", sizeof(":path") - 1, "/", sizeof("/") - 1},
    {"age", sizeof("age") - 1, "0", sizeof("0") - 1},
    {"content-disposition", sizeof("content-disposition") - 1, "", sizeof("") - 1},
    {"content-length", sizeof("content-length") - 1, "0", sizeof("0") - 1},
    {"cookie", sizeof("cookie") - 1, "", sizeof("") - 1},
    {"date", sizeof("date") - 1, "", sizeof("") - 1},
    {"etag", sizeof("etag") - 1, "", sizeof("") - 1},
    {"if-modified-since", sizeof("if-modified-since") - 1, "", sizeof("") - 1},
    {"if-none-match", sizeof("if-none-match") - 1, "", sizeof("") - 1},
    {"last-modified", sizeof("last-modified") - 1, "", sizeof("") - 1},
    {"link", sizeof("link") - 1, "", sizeof("") - 1},
    {"location", sizeof("location") - 1, "", sizeof("") - 1},
    {"referer", sizeof("referer") - 1, "", sizeof("") - 1},
    {"set-cookie", sizeof("set-cookie") - 1, "", sizeof("") - 1},
    {":method", sizeof(":method") - 1, "CONNECT", sizeof("CONNECT") - 1},
    {":method", sizeof(":method") - 1, "DELETE", sizeof("DELETE") - 1},
    {":method", sizeof(":method") - 1, "GET", sizeof("GET") - 1},
    {":method", sizeof(":method") - 1, "HEAD", sizeof("HEAD") - 1},
    {":method", sizeof(":method") - 1, "OPTIONS", sizeof("OPTIONS") - 1},
    {":method", sizeof(":method") - 1, "POST", sizeof("POST") - 1},
    {":method", sizeof(":method") - 1, "PUT", sizeof("PUT") - 1},
    {":scheme", sizeof(":scheme") - 1, "http", sizeof("http") - 1},
    {":scheme", sizeof(":scheme") - 1, "https", sizeof("https") - 1},
    {":status", sizeof(":status") - 1, "103", sizeof("103") - 1},
    {":status", sizeof(":status") - 1, "200", sizeof("200") - 1},
    {":status", sizeof(":status") - 1, "304", sizeof("304") - 1},
    {":status", sizeof(":status") - 1, "404", sizeof("404") - 1},
    {":status", sizeof(":status") - 1, "503", sizeof("503") - 1},
    {"accept", sizeof("accept") - 1, "*/*", sizeof("*/*") - 1},
    {"accept", sizeof("accept") - 1, "application/dns-message", sizeof("application/dns-message") - 1},
    {"accept-encoding", sizeof("accept-encoding") - 1, "gzip, deflate, br", sizeof("gzip, deflate, br") - 1},
    {"accept-ranges", sizeof("accept-ranges") - 1, "bytes", sizeof("bytes") - 1},
    {"access-control-allow-headers",
        sizeof("access-control-allow-headers") - 1,
        "cache-control",
        sizeof("cache-control") - 1},
    {"access-control-allow-headers",
        sizeof("access-control-allow-headers") - 1,
        "content-type",
        sizeof("content-type") - 1},
    {"access-control-allow-origin", sizeof("access-control-allow-origin") - 1, "*", sizeof("*") - 1},
    {"cache-control", sizeof("cache-control") - 1, "max-age=0", sizeof("max-age=0") - 1},
    {"cache-control", sizeof("cache-control") - 1, "max-age=2592000", sizeof("max-age=2592000") - 1},
    {"cache-control", sizeof("cache-control") - 1, "max-age=604800", sizeof("max-age=604800") - 1},
    {"cache-control", sizeof("cache-control") - 1, "no-cache", sizeof("no-cache") - 1},
    {"cache-control", sizeof("cache-control") - 1, "no-store", sizeof("no-store") - 1},
    {"cache-control", sizeof("cache-control") - 1, "public, max-age=31536000", sizeof("public, max-age=31536000") - 1},
    {"content-encoding", sizeof("content-encoding") - 1, "br", sizeof("br") - 1},
    {"content-encoding", sizeof("content-encoding") - 1, "gzip", sizeof("gzip") - 1},
    {"content-type", sizeof("content-type") - 1, "application/dns-message", sizeof("application/dns-message") - 1},
    {"content-type", sizeof("content-type") - 1, "application/javascript", sizeof("application/javascript") - 1},
    {"content-type", sizeof("content-type") - 1, "application/json", sizeof("application/json") - 1},
    {"content-type",
        sizeof("content-type") - 1,
        "application/x-www-form-urlencoded",
        sizeof("application/x-www-form-urlencoded") - 1},
    {"content-type", sizeof("content-type") - 1, "image/gif", sizeof("image/gif") - 1},
    {"content-type", sizeof("content-type") - 1, "image/jpeg", sizeof("image/jpeg") - 1},
    {"content-type", sizeof("content-type") - 1, "image/png", sizeof("image/png") - 1},
    {"content-type", sizeof("content-type") - 1, "text/css", sizeof("text/css") - 1},
    {"content-type", sizeof("content-type") - 1, "text/html; charset=utf-8", sizeof("text/html; charset=utf-8") - 1},
    {"content-type", sizeof("content-type") - 1, "text/plain", sizeof("text/plain") - 1},
    {"content-type", sizeof("content-type") - 1, "text/plain;charset=utf-8", sizeof("text/plain;charset=utf-8") - 1},
    {"range", sizeof("range") - 1, "bytes=0-", sizeof("bytes=0-") - 1},
    {"strict-transport-security",
        sizeof("strict-transport-security") - 1,
        "max-age=31536000",
        sizeof("max-age=31536000") - 1},
    {"strict-transport-security",
        sizeof("strict-transport-security") - 1,
        "max-age=31536000; includesubdomains",
        sizeof("max-age=31536000; includesubdomains") - 1},
    {"strict-transport-security",
        sizeof("strict-transport-security") - 1,
        "max-age=31536000; includesubdomains; preload",
        sizeof("max-age=31536000; includesubdomains; preload") - 1},
    {"vary", sizeof("vary") - 1, "accept-encoding", sizeof("accept-encoding") - 1},
    {"vary", sizeof("vary") - 1, "origin", sizeof("origin") - 1},
    {"x-content-type-options", sizeof("x-content-type-options") - 1, "nosniff", sizeof("nosniff") - 1},
    {"x-xss-protection", sizeof("x-xss-protection") - 1, "1; mode=block", sizeof("1; mode=block") - 1},
    {":status", sizeof(":status") - 1, "100", sizeof("100") - 1},
    {":status", sizeof(":status") - 1, "204", sizeof("204") - 1},
    {":status", sizeof(":status") - 1, "206", sizeof("206") - 1},
    {":status", sizeof(":status") - 1, "302", sizeof("302") - 1},
    {":status", sizeof(":status") - 1, "400", sizeof("400") - 1},
    {":status", sizeof(":status") - 1, "403", sizeof("403") - 1},
    {":status", sizeof(":status") - 1, "421", sizeof("421") - 1},
    {":status", sizeof(":status") - 1, "425", sizeof("425") - 1},
    {":status", sizeof(":status") - 1, "500", sizeof("500") - 1},
    {"accept-language", sizeof("accept-language") - 1, "", sizeof("") - 1},
    {"access-control-allow-credentials", sizeof("access-control-allow-credentials") - 1, "FALSE", sizeof("FALSE") - 1},
    {"access-control-allow-credentials", sizeof("access-control-allow-credentials") - 1, "TRUE", sizeof("TRUE") - 1},
    {"access-control-allow-headers", sizeof("access-control-allow-headers") - 1, "*", sizeof("*") - 1},
    {"access-control-allow-methods", sizeof("access-control-allow-methods") - 1, "get", sizeof("get") - 1},
    {"access-control-allow-methods",
        sizeof("access-control-allow-methods") - 1,
        "get, post, options",
        sizeof("get, post, options") - 1},
    {"access-control-allow-methods", sizeof("access-control-allow-methods") - 1, "options", sizeof("options") - 1},
    {"access-control-expose-headers",
        sizeof("access-control-expose-headers") - 1,
        "content-length",
        sizeof("content-length") - 1},
    {"access-control-request-headers",
        sizeof("access-control-request-headers") - 1,
        "content-type",
        sizeof("content-type") - 1},
    {"access-control-request-method", sizeof("access-control-request-method") - 1, "get", sizeof("get") - 1},
    {"access-control-request-method", sizeof("access-control-request-method") - 1, "post", sizeof("post") - 1},
    {"alt-svc", sizeof("alt-svc") - 1, "clear", sizeof("clear") - 1},
    {"authorization", sizeof("authorization") - 1, "", sizeof("") - 1},
    {"content-security-policy",
        sizeof("content-security-policy") - 1,
        "script-src 'none'; object-src 'none'; base-uri 'none'",
        sizeof("script-src 'none'; object-src 'none'; base-uri 'none'") - 1},
    {"early-data", sizeof("early-data") - 1, "1", sizeof("1") - 1},
    {"expect-ct", sizeof("expect-ct") - 1, "", sizeof("") - 1},
    {"forwarded", sizeof("forwarded") - 1, "", sizeof("") - 1},
    {"if-range", sizeof("if-range") - 1, "", sizeof("") - 1},
    {"origin", sizeof("origin") - 1, "", sizeof("") - 1},
    {"purpose", sizeof("purpose") - 1, "prefetch", sizeof("prefetch") - 1},
    {"server", sizeof("server") - 1, "", sizeof("") - 1},
    {"timing-allow-origin", sizeof("timing-allow-origin") - 1, "*", sizeof("*") - 1},
    {"upgrade-insecure-requests", sizeof("upgrade-insecure-requests") - 1, "1", sizeof("1") - 1},
    {"user-agent", sizeof("user-agent") - 1, "", sizeof("") - 1},
    {"x-forwarded-for", sizeof("x-forwarded-for") - 1, "", sizeof("") - 1},
    {"x-frame-options", sizeof("x-frame-options") - 1, "deny", sizeof("deny") - 1},
    {"x-frame-options", sizeof("x-frame-options") - 1, "sameorigin", sizeof("sameorigin") - 1},
};

/* Sorted by (bit length, code), enabling a bounded binary search per bit. */
static const trevrpc_huffman_entry trevrpc_huffman_table[257] = {
    {0x0u, 48u, 5u},
    {0x1u, 49u, 5u},
    {0x2u, 50u, 5u},
    {0x3u, 97u, 5u},
    {0x4u, 99u, 5u},
    {0x5u, 101u, 5u},
    {0x6u, 105u, 5u},
    {0x7u, 111u, 5u},
    {0x8u, 115u, 5u},
    {0x9u, 116u, 5u},
    {0x14u, 32u, 6u},
    {0x15u, 37u, 6u},
    {0x16u, 45u, 6u},
    {0x17u, 46u, 6u},
    {0x18u, 47u, 6u},
    {0x19u, 51u, 6u},
    {0x1au, 52u, 6u},
    {0x1bu, 53u, 6u},
    {0x1cu, 54u, 6u},
    {0x1du, 55u, 6u},
    {0x1eu, 56u, 6u},
    {0x1fu, 57u, 6u},
    {0x20u, 61u, 6u},
    {0x21u, 65u, 6u},
    {0x22u, 95u, 6u},
    {0x23u, 98u, 6u},
    {0x24u, 100u, 6u},
    {0x25u, 102u, 6u},
    {0x26u, 103u, 6u},
    {0x27u, 104u, 6u},
    {0x28u, 108u, 6u},
    {0x29u, 109u, 6u},
    {0x2au, 110u, 6u},
    {0x2bu, 112u, 6u},
    {0x2cu, 114u, 6u},
    {0x2du, 117u, 6u},
    {0x5cu, 58u, 7u},
    {0x5du, 66u, 7u},
    {0x5eu, 67u, 7u},
    {0x5fu, 68u, 7u},
    {0x60u, 69u, 7u},
    {0x61u, 70u, 7u},
    {0x62u, 71u, 7u},
    {0x63u, 72u, 7u},
    {0x64u, 73u, 7u},
    {0x65u, 74u, 7u},
    {0x66u, 75u, 7u},
    {0x67u, 76u, 7u},
    {0x68u, 77u, 7u},
    {0x69u, 78u, 7u},
    {0x6au, 79u, 7u},
    {0x6bu, 80u, 7u},
    {0x6cu, 81u, 7u},
    {0x6du, 82u, 7u},
    {0x6eu, 83u, 7u},
    {0x6fu, 84u, 7u},
    {0x70u, 85u, 7u},
    {0x71u, 86u, 7u},
    {0x72u, 87u, 7u},
    {0x73u, 89u, 7u},
    {0x74u, 106u, 7u},
    {0x75u, 107u, 7u},
    {0x76u, 113u, 7u},
    {0x77u, 118u, 7u},
    {0x78u, 119u, 7u},
    {0x79u, 120u, 7u},
    {0x7au, 121u, 7u},
    {0x7bu, 122u, 7u},
    {0xf8u, 38u, 8u},
    {0xf9u, 42u, 8u},
    {0xfau, 44u, 8u},
    {0xfbu, 59u, 8u},
    {0xfcu, 88u, 8u},
    {0xfdu, 90u, 8u},
    {0x3f8u, 33u, 10u},
    {0x3f9u, 34u, 10u},
    {0x3fau, 40u, 10u},
    {0x3fbu, 41u, 10u},
    {0x3fcu, 63u, 10u},
    {0x7fau, 39u, 11u},
    {0x7fbu, 43u, 11u},
    {0x7fcu, 124u, 11u},
    {0xffau, 35u, 12u},
    {0xffbu, 62u, 12u},
    {0x1ff8u, 0u, 13u},
    {0x1ff9u, 36u, 13u},
    {0x1ffau, 64u, 13u},
    {0x1ffbu, 91u, 13u},
    {0x1ffcu, 93u, 13u},
    {0x1ffdu, 126u, 13u},
    {0x3ffcu, 94u, 14u},
    {0x3ffdu, 125u, 14u},
    {0x7ffcu, 60u, 15u},
    {0x7ffdu, 96u, 15u},
    {0x7ffeu, 123u, 15u},
    {0x7fff0u, 92u, 19u},
    {0x7fff1u, 195u, 19u},
    {0x7fff2u, 208u, 19u},
    {0xfffe6u, 128u, 20u},
    {0xfffe7u, 130u, 20u},
    {0xfffe8u, 131u, 20u},
    {0xfffe9u, 162u, 20u},
    {0xfffeau, 184u, 20u},
    {0xfffebu, 194u, 20u},
    {0xfffecu, 224u, 20u},
    {0xfffedu, 226u, 20u},
    {0x1fffdcu, 153u, 21u},
    {0x1fffddu, 161u, 21u},
    {0x1fffdeu, 167u, 21u},
    {0x1fffdfu, 172u, 21u},
    {0x1fffe0u, 176u, 21u},
    {0x1fffe1u, 177u, 21u},
    {0x1fffe2u, 179u, 21u},
    {0x1fffe3u, 209u, 21u},
    {0x1fffe4u, 216u, 21u},
    {0x1fffe5u, 217u, 21u},
    {0x1fffe6u, 227u, 21u},
    {0x1fffe7u, 229u, 21u},
    {0x1fffe8u, 230u, 21u},
    {0x3fffd2u, 129u, 22u},
    {0x3fffd3u, 132u, 22u},
    {0x3fffd4u, 133u, 22u},
    {0x3fffd5u, 134u, 22u},
    {0x3fffd6u, 136u, 22u},
    {0x3fffd7u, 146u, 22u},
    {0x3fffd8u, 154u, 22u},
    {0x3fffd9u, 156u, 22u},
    {0x3fffdau, 160u, 22u},
    {0x3fffdbu, 163u, 22u},
    {0x3fffdcu, 164u, 22u},
    {0x3fffddu, 169u, 22u},
    {0x3fffdeu, 170u, 22u},
    {0x3fffdfu, 173u, 22u},
    {0x3fffe0u, 178u, 22u},
    {0x3fffe1u, 181u, 22u},
    {0x3fffe2u, 185u, 22u},
    {0x3fffe3u, 186u, 22u},
    {0x3fffe4u, 187u, 22u},
    {0x3fffe5u, 189u, 22u},
    {0x3fffe6u, 190u, 22u},
    {0x3fffe7u, 196u, 22u},
    {0x3fffe8u, 198u, 22u},
    {0x3fffe9u, 228u, 22u},
    {0x3fffeau, 232u, 22u},
    {0x3fffebu, 233u, 22u},
    {0x7fffd8u, 1u, 23u},
    {0x7fffd9u, 135u, 23u},
    {0x7fffdau, 137u, 23u},
    {0x7fffdbu, 138u, 23u},
    {0x7fffdcu, 139u, 23u},
    {0x7fffddu, 140u, 23u},
    {0x7fffdeu, 141u, 23u},
    {0x7fffdfu, 143u, 23u},
    {0x7fffe0u, 147u, 23u},
    {0x7fffe1u, 149u, 23u},
    {0x7fffe2u, 150u, 23u},
    {0x7fffe3u, 151u, 23u},
    {0x7fffe4u, 152u, 23u},
    {0x7fffe5u, 155u, 23u},
    {0x7fffe6u, 157u, 23u},
    {0x7fffe7u, 158u, 23u},
    {0x7fffe8u, 165u, 23u},
    {0x7fffe9u, 166u, 23u},
    {0x7fffeau, 168u, 23u},
    {0x7fffebu, 174u, 23u},
    {0x7fffecu, 175u, 23u},
    {0x7fffedu, 180u, 23u},
    {0x7fffeeu, 182u, 23u},
    {0x7fffefu, 183u, 23u},
    {0x7ffff0u, 188u, 23u},
    {0x7ffff1u, 191u, 23u},
    {0x7ffff2u, 197u, 23u},
    {0x7ffff3u, 231u, 23u},
    {0x7ffff4u, 239u, 23u},
    {0xffffeau, 9u, 24u},
    {0xffffebu, 142u, 24u},
    {0xffffecu, 144u, 24u},
    {0xffffedu, 145u, 24u},
    {0xffffeeu, 148u, 24u},
    {0xffffefu, 159u, 24u},
    {0xfffff0u, 171u, 24u},
    {0xfffff1u, 206u, 24u},
    {0xfffff2u, 215u, 24u},
    {0xfffff3u, 225u, 24u},
    {0xfffff4u, 236u, 24u},
    {0xfffff5u, 237u, 24u},
    {0x1ffffecu, 199u, 25u},
    {0x1ffffedu, 207u, 25u},
    {0x1ffffeeu, 234u, 25u},
    {0x1ffffefu, 235u, 25u},
    {0x3ffffe0u, 192u, 26u},
    {0x3ffffe1u, 193u, 26u},
    {0x3ffffe2u, 200u, 26u},
    {0x3ffffe3u, 201u, 26u},
    {0x3ffffe4u, 202u, 26u},
    {0x3ffffe5u, 205u, 26u},
    {0x3ffffe6u, 210u, 26u},
    {0x3ffffe7u, 213u, 26u},
    {0x3ffffe8u, 218u, 26u},
    {0x3ffffe9u, 219u, 26u},
    {0x3ffffeau, 238u, 26u},
    {0x3ffffebu, 240u, 26u},
    {0x3ffffecu, 242u, 26u},
    {0x3ffffedu, 243u, 26u},
    {0x3ffffeeu, 255u, 26u},
    {0x7ffffdeu, 203u, 27u},
    {0x7ffffdfu, 204u, 27u},
    {0x7ffffe0u, 211u, 27u},
    {0x7ffffe1u, 212u, 27u},
    {0x7ffffe2u, 214u, 27u},
    {0x7ffffe3u, 221u, 27u},
    {0x7ffffe4u, 222u, 27u},
    {0x7ffffe5u, 223u, 27u},
    {0x7ffffe6u, 241u, 27u},
    {0x7ffffe7u, 244u, 27u},
    {0x7ffffe8u, 245u, 27u},
    {0x7ffffe9u, 246u, 27u},
    {0x7ffffeau, 247u, 27u},
    {0x7ffffebu, 248u, 27u},
    {0x7ffffecu, 250u, 27u},
    {0x7ffffedu, 251u, 27u},
    {0x7ffffeeu, 252u, 27u},
    {0x7ffffefu, 253u, 27u},
    {0x7fffff0u, 254u, 27u},
    {0xfffffe2u, 2u, 28u},
    {0xfffffe3u, 3u, 28u},
    {0xfffffe4u, 4u, 28u},
    {0xfffffe5u, 5u, 28u},
    {0xfffffe6u, 6u, 28u},
    {0xfffffe7u, 7u, 28u},
    {0xfffffe8u, 8u, 28u},
    {0xfffffe9u, 11u, 28u},
    {0xfffffeau, 12u, 28u},
    {0xfffffebu, 14u, 28u},
    {0xfffffecu, 15u, 28u},
    {0xfffffedu, 16u, 28u},
    {0xfffffeeu, 17u, 28u},
    {0xfffffefu, 18u, 28u},
    {0xffffff0u, 19u, 28u},
    {0xffffff1u, 20u, 28u},
    {0xffffff2u, 21u, 28u},
    {0xffffff3u, 23u, 28u},
    {0xffffff4u, 24u, 28u},
    {0xffffff5u, 25u, 28u},
    {0xffffff6u, 26u, 28u},
    {0xffffff7u, 27u, 28u},
    {0xffffff8u, 28u, 28u},
    {0xffffff9u, 29u, 28u},
    {0xffffffau, 30u, 28u},
    {0xffffffbu, 31u, 28u},
    {0xffffffcu, 127u, 28u},
    {0xffffffdu, 220u, 28u},
    {0xffffffeu, 249u, 28u},
    {0x3ffffffcu, 10u, 30u},
    {0x3ffffffdu, 13u, 30u},
    {0x3ffffffeu, 22u, 30u},
    {0x3fffffffu, 256u, 30u},
};

static const uint16_t trevrpc_huffman_length_start[31] = {0,
    0,
    0,
    0,
    0,
    0,
    10,
    36,
    68,
    74,
    74,
    79,
    82,
    84,
    90,
    92,
    95,
    95,
    95,
    95,
    98,
    106,
    119,
    145,
    174,
    186,
    190,
    205,
    224,
    253,
    253};
static const uint8_t trevrpc_huffman_length_count[31] = {
    0, 0, 0, 0, 0, 10, 26, 32, 6, 0, 5, 3, 2, 6, 2, 3, 0, 0, 0, 3, 8, 13, 26, 29, 12, 4, 15, 19, 29, 0, 4};

static int trevrpc_size_add(size_t left, size_t right, size_t* out) {
    if (right > SIZE_MAX - left) {
        return -1;
    }
    *out = left + right;
    return 0;
}

static int trevrpc_size_multiply(size_t left, size_t right, size_t* out) {
    if (left != 0 && right > SIZE_MAX / left) {
        return -1;
    }
    *out = left * right;
    return 0;
}

static int trevrpc_uint64_to_size(uint64_t value, size_t* out) {
#if SIZE_MAX < UINT64_MAX
    if (value > SIZE_MAX) {
        return -1;
    }
#endif
    *out = (size_t)value;
    return 0;
}

static int trevrpc_qpack_integer(
    const uint8_t* encoded, size_t encoded_len, size_t* offset, unsigned prefix_bits, uint64_t* out_value) {
    if (*offset >= encoded_len || prefix_bits == 0 || prefix_bits > 8) {
        return -1;
    }
    uint8_t first = encoded[(*offset)++];
    uint64_t prefix_max = UINT64_C(0xff) >> (8u - prefix_bits);
    uint64_t value = (uint64_t)first & prefix_max;
    if (value != prefix_max) {
        *out_value = value;
        return 0;
    }

    unsigned shift = 0;
    for (;;) {
        if (*offset >= encoded_len) {
            return -1;
        }
        uint8_t byte = encoded[(*offset)++];
        uint64_t payload = (uint64_t)(byte & 0x7fu);
        if (payload > (UINT64_MAX - value) >> shift) {
            return -1;
        }
        value += payload << shift;
        if ((byte & 0x80u) == 0) {
            *out_value = value;
            return 0;
        }
        if (shift >= 63) {
            return -1;
        }
        shift += 7;
    }
}

static int trevrpc_huffman_symbol(uint32_t code, uint8_t bit_len, uint16_t* out_symbol) {
    if (bit_len > 30 || trevrpc_huffman_length_count[bit_len] == 0) {
        return 0;
    }
    size_t low = trevrpc_huffman_length_start[bit_len];
    size_t high = low + trevrpc_huffman_length_count[bit_len];
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (trevrpc_huffman_table[middle].code < code) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if (low < 257 && trevrpc_huffman_table[low].bit_len == bit_len && trevrpc_huffman_table[low].code == code) {
        *out_symbol = trevrpc_huffman_table[low].symbol;
        return 1;
    }
    return 0;
}

static int trevrpc_huffman_decode(
    const uint8_t* encoded, size_t encoded_len, uint8_t* out, size_t max_decoded_len, size_t* out_len) {
    uint32_t code = 0;
    uint8_t bit_len = 0;
    size_t decoded_len = 0;

    for (size_t i = 0; i < encoded_len; i++) {
        for (unsigned bit = 0; bit < 8; bit++) {
            code = (code << 1u) | ((encoded[i] >> (7u - bit)) & 1u);
            bit_len++;
            uint16_t symbol = 0;
            if (!trevrpc_huffman_symbol(code, bit_len, &symbol)) {
                if (bit_len == 30) {
                    return -1;
                }
                continue;
            }
            if (symbol == TREV_HPACK_EOS) {
                return -1;
            }
            if (decoded_len == max_decoded_len) {
                return -2;
            }
            if (out != NULL) {
                out[decoded_len] = (uint8_t)symbol;
            }
            decoded_len++;
            code = 0;
            bit_len = 0;
        }
    }

    if (bit_len != 0 && (bit_len > 7 || code != ((1u << bit_len) - 1u))) {
        return -1;
    }
    *out_len = decoded_len;
    return 0;
}

static trevrpc_qpack_status trevrpc_qpack_parse_string(
    trevrpc_qpack_parse_state* state, unsigned prefix_bits, uint8_t huffman_mask, trevrpc_qpack_string* out_string) {
    if (state->offset >= state->encoded_len) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }
    int huffman = (state->encoded[state->offset] & huffman_mask) != 0;
    uint64_t encoded_string_len_wire = 0;
    size_t encoded_string_len = 0;
    if (trevrpc_qpack_integer(
            state->encoded, state->encoded_len, &state->offset, prefix_bits, &encoded_string_len_wire) != 0 ||
        trevrpc_uint64_to_size(encoded_string_len_wire, &encoded_string_len) != 0) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }
    if (encoded_string_len > state->encoded_len - state->offset) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }

    const uint8_t* string_bytes = state->encoded + state->offset;
    state->offset += encoded_string_len;
    size_t decoded_len = encoded_string_len;
    if (huffman) {
        int result = trevrpc_huffman_decode(string_bytes, encoded_string_len, NULL, SIZE_MAX, &decoded_len);
        if (result != 0) {
            return TREV_QPACK_DECOMPRESSION_FAILED;
        }
    }
    *out_string = (trevrpc_qpack_string){
        .encoded = string_bytes,
        .encoded_len = encoded_string_len,
        .decoded_len = decoded_len,
        .huffman = huffman,
    };
    return TREV_QPACK_OK;
}

static trevrpc_qpack_string trevrpc_qpack_static_string(const char* value, size_t value_len) {
    return (trevrpc_qpack_string){
        .encoded = (const uint8_t*)value,
        .encoded_len = value_len,
        .decoded_len = value_len,
        .huffman = 0,
    };
}

static trevrpc_qpack_status trevrpc_qpack_accumulate_field(
    trevrpc_qpack_parse_state* state, const trevrpc_qpack_decoded_field* field) {
    if (state->field_count == state->limits->max_field_count) {
        return TREV_QPACK_FIELD_COUNT_EXCEEDED;
    }
    size_t field_size = 32;
    if (trevrpc_size_add(field_size, field->name.decoded_len, &field_size) != 0 ||
        trevrpc_size_add(field_size, field->value.decoded_len, &field_size) != 0 ||
        trevrpc_size_add(state->decoded_size, field_size, &state->decoded_size) != 0 ||
        state->decoded_size > state->limits->max_decoded_size) {
        return TREV_QPACK_DECODED_SIZE_EXCEEDED;
    }
    state->field_count++;
    return TREV_QPACK_OK;
}

static int trevrpc_qpack_copy_string(const trevrpc_qpack_string* string, uint8_t* destination) {
    if (!string->huffman) {
        if (string->decoded_len != 0) {
            memcpy(destination, string->encoded, string->decoded_len);
        }
        return 0;
    }
    size_t decoded_len = 0;
    return trevrpc_huffman_decode(
               string->encoded, string->encoded_len, destination, string->decoded_len, &decoded_len) == 0 &&
                   decoded_len == string->decoded_len
               ? 0
               : -1;
}

static trevrpc_qpack_status trevrpc_qpack_emit_field(
    trevrpc_qpack_parse_state* state, const trevrpc_qpack_decoded_field* decoded) {
    trevrpc_qpack_field* field = &state->fields[state->field_index++];
    field->name.data = state->bytes;
    field->name.len = decoded->name.decoded_len;
    if (trevrpc_qpack_copy_string(&decoded->name, state->bytes) != 0) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }
    state->bytes += decoded->name.decoded_len;
    field->value.data = state->bytes;
    field->value.len = decoded->value.decoded_len;
    if (trevrpc_qpack_copy_string(&decoded->value, state->bytes) != 0) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }
    state->bytes += decoded->value.decoded_len;
    return TREV_QPACK_OK;
}

static trevrpc_qpack_status trevrpc_qpack_parse_prefix(trevrpc_qpack_parse_state* state) {
    uint64_t required_insert_count = 0;
    if (trevrpc_qpack_integer(state->encoded, state->encoded_len, &state->offset, 8, &required_insert_count) != 0 ||
        required_insert_count != 0 || state->offset >= state->encoded_len) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }

    int sign = (state->encoded[state->offset] & 0x80u) != 0;
    uint64_t delta_base = 0;
    if (trevrpc_qpack_integer(state->encoded, state->encoded_len, &state->offset, 7, &delta_base) != 0 ||
        delta_base > TREV_QPACK_MAX_INTEGER) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }

    /*
     * This static-only subset requires the reconstructed Base to be zero. With
     * Required Insert Count zero, only S=0 and Delta Base zero satisfy that
     * constraint. Other prefixes can be valid for a general QPACK decoder.
     */
    if (sign || delta_base != 0) {
        return TREV_QPACK_DECOMPRESSION_FAILED;
    }
    return TREV_QPACK_OK;
}

static trevrpc_qpack_status trevrpc_qpack_parse_fields(trevrpc_qpack_parse_state* state, int emit) {
    trevrpc_qpack_status status = trevrpc_qpack_parse_prefix(state);
    if (status != TREV_QPACK_OK) {
        return status;
    }
    while (state->offset < state->encoded_len) {
        uint8_t representation = state->encoded[state->offset];
        trevrpc_qpack_decoded_field field = {0};
        if ((representation & 0x80u) != 0) {
            if ((representation & 0x40u) == 0) {
                return TREV_QPACK_DECOMPRESSION_FAILED;
            }
            uint64_t index_wire = 0;
            if (trevrpc_qpack_integer(state->encoded, state->encoded_len, &state->offset, 6, &index_wire) != 0 ||
                index_wire >= TREV_QPACK_STATIC_TABLE_LEN) {
                return TREV_QPACK_DECOMPRESSION_FAILED;
            }
            size_t index = (size_t)index_wire;
            const trevrpc_qpack_static_entry* entry = &trevrpc_qpack_static_table[index];
            field.name = trevrpc_qpack_static_string(entry->name, entry->name_len);
            field.value = trevrpc_qpack_static_string(entry->value, entry->value_len);
        } else if ((representation & 0xc0u) == 0x40u) {
            if ((representation & 0x10u) == 0) {
                return TREV_QPACK_DECOMPRESSION_FAILED;
            }
            uint64_t index_wire = 0;
            if (trevrpc_qpack_integer(state->encoded, state->encoded_len, &state->offset, 4, &index_wire) != 0 ||
                index_wire >= TREV_QPACK_STATIC_TABLE_LEN) {
                return TREV_QPACK_DECOMPRESSION_FAILED;
            }
            size_t index = (size_t)index_wire;
            const trevrpc_qpack_static_entry* entry = &trevrpc_qpack_static_table[index];
            field.name = trevrpc_qpack_static_string(entry->name, entry->name_len);
            status = trevrpc_qpack_parse_string(state, 7, 0x80u, &field.value);
            if (status != TREV_QPACK_OK) {
                return status;
            }
        } else if ((representation & 0xe0u) == 0x20u) {
            status = trevrpc_qpack_parse_string(state, 3, 0x08u, &field.name);
            if (status != TREV_QPACK_OK) {
                return status;
            }
            status = trevrpc_qpack_parse_string(state, 7, 0x80u, &field.value);
            if (status != TREV_QPACK_OK) {
                return status;
            }
        } else {
            /* Dynamic indexed/name references and both post-base forms. */
            return TREV_QPACK_DECOMPRESSION_FAILED;
        }

        status = emit ? trevrpc_qpack_emit_field(state, &field) : trevrpc_qpack_accumulate_field(state, &field);
        if (status != TREV_QPACK_OK) {
            return status;
        }
    }
    return TREV_QPACK_OK;
}

void trevrpc_qpack_field_section_init(trevrpc_qpack_field_section* section) {
    if (section != NULL) {
        *section = (trevrpc_qpack_field_section){0};
    }
}

void trevrpc_qpack_field_section_release(trevrpc_qpack_field_section* section) {
    if (section != NULL) {
        free(section->allocation);
        *section = (trevrpc_qpack_field_section){0};
    }
}

trevrpc_qpack_status trevrpc_qpack_static_decode(const uint8_t* encoded,
    size_t encoded_len,
    const trevrpc_qpack_limits* limits,
    trevrpc_qpack_field_section* out_section) {
    if (out_section == NULL) {
        return TREV_QPACK_INVALID_ARGUMENT;
    }
    if ((encoded == NULL && encoded_len != 0) || limits == NULL) {
        return TREV_QPACK_INVALID_ARGUMENT;
    }
    if (encoded_len > limits->max_encoded_size) {
        return TREV_QPACK_ENCODED_SIZE_EXCEEDED;
    }

    trevrpc_qpack_parse_state first = {
        .encoded = encoded,
        .encoded_len = encoded_len,
        .limits = limits,
    };
    trevrpc_qpack_status status = trevrpc_qpack_parse_fields(&first, 0);
    if (status != TREV_QPACK_OK) {
        return status;
    }

    size_t fields_bytes = 0;
    size_t string_bytes = first.decoded_size - first.field_count * 32u;
    size_t allocation_size = 0;
    if (trevrpc_size_multiply(first.field_count, sizeof(trevrpc_qpack_field), &fields_bytes) != 0 ||
        trevrpc_size_add(fields_bytes, string_bytes, &allocation_size) != 0) {
        return TREV_QPACK_DECODED_SIZE_EXCEEDED;
    }
    void* allocation = allocation_size == 0 ? NULL : malloc(allocation_size);
    if (allocation_size != 0 && allocation == NULL) {
        return TREV_QPACK_ALLOCATION_FAILED;
    }

    trevrpc_qpack_parse_state second = {
        .encoded = encoded,
        .encoded_len = encoded_len,
        .limits = limits,
        .fields = allocation,
        .bytes = allocation == NULL ? NULL : (uint8_t*)allocation + fields_bytes,
    };
    status = trevrpc_qpack_parse_fields(&second, 1);
    if (status != TREV_QPACK_OK || second.field_index != first.field_count) {
        free(allocation);
        return status == TREV_QPACK_OK ? TREV_QPACK_DECOMPRESSION_FAILED : status;
    }

    trevrpc_qpack_field_section replacement = {
        .fields = allocation,
        .field_count = first.field_count,
        .encoded_size = encoded_len,
        .decoded_size = first.decoded_size,
        .allocation = allocation,
    };
    trevrpc_qpack_field_section_release(out_section);
    *out_section = replacement;
    return TREV_QPACK_OK;
}

uint64_t trevrpc_qpack_status_error_code(trevrpc_qpack_status status) {
    switch (status) {
    case TREV_QPACK_DECOMPRESSION_FAILED:
        return TREV_QPACK_ERROR_DECOMPRESSION_FAILED;
    case TREV_QPACK_ENCODED_SIZE_EXCEEDED:
    case TREV_QPACK_DECODED_SIZE_EXCEEDED:
    case TREV_QPACK_FIELD_COUNT_EXCEEDED:
        return TREV_H3_ERROR_EXCESSIVE_LOAD;
    case TREV_QPACK_ALLOCATION_FAILED:
        return TREV_H3_ERROR_INTERNAL_ERROR;
    case TREV_QPACK_OK:
    case TREV_QPACK_INVALID_ARGUMENT:
        return 0;
    }
    return 0;
}

static int trevrpc_qpack_encoder_integer(
    trevrpc_qpack_static_encoder* encoder, unsigned prefix_bits, uint8_t flags, uint64_t value) {
    uint64_t prefix_max;
    if (encoder == NULL || encoder->output == NULL || prefix_bits == 0 || prefix_bits > 8 ||
        encoder->length >= encoder->capacity || value > TREV_QPACK_MAX_INTEGER)
        return -EINVAL;
    prefix_max = (UINT64_C(1) << prefix_bits) - 1u;
    if (value < prefix_max) {
        encoder->output[encoder->length++] = (uint8_t)(flags | value);
        return 0;
    }
    encoder->output[encoder->length++] = (uint8_t)(flags | prefix_max);
    value -= prefix_max;
    while (value >= 128) {
        if (encoder->length >= encoder->capacity)
            return -ENOBUFS;
        encoder->output[encoder->length++] = (uint8_t)(0x80u | (value & 0x7fu));
        value >>= 7;
    }
    if (encoder->length >= encoder->capacity)
        return -ENOBUFS;
    encoder->output[encoder->length++] = (uint8_t)value;
    return 0;
}

static int trevrpc_qpack_encoder_bytes(trevrpc_qpack_static_encoder* encoder, const uint8_t* value, size_t value_len) {
    int result;
    if (encoder == NULL || (value == NULL && value_len != 0))
        return -EINVAL;
    result = trevrpc_qpack_encoder_integer(encoder, 7, 0, value_len);
    if (result != 0)
        return result;
    if (value_len > encoder->capacity - encoder->length)
        return -ENOBUFS;
    if (value_len != 0)
        memcpy(encoder->output + encoder->length, value, value_len);
    encoder->length += value_len;
    return 0;
}

int trevrpc_qpack_static_encoder_init(trevrpc_qpack_static_encoder* encoder, uint8_t* output, size_t capacity) {
    if (encoder == NULL || output == NULL || capacity < 2)
        return -EINVAL;
    output[0] = 0;
    output[1] = 0;
    encoder->output = output;
    encoder->capacity = capacity;
    encoder->length = 2;
    return 0;
}

int trevrpc_qpack_static_encoder_put_indexed(trevrpc_qpack_static_encoder* encoder, uint64_t static_index) {
    return trevrpc_qpack_encoder_integer(encoder, 6, 0xc0, static_index);
}

int trevrpc_qpack_static_encoder_put_literal_name_reference(
    trevrpc_qpack_static_encoder* encoder, uint64_t static_name_index, const uint8_t* value, size_t value_len) {
    int result = trevrpc_qpack_encoder_integer(encoder, 4, 0x50, static_name_index);
    return result != 0 ? result : trevrpc_qpack_encoder_bytes(encoder, value, value_len);
}

int trevrpc_qpack_static_encoder_put_literal(trevrpc_qpack_static_encoder* encoder,
    const uint8_t* name,
    size_t name_len,
    const uint8_t* value,
    size_t value_len) {
    int result;
    if (name == NULL && name_len != 0)
        return -EINVAL;
    result = trevrpc_qpack_encoder_integer(encoder, 3, 0x20, name_len);
    if (result != 0)
        return result;
    if (name_len > encoder->capacity - encoder->length)
        return -ENOBUFS;
    if (name_len != 0)
        memcpy(encoder->output + encoder->length, name, name_len);
    encoder->length += name_len;
    return trevrpc_qpack_encoder_bytes(encoder, value, value_len);
}

const trevrpc_qpack_field* trevrpc_qpack_field_at(const trevrpc_qpack_field_section* section, size_t index) {
    if (section == NULL || index >= section->field_count) {
        return NULL;
    }
    return &section->fields[index];
}

int trevrpc_qpack_bytes_equal(trevrpc_qpack_bytes bytes, const uint8_t* value, size_t value_len) {
    if ((bytes.data == NULL && bytes.len != 0) || (value == NULL && value_len != 0) || bytes.len != value_len) {
        return 0;
    }
    return bytes.len == 0 || memcmp(bytes.data, value, bytes.len) == 0;
}

int trevrpc_qpack_field_name_equal(const trevrpc_qpack_field* field, const uint8_t* name, size_t name_len) {
    return field != NULL && trevrpc_qpack_bytes_equal(field->name, name, name_len);
}

int trevrpc_qpack_find_first(const trevrpc_qpack_field_section* section,
    const uint8_t* name,
    size_t name_len,
    size_t start_index,
    size_t* out_index) {
    if (section == NULL || (name == NULL && name_len != 0) || out_index == NULL || start_index > section->field_count) {
        return 0;
    }
    for (size_t i = start_index; i < section->field_count; i++) {
        if (trevrpc_qpack_field_name_equal(&section->fields[i], name, name_len)) {
            *out_index = i;
            return 1;
        }
    }
    return 0;
}
