#include "google/protobuf/compiler/plugin.pb-c.h"

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef Google__Protobuf__Compiler__CodeGeneratorRequest code_generator_request;
typedef Google__Protobuf__Compiler__CodeGeneratorResponse code_generator_response;
typedef Google__Protobuf__Compiler__CodeGeneratorResponse__File code_generator_response_file;
typedef Google__Protobuf__DescriptorProto descriptor_proto;
typedef Google__Protobuf__FileDescriptorProto file_descriptor_proto;
typedef Google__Protobuf__MethodDescriptorProto method_descriptor_proto;
typedef Google__Protobuf__ServiceDescriptorProto service_descriptor_proto;

typedef struct {
    char* header_suffix;
    char* source_suffix;
    char* runtime_include;
} plugin_options;

typedef struct {
    char* proto_name;
    char* c_type;
    char* c_prefix;
} type_ref;

typedef struct {
    type_ref* items;
    size_t len;
    size_t cap;
} type_index;

typedef struct {
    char* name;
    char* c_name;
    const type_ref* input;
    const type_ref* output;
    bool client_streaming;
    bool server_streaming;
} method_info;

typedef struct {
    method_info* items;
    size_t len;
    size_t cap;
} method_list;

typedef struct {
    const type_ref** items;
    size_t len;
    size_t cap;
} type_ref_list;

typedef struct {
    char* proto_name;
    char* c_name;
    char* type_name;
    method_list methods;
    type_ref_list inputs;
    type_ref_list outputs;
} service_info;

typedef struct {
    service_info* items;
    size_t len;
    size_t cap;
} service_list;

typedef struct {
    char* name;
    char* content;
} generated_file;

typedef struct {
    generated_file* items;
    size_t len;
    size_t cap;
} generated_file_list;

typedef struct {
    char* name;
    char* declaration;
    bool macro;
} generated_identifier;

typedef struct {
    generated_identifier* items;
    size_t len;
    size_t cap;
} generated_identifier_list;

typedef struct {
    char* data;
    size_t len;
    size_t cap;
    bool failed;
} string_builder;

static const char* str_or_empty(const char* value) {
    return value == NULL ? "" : value;
}

static char* duplicate_range(const char* value, size_t len) {
    char* out = malloc(len + 1);
    if (out == NULL) {
        return NULL;
    }
    if (len > 0) {
        memcpy(out, value, len);
    }
    out[len] = '\0';
    return out;
}

static char* duplicate_string(const char* value) {
    value = str_or_empty(value);
    return duplicate_range(value, strlen(value));
}

static char* format_string(const char* format, ...) {
    va_list args;
    va_start(args, format);
    va_list copy;
    va_copy(copy, args);
    int needed = vsnprintf(NULL, 0, format, copy);
    va_end(copy);
    if (needed < 0) {
        va_end(args);
        return NULL;
    }

    char* out = malloc((size_t)needed + 1);
    if (out == NULL) {
        va_end(args);
        return NULL;
    }
    vsnprintf(out, (size_t)needed + 1, format, args);
    va_end(args);
    return out;
}

static bool set_error(char** error, const char* format, ...) {
    if (error == NULL || *error != NULL) {
        return false;
    }

    va_list args;
    va_start(args, format);
    va_list copy;
    va_copy(copy, args);
    int needed = vsnprintf(NULL, 0, format, copy);
    va_end(copy);
    if (needed < 0) {
        va_end(args);
        *error = duplicate_string("internal formatting error");
        return false;
    }

    *error = malloc((size_t)needed + 1);
    if (*error == NULL) {
        va_end(args);
        return false;
    }
    vsnprintf(*error, (size_t)needed + 1, format, args);
    va_end(args);
    return false;
}

static bool set_oom(char** error) {
    return set_error(error, "out of memory");
}

static void string_builder_init(string_builder* builder) {
    builder->data = NULL;
    builder->len = 0;
    builder->cap = 0;
    builder->failed = false;
}

static bool string_builder_reserve(string_builder* builder, size_t extra) {
    if (builder->failed) {
        return false;
    }
    if (extra > SIZE_MAX - builder->len - 1) {
        builder->failed = true;
        return false;
    }
    size_t needed = builder->len + extra + 1;
    if (needed <= builder->cap) {
        return true;
    }

    size_t next = builder->cap == 0 ? 256 : builder->cap;
    while (next < needed) {
        if (next > SIZE_MAX / 2) {
            next = needed;
            break;
        }
        next *= 2;
    }

    char* data = realloc(builder->data, next);
    if (data == NULL) {
        builder->failed = true;
        return false;
    }
    builder->data = data;
    builder->cap = next;
    return true;
}

static bool string_builder_append_len(string_builder* builder, const char* value, size_t len) {
    if (builder->len == SIZE_MAX || !string_builder_reserve(builder, len)) {
        return false;
    }
    if (builder->data == NULL || builder->cap <= builder->len || len > builder->cap - builder->len - 1) {
        builder->failed = true;
        return false;
    }
    if (len > 0) {
        memcpy(builder->data + builder->len, value, len);
    }
    builder->len += len;
    builder->data[builder->len] = '\0';
    return true;
}

static bool string_builder_append(string_builder* builder, const char* value) {
    value = str_or_empty(value);
    return string_builder_append_len(builder, value, strlen(value));
}

static bool string_builder_append_char(string_builder* builder, char value) {
    return string_builder_append_len(builder, &value, 1);
}

static bool string_builder_appendf(string_builder* builder, const char* format, ...) {
    if (builder->failed) {
        return false;
    }

    va_list args;
    va_start(args, format);
    va_list copy;
    va_copy(copy, args);
    int needed = vsnprintf(NULL, 0, format, copy);
    va_end(copy);
    if (needed < 0) {
        va_end(args);
        builder->failed = true;
        return false;
    }

    if (!string_builder_reserve(builder, (size_t)needed)) {
        va_end(args);
        return false;
    }
    vsnprintf(builder->data + builder->len, (size_t)needed + 1, format, args);
    builder->len += (size_t)needed;
    va_end(args);
    return true;
}

static char* string_builder_steal(string_builder* builder) {
    if (builder->failed) {
        free(builder->data);
        builder->data = NULL;
        return NULL;
    }
    if (builder->data == NULL) {
        builder->data = duplicate_string("");
        if (builder->data == NULL) {
            return NULL;
        }
    }
    char* out = builder->data;
    builder->data = NULL;
    builder->len = 0;
    builder->cap = 0;
    return out;
}

static void string_builder_free(string_builder* builder) {
    free(builder->data);
    builder->data = NULL;
    builder->len = 0;
    builder->cap = 0;
    builder->failed = false;
}

static bool read_all(FILE* input, uint8_t** out, size_t* out_len) {
    uint8_t* data = NULL;
    size_t len = 0;
    size_t cap = 0;

    for (;;) {
        if (len == cap) {
            size_t next = cap == 0 ? 8192 : cap * 2;
            if (next < cap) {
                free(data);
                return false;
            }
            uint8_t* resized = realloc(data, next);
            if (resized == NULL) {
                free(data);
                return false;
            }
            data = resized;
            cap = next;
        }

        size_t n = fread(data + len, 1, cap - len, input);
        len += n;
        if (n == 0) {
            if (ferror(input)) {
                free(data);
                return false;
            }
            break;
        }
    }

    *out = data;
    *out_len = len;
    return true;
}

static bool write_all(FILE* output, const uint8_t* data, size_t len) {
    while (len > 0) {
        size_t n = fwrite(data, 1, len, output);
        if (n == 0) {
            return false;
        }
        data += n;
        len -= n;
    }
    return !ferror(output);
}

static bool type_index_append(type_index* index, char* proto_name, char* c_type, char* c_prefix, char** error) {
    for (size_t i = 0; i < index->len; i++) {
        const type_ref* existing = &index->items[i];
        if (strcmp(existing->proto_name, proto_name) != 0 && strcmp(existing->c_type, c_type) == 0) {
            return set_error(error,
                "protobuf messages \"%s\" and \"%s\" normalize to duplicate C type \"%s\"",
                existing->proto_name,
                proto_name,
                c_type);
        }
        if (strcmp(existing->proto_name, proto_name) != 0 && strcmp(existing->c_prefix, c_prefix) == 0) {
            return set_error(error,
                "protobuf messages \"%s\" and \"%s\" normalize to duplicate C symbol prefix \"%s\"",
                existing->proto_name,
                proto_name,
                c_prefix);
        }
    }
    if (index->len == index->cap) {
        size_t next = index->cap == 0 ? 16 : index->cap * 2;
        type_ref* items = realloc(index->items, next * sizeof(*items));
        if (items == NULL) {
            return false;
        }
        index->items = items;
        index->cap = next;
    }
    index->items[index->len++] = (type_ref){
        .proto_name = proto_name,
        .c_type = c_type,
        .c_prefix = c_prefix,
    };
    return true;
}

static void type_index_free(type_index* index) {
    for (size_t i = 0; i < index->len; i++) {
        free(index->items[i].proto_name);
        free(index->items[i].c_type);
        free(index->items[i].c_prefix);
    }
    free(index->items);
    index->items = NULL;
    index->len = 0;
    index->cap = 0;
}

static const type_ref* type_index_find(const type_index* index, const char* proto_name) {
    for (size_t i = 0; i < index->len; i++) {
        if (strcmp(index->items[i].proto_name, proto_name) == 0) {
            return &index->items[i];
        }
    }
    return NULL;
}

static bool method_list_append(method_list* list, method_info value) {
    if (list->len == list->cap) {
        size_t next = list->cap == 0 ? 8 : list->cap * 2;
        method_info* items = realloc(list->items, next * sizeof(*items));
        if (items == NULL) {
            return false;
        }
        list->items = items;
        list->cap = next;
    }
    list->items[list->len++] = value;
    return true;
}

static void method_list_free(method_list* list) {
    for (size_t i = 0; i < list->len; i++) {
        free(list->items[i].name);
        free(list->items[i].c_name);
    }
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static bool type_ref_list_contains(const type_ref_list* list, const char* proto_name) {
    for (size_t i = 0; i < list->len; i++) {
        if (strcmp(list->items[i]->proto_name, proto_name) == 0) {
            return true;
        }
    }
    return false;
}

static bool type_ref_list_append(type_ref_list* list, const type_ref* value) {
    if (list->len == list->cap) {
        size_t next = list->cap == 0 ? 8 : list->cap * 2;
        const type_ref** items = realloc(list->items, next * sizeof(*items));
        if (items == NULL) {
            return false;
        }
        list->items = items;
        list->cap = next;
    }
    list->items[list->len++] = value;
    return true;
}

static void type_ref_list_free(type_ref_list* list) {
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static int compare_type_ref_by_c_type(const void* left, const void* right) {
    const type_ref* const* lhs = left;
    const type_ref* const* rhs = right;
    return strcmp((*lhs)->c_type, (*rhs)->c_type);
}

static bool service_list_append(service_list* list, service_info value) {
    if (list->len == list->cap) {
        size_t next = list->cap == 0 ? 4 : list->cap * 2;
        service_info* items = realloc(list->items, next * sizeof(*items));
        if (items == NULL) {
            return false;
        }
        list->items = items;
        list->cap = next;
    }
    list->items[list->len++] = value;
    return true;
}

static void service_info_free(service_info* service) {
    free(service->proto_name);
    free(service->c_name);
    free(service->type_name);
    method_list_free(&service->methods);
    type_ref_list_free(&service->inputs);
    type_ref_list_free(&service->outputs);
}

static void service_list_free(service_list* list) {
    for (size_t i = 0; i < list->len; i++) {
        service_info_free(&list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static bool generated_file_list_append(generated_file_list* list, char* name, char* content) {
    if (list->len == list->cap) {
        size_t next = list->cap == 0 ? 4 : list->cap * 2;
        generated_file* items = realloc(list->items, next * sizeof(*items));
        if (items == NULL) {
            return false;
        }
        list->items = items;
        list->cap = next;
    }
    list->items[list->len++] = (generated_file){.name = name, .content = content};
    return true;
}

static void generated_file_list_free(generated_file_list* list) {
    for (size_t i = 0; i < list->len; i++) {
        free(list->items[i].name);
        free(list->items[i].content);
    }
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static void generated_identifier_list_free(generated_identifier_list* list) {
    for (size_t i = 0; i < list->len; i++) {
        free(list->items[i].name);
        free(list->items[i].declaration);
    }
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static bool generated_identifier_list_add(
    generated_identifier_list* list, const char* name, const char* declaration, bool macro, char** error) {
    for (size_t i = 0; i < list->len; i++) {
        const generated_identifier* existing = &list->items[i];
        if (existing->macro == macro && strcmp(existing->name, name) == 0) {
            return set_error(error,
                "generated declarations %s and %s normalize to duplicate C %s \"%s\"",
                existing->declaration,
                declaration,
                macro ? "macro" : "symbol",
                name);
        }
    }

    if (list->len == list->cap) {
        size_t next = list->cap == 0 ? 16 : list->cap * 2;
        generated_identifier* items = realloc(list->items, next * sizeof(*items));
        if (items == NULL) {
            return set_oom(error);
        }
        list->items = items;
        list->cap = next;
    }
    char* owned_name = duplicate_string(name);
    char* owned_declaration = duplicate_string(declaration);
    if (owned_name == NULL || owned_declaration == NULL) {
        free(owned_name);
        free(owned_declaration);
        return set_oom(error);
    }
    list->items[list->len++] = (generated_identifier){
        .name = owned_name,
        .declaration = owned_declaration,
        .macro = macro,
    };
    return true;
}

static char* path_extension(const char* name) {
    const char* slash = strrchr(name, '/');
    const char* base = slash == NULL ? name : slash + 1;
    const char* dot = strrchr(base, '.');
    return dot == NULL ? duplicate_string("") : duplicate_string(dot);
}

static const char* path_base_ptr(const char* name) {
    const char* slash = strrchr(name, '/');
    return slash == NULL ? name : slash + 1;
}

static char* output_file_name(const char* input_name, const char* suffix) {
    char* ext = path_extension(input_name);
    if (ext == NULL) {
        return NULL;
    }
    size_t input_len = strlen(input_name);
    size_t ext_len = strlen(ext);
    size_t stem_len = input_len >= ext_len ? input_len - ext_len : input_len;
    size_t suffix_len = strlen(suffix);
    char* out = malloc(stem_len + suffix_len + 1);
    if (out != NULL) {
        memcpy(out, input_name, stem_len);
        memcpy(out + stem_len, suffix, suffix_len);
        out[stem_len + suffix_len] = '\0';
    }
    free(ext);
    return out;
}

static char* protobuf_c_header(const char* input_name) {
    char* output = output_file_name(input_name, ".pb-c.h");
    if (output == NULL) {
        return NULL;
    }
    char* base = duplicate_string(path_base_ptr(output));
    free(output);
    return base;
}

static char* header_guard(const char* name) {
    string_builder builder;
    string_builder_init(&builder);
    for (const unsigned char* p = (const unsigned char*)name; *p != '\0'; p++) {
        unsigned char c = *p;
        if (c == '_' || isalnum(c)) {
            string_builder_append_char(&builder, (char)toupper(c));
        } else {
            string_builder_append_char(&builder, '_');
        }
    }
    return string_builder_steal(&builder);
}

static char* to_camel(const char* name) {
    string_builder builder;
    string_builder_init(&builder);
    const char* start = name;
    for (const char* p = name;; p++) {
        if (*p == '_' || *p == '-' || *p == '.' || *p == '\0') {
            if (p > start) {
                char first = *start;
                string_builder_append_char(&builder, (char)toupper((unsigned char)first));
                string_builder_append_len(&builder, start + 1, (size_t)(p - start - 1));
            }
            if (*p == '\0') {
                break;
            }
            start = p + 1;
        }
    }
    return string_builder_steal(&builder);
}

static char* to_snake(const char* name) {
    string_builder builder;
    string_builder_init(&builder);
    bool prev_lower = false;
    for (size_t i = 0; name[i] != '\0'; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '-' || c == '.' || c == '_') {
            if (builder.len > 0 && builder.data[builder.len - 1] != '_') {
                string_builder_append_char(&builder, '_');
            }
            prev_lower = false;
            continue;
        }
        if (isupper(c) && prev_lower && i > 0) {
            string_builder_append_char(&builder, '_');
        }
        if (isalnum(c)) {
            string_builder_append_char(&builder, (char)tolower(c));
            prev_lower = islower(c) || isdigit(c);
        }
    }

    char* raw = string_builder_steal(&builder);
    if (raw == NULL) {
        return NULL;
    }
    size_t raw_len = strlen(raw);
    size_t start = 0;
    while (start < raw_len && raw[start] == '_') {
        start++;
    }
    size_t end = raw_len;
    while (end > start && raw[end - 1] == '_') {
        end--;
    }
    size_t trimmed_len = end - start;
    if (trimmed_len == 0) {
        free(raw);
        return duplicate_string("x");
    }

    bool prefix_digit = isdigit((unsigned char)raw[start]);
    size_t prefix_len = prefix_digit ? 1 : 0;
    char* out = malloc(prefix_len + trimmed_len + 1);
    if (out == NULL) {
        free(raw);
        return NULL;
    }
    size_t offset = 0;
    if (prefix_digit) {
        out[offset++] = '_';
    }
    memcpy(out + offset, raw + start, trimmed_len);
    out[offset + trimmed_len] = '\0';
    free(raw);
    return out;
}

static char** split_nonempty(const char* value, char delimiter, size_t* out_len) {
    char** parts = NULL;
    size_t len = 0;
    size_t cap = 0;
    const char* start = value;
    for (const char* p = value;; p++) {
        if (*p == delimiter || *p == '\0') {
            if (p > start) {
                if (len == cap) {
                    size_t next = cap == 0 ? 4 : cap * 2;
                    char** resized = realloc(parts, next * sizeof(*parts));
                    if (resized == NULL) {
                        for (size_t i = 0; i < len; i++) {
                            free(parts[i]);
                        }
                        free(parts);
                        return NULL;
                    }
                    parts = resized;
                    cap = next;
                }
                parts[len] = duplicate_range(start, (size_t)(p - start));
                if (parts[len] == NULL) {
                    for (size_t i = 0; i < len; i++) {
                        free(parts[i]);
                    }
                    free(parts);
                    return NULL;
                }
                len++;
            }
            if (*p == '\0') {
                break;
            }
            start = p + 1;
        }
    }
    *out_len = len;
    return parts;
}

static void free_parts(char** parts, size_t n_parts) {
    for (size_t i = 0; i < n_parts; i++) {
        free(parts[i]);
    }
    free(parts);
}

typedef char* (*name_transform)(const char* value);

static char* join_transformed(
    const char* const* parts, size_t n_parts, const char* separator, name_transform transform) {
    string_builder builder;
    string_builder_init(&builder);
    for (size_t i = 0; i < n_parts; i++) {
        char* transformed = transform(parts[i]);
        if (transformed == NULL) {
            string_builder_free(&builder);
            return NULL;
        }
        if (i > 0) {
            string_builder_append(&builder, separator);
        }
        string_builder_append(&builder, transformed);
        free(transformed);
    }
    return string_builder_steal(&builder);
}

static char* full_proto_name(const char* proto_package, const char* const* names, size_t n_names) {
    string_builder builder;
    string_builder_init(&builder);
    string_builder_append_char(&builder, '.');
    if (proto_package != NULL && proto_package[0] != '\0') {
        string_builder_append(&builder, proto_package);
        if (n_names > 0) {
            string_builder_append_char(&builder, '.');
        }
    }
    for (size_t i = 0; i < n_names; i++) {
        if (i > 0) {
            string_builder_append_char(&builder, '.');
        }
        string_builder_append(&builder, names[i]);
    }
    return string_builder_steal(&builder);
}

static bool build_c_parts(const char* proto_package,
    const char* const* names,
    size_t n_names,
    const char*** out_parts,
    size_t* out_n_parts,
    char*** out_owned_package_parts,
    size_t* out_n_owned_package_parts) {
    size_t n_package_parts = 0;
    char** package_parts = NULL;
    if (proto_package != NULL && proto_package[0] != '\0') {
        package_parts = split_nonempty(proto_package, '.', &n_package_parts);
        if (package_parts == NULL && n_package_parts != 0) {
            return false;
        }
    }

    size_t n_parts = n_package_parts + n_names;
    const char** parts = malloc(n_parts * sizeof(*parts));
    if (parts == NULL && n_parts > 0) {
        free_parts(package_parts, n_package_parts);
        return false;
    }
    for (size_t i = 0; i < n_package_parts; i++) {
        parts[i] = package_parts[i];
    }
    for (size_t i = 0; i < n_names; i++) {
        parts[n_package_parts + i] = names[i];
    }

    *out_parts = parts;
    *out_n_parts = n_parts;
    *out_owned_package_parts = package_parts;
    *out_n_owned_package_parts = n_package_parts;
    return true;
}

static bool index_message(type_index* index,
    const char* proto_package,
    const char* const* parents,
    size_t n_parents,
    const descriptor_proto* message,
    char** error) {
    const char* message_name = str_or_empty(message->name);
    const char** names = malloc((n_parents + 1) * sizeof(*names));
    if (names == NULL) {
        return set_oom(error);
    }
    for (size_t i = 0; i < n_parents; i++) {
        names[i] = parents[i];
    }
    names[n_parents] = message_name;

    const char** c_parts = NULL;
    size_t n_c_parts = 0;
    char** package_parts = NULL;
    size_t n_package_parts = 0;
    if (!build_c_parts(proto_package, names, n_parents + 1, &c_parts, &n_c_parts, &package_parts, &n_package_parts)) {
        free(names);
        return set_oom(error);
    }

    char* proto_name = full_proto_name(proto_package, names, n_parents + 1);
    char* c_type = join_transformed(c_parts, n_c_parts, "__", to_camel);
    char* c_prefix = join_transformed(c_parts, n_c_parts, "__", to_snake);
    free((void*)c_parts);
    free_parts(package_parts, n_package_parts);
    if (proto_name == NULL || c_type == NULL || c_prefix == NULL) {
        free(names);
        free(proto_name);
        free(c_type);
        free(c_prefix);
        return set_oom(error);
    }
    if (!type_index_append(index, proto_name, c_type, c_prefix, error)) {
        free(names);
        free(proto_name);
        free(c_type);
        free(c_prefix);
        return set_oom(error);
    }

    for (size_t i = 0; i < message->n_nested_type; i++) {
        if (!index_message(index, proto_package, names, n_parents + 1, message->nested_type[i], error)) {
            free(names);
            return false;
        }
    }

    free(names);
    return true;
}

static bool build_type_index(const code_generator_request* request, type_index* index, char** error) {
    for (size_t i = 0; i < request->n_proto_file; i++) {
        const file_descriptor_proto* file = request->proto_file[i];
        for (size_t j = 0; j < file->n_message_type; j++) {
            if (!index_message(index, str_or_empty(file->package), NULL, 0, file->message_type[j], error)) {
                return false;
            }
        }
    }
    return true;
}

static const type_ref* c_type_for(
    const file_descriptor_proto* file, const char* proto_name, const type_index* index, char** error) {
    char* qualified = NULL;
    if (proto_name != NULL && proto_name[0] == '.') {
        qualified = duplicate_string(proto_name);
    } else {
        qualified = format_string(".%s.%s", str_or_empty(file->package), str_or_empty(proto_name));
    }
    if (qualified == NULL) {
        set_oom(error);
        return NULL;
    }

    const type_ref* ref = type_index_find(index, qualified);
    if (ref == NULL) {
        set_error(error, "unknown protobuf message type %s", qualified);
    }
    free(qualified);
    return ref;
}

static bool describe_method(const file_descriptor_proto* file,
    const method_descriptor_proto* method,
    const type_index* index,
    method_info* out,
    char** error) {
    const type_ref* input = c_type_for(file, str_or_empty(method->input_type), index, error);
    if (input == NULL) {
        return false;
    }
    const type_ref* output = c_type_for(file, str_or_empty(method->output_type), index, error);
    if (output == NULL) {
        return false;
    }

    char* name = duplicate_string(method->name);
    char* c_name = to_snake(str_or_empty(method->name));
    if (name == NULL || c_name == NULL) {
        free(name);
        free(c_name);
        return set_oom(error);
    }

    *out = (method_info){
        .name = name,
        .c_name = c_name,
        .input = input,
        .output = output,
        .client_streaming = method->client_streaming,
        .server_streaming = method->server_streaming,
    };
    return true;
}

static bool c_namespace_parts(const char* proto_package,
    const char* name,
    const char*** out_parts,
    size_t* out_n_parts,
    char*** out_owned_package_parts,
    size_t* out_n_owned_package_parts) {
    const char* names[1] = {name};
    return build_c_parts(
        proto_package, names, 1, out_parts, out_n_parts, out_owned_package_parts, out_n_owned_package_parts);
}

static bool describe_service(const file_descriptor_proto* file,
    const service_descriptor_proto* service,
    const type_index* index,
    service_info* out,
    char** error) {
    const char** service_parts = NULL;
    size_t n_service_parts = 0;
    char** package_parts = NULL;
    size_t n_package_parts = 0;
    if (!c_namespace_parts(str_or_empty(file->package),
            str_or_empty(service->name),
            &service_parts,
            &n_service_parts,
            &package_parts,
            &n_package_parts)) {
        return set_oom(error);
    }

    char* c_name = join_transformed(service_parts, n_service_parts, "_", to_snake);
    free((void*)service_parts);
    free_parts(package_parts, n_package_parts);
    if (c_name == NULL) {
        return set_oom(error);
    }
    char* type_name = format_string("%s_server", c_name);
    char* proto_name = NULL;
    if (file->package != NULL && file->package[0] != '\0') {
        proto_name = format_string("%s.%s", file->package, str_or_empty(service->name));
    } else {
        proto_name = duplicate_string(service->name);
    }
    if (type_name == NULL || proto_name == NULL) {
        free(c_name);
        free(type_name);
        free(proto_name);
        return set_oom(error);
    }

    service_info info = {
        .proto_name = proto_name,
        .c_name = c_name,
        .type_name = type_name,
    };

    for (size_t i = 0; i < service->n_method; i++) {
        method_info method = {0};
        if (!describe_method(file, service->method[i], index, &method, error)) {
            service_info_free(&info);
            return false;
        }
        if (!method_list_append(&info.methods, method)) {
            free(method.name);
            free(method.c_name);
            service_info_free(&info);
            return set_oom(error);
        }
        if (!type_ref_list_contains(&info.inputs, method.input->proto_name) &&
            !type_ref_list_append(&info.inputs, method.input)) {
            service_info_free(&info);
            return set_oom(error);
        }
        if (!type_ref_list_contains(&info.outputs, method.output->proto_name) &&
            !type_ref_list_append(&info.outputs, method.output)) {
            service_info_free(&info);
            return set_oom(error);
        }
    }

    if (info.inputs.len > 1) {
        qsort(info.inputs.items, info.inputs.len, sizeof(*info.inputs.items), compare_type_ref_by_c_type);
    }
    if (info.outputs.len > 1) {
        qsort(info.outputs.items, info.outputs.len, sizeof(*info.outputs.items), compare_type_ref_by_c_type);
    }
    *out = info;
    return true;
}

static bool describe_services(
    const file_descriptor_proto* file, const type_index* index, service_list* services, char** error) {
    for (size_t i = 0; i < file->n_service; i++) {
        service_info service = {0};
        if (!describe_service(file, file->service[i], index, &service, error)) {
            return false;
        }
        if (!service_list_append(services, service)) {
            service_info_free(&service);
            return set_oom(error);
        }
    }
    return true;
}

static char* c_quote(const char* value) {
    string_builder builder;
    string_builder_init(&builder);
    string_builder_append_char(&builder, '"');
    for (const unsigned char* p = (const unsigned char*)str_or_empty(value); *p != '\0'; p++) {
        switch (*p) {
        case '\\':
            string_builder_append(&builder, "\\\\");
            break;
        case '"':
            string_builder_append(&builder, "\\\"");
            break;
        case '\n':
            string_builder_append(&builder, "\\n");
            break;
        case '\r':
            string_builder_append(&builder, "\\r");
            break;
        case '\t':
            string_builder_append(&builder, "\\t");
            break;
        default:
            if (isprint(*p)) {
                string_builder_append_char(&builder, (char)*p);
            } else {
                string_builder_appendf(&builder, "\\x%02x", *p);
            }
            break;
        }
    }
    string_builder_append_char(&builder, '"');
    return string_builder_steal(&builder);
}

static char* macro_name(const char* value) {
    return header_guard(value);
}

static char* method_base_name(const service_info* service, const method_info* method) {
    return format_string("%s_%s", service->c_name, method->c_name);
}

static char* generated_test_prefix(const file_descriptor_proto* file) {
    char* package_name = to_snake(str_or_empty(file->package));
    char* file_name = to_snake(str_or_empty(file->name));
    if (package_name == NULL || file_name == NULL) {
        free(package_name);
        free(file_name);
        return NULL;
    }
    char* prefix = file->package != NULL && file->package[0] != '\0'
                       ? format_string("trevrpc_%s_%s", package_name, file_name)
                       : format_string("trevrpc_%s", file_name);
    free(package_name);
    free(file_name);
    return prefix;
}

static char* method_declaration(const service_info* service, const method_info* method) {
    return format_string("protobuf method \".%s.%s\"", str_or_empty(service->proto_name), str_or_empty(method->name));
}

static bool register_method_symbol(generated_identifier_list* identifiers,
    const char* base,
    const char* suffix,
    const char* declaration,
    char** error) {
    char* name = format_string("%s%s", base, suffix);
    if (name == NULL) {
        return set_oom(error);
    }
    bool ok = generated_identifier_list_add(identifiers, name, declaration, false, error);
    free(name);
    return ok;
}

static bool validate_generated_identifiers(const file_descriptor_proto* file,
    const service_list* services,
    const plugin_options* options,
    generated_identifier_list* identifiers,
    char** error) {
    char* header_name = output_file_name(str_or_empty(file->name), options->header_suffix);
    char* guard = header_name == NULL ? NULL : header_guard(header_name);
    char* file_declaration = format_string("protobuf file \"%s\"", str_or_empty(file->name));
    char* test_prefix = generated_test_prefix(file);
    if (header_name == NULL || guard == NULL || file_declaration == NULL || test_prefix == NULL) {
        free(header_name);
        free(guard);
        free(file_declaration);
        free(test_prefix);
        return set_oom(error);
    }

    bool ok = generated_identifier_list_add(identifiers, guard, file_declaration, true, error);
    if (ok) {
        char* test_symbol = format_string("%s_test_fail_allocation_after", test_prefix);
        if (test_symbol == NULL) {
            ok = set_oom(error);
        } else {
            ok = generated_identifier_list_add(identifiers, test_symbol, file_declaration, false, error);
            free(test_symbol);
        }
    }
    free(header_name);
    free(guard);
    free(file_declaration);
    free(test_prefix);
    if (!ok) {
        return false;
    }

    for (size_t i = 0; i < services->len; i++) {
        const service_info* service = &services->items[i];
        for (size_t j = 0; j < service->methods.len; j++) {
            const method_info* method = &service->methods.items[j];
            char* base = method_base_name(service, method);
            char* macro = base == NULL ? NULL : macro_name(base);
            char* declaration = base == NULL ? NULL : method_declaration(service, method);
            if (base == NULL || macro == NULL || declaration == NULL) {
                free(base);
                free(macro);
                free(declaration);
                return set_oom(error);
            }

            ok = generated_identifier_list_add(identifiers, macro, declaration, true, error);
            if (ok) {
                static const char* const common_suffixes[] = {
                    "_call_config_init",
                    "_open",
                    "_decode_request_receive",
                    "_decode_response_receive",
                    "_matches_incoming",
                    "_take_incoming",
                    "_accept",
                };
                for (size_t k = 0; ok && k < sizeof(common_suffixes) / sizeof(common_suffixes[0]); k++) {
                    ok = register_method_symbol(identifiers, base, common_suffixes[k], declaration, error);
                }
                if (ok && method->client_streaming) {
                    ok = register_method_symbol(
                        identifiers, base, method->server_streaming ? "_send_request" : "_send", declaration, error);
                    if (ok) {
                        ok = register_method_symbol(identifiers, base, "_finish_send", declaration, error);
                    }
                }
                if (ok && method->server_streaming) {
                    ok = register_method_symbol(
                        identifiers, base, method->client_streaming ? "_send_response" : "_send", declaration, error);
                    if (ok) {
                        ok = register_method_symbol(identifiers, base, "_finish", declaration, error);
                    }
                }
                if (ok && !method->server_streaming) {
                    ok = register_method_symbol(identifiers, base, "_respond", declaration, error);
                }
            }
            free(base);
            free(macro);
            free(declaration);
            if (!ok) {
                return false;
            }
        }
    }
    return true;
}

static char* method_rpc_kind(const method_info* method) {
    if (method->client_streaming && method->server_streaming)
        return duplicate_string("TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING");
    if (method->client_streaming)
        return duplicate_string("TREVRPC_RPC_KIND_CLIENT_STREAMING");
    if (method->server_streaming)
        return duplicate_string("TREVRPC_RPC_KIND_SERVER_STREAMING");
    return duplicate_string("TREVRPC_RPC_KIND_UNARY");
}

static void generate_header_method(string_builder* buffer, const service_info* service, const method_info* method) {
    char* base = method_base_name(service, method);
    char* macro = base == NULL ? NULL : macro_name(base);
    if (base == NULL || macro == NULL) {
        free(base);
        free(macro);
        buffer->failed = true;
        return;
    }
    char* kind = method_rpc_kind(method);
    if (kind == NULL) {
        free(base);
        free(macro);
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer, "#define %s_RPC_KIND %s\n\n", macro, kind);
    string_builder_appendf(buffer,
        "int %s_call_config_init(trevrpc_rpc_call_config_v1* config, size_t struct_size);\n"
        "int %s_open(trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, const %s* initial_or_null, "
        "uint64_t operation_id, trevrpc_rpc_call_v1* out_call, trevrpc_rpc_stream_v1* out_stream);\n",
        base,
        base,
        method->input->c_type);
    if (method->client_streaming) {
        if (method->server_streaming) {
            string_builder_appendf(buffer,
                "int %s_send_request(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, const %s* message, "
                "uint64_t operation_id);\n",
                base,
                method->input->c_type);
        } else {
            string_builder_appendf(buffer,
                "int %s_send(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, const %s* message, uint64_t "
                "operation_id);\n",
                base,
                method->input->c_type);
        }
        string_builder_appendf(buffer,
            "int %s_finish_send(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, uint64_t operation_id);\n",
            base);
    }
    if (method->server_streaming) {
        if (method->client_streaming) {
            string_builder_appendf(buffer,
                "int %s_send_response(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, const %s* message, "
                "uint64_t operation_id);\n",
                base,
                method->output->c_type);
        } else {
            string_builder_appendf(buffer,
                "int %s_send(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, const %s* message, uint64_t "
                "operation_id);\n",
                base,
                method->output->c_type);
        }
        string_builder_appendf(buffer,
            "int %s_finish(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id, "
            "const trevrpc_rpc_status_v1* status);\n",
            base);
    }
    if (!method->server_streaming) {
        string_builder_appendf(buffer,
            "int %s_respond(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, const %s* message, "
            "const trevrpc_rpc_status_v1* status, uint64_t operation_id);\n",
            base,
            method->output->c_type);
    }
    string_builder_appendf(buffer,
        "int %s_decode_request_receive(const trevrpc_rpc_receive* receive, %s** out_message);\n"
        "int %s_decode_response_receive(const trevrpc_rpc_receive* receive, %s** out_message);\n"
        "int %s_matches_incoming(const trevrpc_rpc_event_info_v1* info);\n"
        "/* On success, atomically transfers call, stream, and initial receive; "
        "outputs are unchanged on failure. Client-streaming and bidi initial "
        "receives may be empty and should be released without decoding. */\n"
        "int %s_take_incoming(trevrpc_rpc_event* event, trevrpc_rpc_call_v1* out_call, trevrpc_rpc_stream_v1* "
        "out_stream, trevrpc_rpc_receive** out_initial);\n"
        "int %s_accept(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id);\n\n",
        base,
        method->input->c_type,
        base,
        method->output->c_type,
        base,
        base,
        base);
    free(base);
    free(macro);
    free(kind);
}

static void generate_header_service(string_builder* buffer, const service_info* service) {
    for (size_t i = 0; i < service->methods.len; i++) {
        generate_header_method(buffer, service, &service->methods.items[i]);
    }
}

static void generate_pack_body(string_builder* buffer, const type_ref* message, const char* expression) {
    string_builder_appendf(buffer,
        "    size_t body_len = %s__get_packed_size(%s);\n"
        "    uint8_t stack_body[TREVRPC_GENERATED_STACK_BODY_LEN];\n"
        "    uint8_t* body = body_len == 0 ? stack_body : (body_len <= sizeof(stack_body) ? stack_body : "
        "malloc(body_len));\n"
        "    if (body_len > 0 && body == NULL) { return -ENOMEM; }\n"
        "    %s__pack(%s, body);\n",
        message->c_prefix,
        expression,
        message->c_prefix,
        expression);
}

static void generate_send_function(string_builder* buffer,
    const service_info* service,
    const method_info* method,
    const type_ref* message,
    const char* suffix) {
    char* base = method_base_name(service, method);
    char* name = base == NULL || suffix == NULL ? NULL : format_string("%s_send%s", base, suffix);
    if (base == NULL || name == NULL) {
        free(base);
        free(name);
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, const %s* message, uint64_t operation_id) "
        "{\n"
        "    if (runtime == NULL || message == NULL || operation_id == TREVRPC_RPC_OPERATION_ID_NONE) { return "
        "-EINVAL; }\n",
        name,
        message->c_type);
    generate_pack_body(buffer, message, "message");
    string_builder_append(buffer,
        "    int err = trevrpc_rpc_stream_send_copy_v1(runtime, stream, operation_id, body, body_len, "
        "TREVRPC_RPC_SEND_FLAG_NONE);\n"
        "    if (body != stack_body) { free(body); }\n"
        "    return err;\n}\n\n");
    free(base);
    free(name);
}

static void generate_finish_send_function(
    string_builder* buffer, const service_info* service, const method_info* method) {
    char* base = method_base_name(service, method);
    if (base == NULL) {
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s_finish_send(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, uint64_t operation_id) {\n"
        "    if (runtime == NULL || operation_id == TREVRPC_RPC_OPERATION_ID_NONE) { return -EINVAL; }\n"
        "    return trevrpc_rpc_stream_finish_send(runtime, stream, operation_id);\n}\n\n",
        base);
    free(base);
}

static void generate_finish_function(string_builder* buffer, const service_info* service, const method_info* method) {
    char* base = method_base_name(service, method);
    if (base == NULL) {
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s_finish(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id, "
        "const trevrpc_rpc_status_v1* status) {\n"
        "    if (runtime == NULL || status == NULL || operation_id == TREVRPC_RPC_OPERATION_ID_NONE) { return -EINVAL; "
        "}\n"
        "    return trevrpc_rpc_call_finish_v1(runtime, call, operation_id, status);\n}\n\n",
        base);
    free(base);
}

static void generate_config_and_open(string_builder* buffer, const service_info* service, const method_info* method) {
    char* base = method_base_name(service, method);
    char* service_name = c_quote(service->proto_name);
    char* method_name = c_quote(method->name);
    char* kind = method_rpc_kind(method);
    if (base == NULL || service_name == NULL || method_name == NULL || kind == NULL) {
        free(base);
        free(service_name);
        free(method_name);
        free(kind);
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s_call_config_init(trevrpc_rpc_call_config_v1* config, size_t struct_size) {\n"
        "    int err = trevrpc_rpc_call_config_v1_init(config, struct_size);\n"
        "    if (err != 0) { return err; }\n"
        "    config->kind = %s; config->service = %s; config->service_len = sizeof(%s) - 1;\n"
        "    config->method = %s; config->method_len = sizeof(%s) - 1; return 0;\n}\n\n",
        base,
        kind,
        service_name,
        service_name,
        method_name,
        method_name);
    string_builder_appendf(buffer,
        "int %s_open(trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, const %s* initial_or_null, "
        "uint64_t operation_id, trevrpc_rpc_call_v1* out_call, trevrpc_rpc_stream_v1* out_stream) {\n"
        "    if (runtime == NULL || operation_id == TREVRPC_RPC_OPERATION_ID_NONE || out_call == NULL || out_stream == "
        "NULL) { return -EINVAL; }\n"
        "    trevrpc_rpc_call_config_v1 config; int err = %s_call_config_init(&config, sizeof(config));\n"
        "    if (err != 0) { return err; }\n",
        base,
        method->input->c_type,
        base);
    string_builder_append(buffer,
        "    uint8_t stack_body[TREVRPC_GENERATED_STACK_BODY_LEN]; uint8_t* body = NULL; size_t body_len = 0;\n"
        "    if (initial_or_null != NULL) {\n");
    string_builder_appendf(buffer,
        "        body_len = %s__get_packed_size(initial_or_null);\n"
        "        body = body_len == 0 ? stack_body : (body_len <= sizeof(stack_body) ? stack_body : "
        "malloc(body_len));\n"
        "        if (body_len > 0 && body == NULL) { return -ENOMEM; }\n"
        "        %s__pack(initial_or_null, body); config.initial_message = body; config.initial_message_len = "
        "body_len;\n"
        "    }\n",
        method->input->c_prefix,
        method->input->c_prefix);
    string_builder_append(buffer,
        "    err = trevrpc_rpc_call_open_v1(runtime, endpoint, &config, operation_id, out_call, out_stream);\n"
        "    if (initial_or_null != NULL) { if (body != stack_body) { free(body); } }\n");
    string_builder_append(buffer, "    return err;\n}\n\n");
    free(base);
    free(service_name);
    free(method_name);
    free(kind);
}

static void generate_respond_function(string_builder* buffer, const service_info* service, const method_info* method) {
    if (method->server_streaming) {
        return;
    }
    char* base = method_base_name(service, method);
    if (base == NULL) {
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s_respond(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, const %s* message, "
        "const trevrpc_rpc_status_v1* status, uint64_t operation_id) {\n"
        "    if (runtime == NULL || status == NULL || operation_id == TREVRPC_RPC_OPERATION_ID_NONE) { return -EINVAL; "
        "}\n"
        "    uint8_t stack_body[TREVRPC_GENERATED_STACK_BODY_LEN]; uint8_t* body = NULL; size_t body_len = 0;\n"
        "    if (message != NULL) {\n",
        base,
        method->output->c_type);
    string_builder_appendf(buffer,
        "        body_len = %s__get_packed_size(message);\n"
        "        body = body_len == 0 ? stack_body : (body_len <= sizeof(stack_body) ? stack_body : "
        "malloc(body_len));\n"
        "        if (body_len > 0 && body == NULL) { return -ENOMEM; }\n"
        "        %s__pack(message, body);\n"
        "    }\n"
        "    int err = trevrpc_rpc_call_respond_copy_v1(runtime, call, operation_id, status, body, body_len);\n"
        "    if (body != stack_body) { free(body); }\n"
        "    return err;\n}\n\n",
        method->output->c_prefix,
        method->output->c_prefix);
    free(base);
}

static void generate_decode_receive_function(
    string_builder* buffer, const service_info* service, const method_info* method, bool request_side) {
    char* base = method_base_name(service, method);
    char* suffix =
        request_side ? duplicate_string("_decode_request_receive") : duplicate_string("_decode_response_receive");
    const type_ref* message = request_side ? method->input : method->output;
    const char* expected_kind_check = request_side && method->client_streaming
                                          ? "info.kind != TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE && info.kind != "
                                            "TREVRPC_RPC_RECEIVE_MESSAGE"
                                      : request_side ? "info.kind != TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE"
                                                     : "info.kind != TREVRPC_RPC_RECEIVE_MESSAGE";
    char* name = base == NULL || suffix == NULL ? NULL : format_string("%s%s", base, suffix);
    if (base == NULL || suffix == NULL || name == NULL) {
        free(base);
        free(suffix);
        free(name);
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s(const trevrpc_rpc_receive* receive, %s** out_message) {\n"
        "    if (receive == NULL || out_message == NULL) { return -EINVAL; }\n"
        "    trevrpc_rpc_receive_info_v1 info; int err = trevrpc_rpc_receive_info_v1_init(&info, sizeof(info));\n"
        "    if (err == 0) { err = trevrpc_rpc_receive_get_info_v1(receive, &info); }\n"
        "    if (err != 0) { return err; }\n"
        "    if (%s) { return -EPROTO; }\n"
        "    if (info.data_len > SIZE_MAX) { return -EOVERFLOW; }\n"
        "    trevrpc_generated_allocator_state allocator_state = {0};\n"
        "    ProtobufCAllocator allocator = trevrpc_generated_allocator(&allocator_state);\n"
        "    %s* message = %s__unpack(&allocator, (size_t)info.data_len, info.data);\n"
        "    if (message == NULL) { return allocator_state.failed ? -ENOMEM : -EPROTO; }\n"
        "    *out_message = message;\n"
        "    return 0;\n}\n\n",
        name,
        message->c_type,
        expected_kind_check,
        message->c_type,
        message->c_prefix);
    free(base);
    free(suffix);
    free(name);
}

static void generate_matches_function(string_builder* buffer, const service_info* service, const method_info* method) {
    char* base = method_base_name(service, method);
    char* service_name = c_quote(service->proto_name);
    char* method_name = c_quote(method->name);
    char* kind = method_rpc_kind(method);
    if (base == NULL || service_name == NULL || method_name == NULL || kind == NULL) {
        free(base);
        free(service_name);
        free(method_name);
        free(kind);
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s_matches_incoming(const trevrpc_rpc_event_info_v1* info) {\n"
        "    if (info == NULL || info->kind != TREVRPC_RPC_EVENT_CALL_INCOMING || info->rpc_kind != %s) { return 0; }\n"
        "    return info->service != NULL && info->method != NULL && info->service_len == sizeof(%s) - 1 && "
        "memcmp(info->service, %s, sizeof(%s) - 1) == 0 && info->method_len == sizeof(%s) - 1 && memcmp(info->method, "
        "%s, sizeof(%s) - 1) == 0;\n}\n\n",
        base,
        kind,
        service_name,
        service_name,
        service_name,
        method_name,
        method_name,
        method_name);
    free(base);
    free(service_name);
    free(method_name);
    free(kind);
}

static void generate_take_incoming_function(
    string_builder* buffer, const service_info* service, const method_info* method) {
    char* base = method_base_name(service, method);
    if (base == NULL) {
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s_take_incoming(trevrpc_rpc_event* event, trevrpc_rpc_call_v1* out_call, trevrpc_rpc_stream_v1* "
        "out_stream, trevrpc_rpc_receive** out_initial) {\n"
        "    if (event == NULL || out_call == NULL || out_stream == NULL || out_initial == NULL) { return -EINVAL; }\n"
        "    trevrpc_rpc_event_info_v1 info; int err = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));\n"
        "    if (err == 0) { err = trevrpc_rpc_event_get_info_v1(event, &info); }\n"
        "    if (err == 0 && !%s_matches_incoming(&info)) { err = -EPROTO; }\n"
        "    if (err != 0) { return err; }\n"
        "    trevrpc_rpc_call_v1 call = {0}; trevrpc_rpc_stream_v1 stream = {0}; trevrpc_rpc_receive* initial = NULL;\n"
        "    err = trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &initial);\n"
        "    if (err != 0) { return err; }\n"
        "    *out_call = call; *out_stream = stream; *out_initial = initial;\n"
        "    return 0;\n}\n\n",
        base,
        base);
    free(base);
}

static void generate_accept_function(string_builder* buffer, const service_info* service, const method_info* method) {
    char* base = method_base_name(service, method);
    if (base == NULL) {
        buffer->failed = true;
        return;
    }
    string_builder_appendf(buffer,
        "int %s_accept(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id) {\n"
        "    if (runtime == NULL || operation_id == TREVRPC_RPC_OPERATION_ID_NONE) { return -EINVAL; }\n"
        "    return trevrpc_rpc_call_accept(runtime, call, operation_id);\n}\n\n",
        base);
    free(base);
}

static void generate_source_service(string_builder* buffer, const service_info* service) {
    for (size_t i = 0; i < service->methods.len; i++) {
        const method_info* method = &service->methods.items[i];
        generate_config_and_open(buffer, service, method);
        if (method->client_streaming) {
            generate_send_function(buffer, service, method, method->input, method->server_streaming ? "_request" : "");
            generate_finish_send_function(buffer, service, method);
        }
        if (method->server_streaming) {
            generate_send_function(
                buffer, service, method, method->output, method->client_streaming ? "_response" : "");
            generate_finish_function(buffer, service, method);
        }
        generate_respond_function(buffer, service, method);
        generate_decode_receive_function(buffer, service, method, true);
        generate_decode_receive_function(buffer, service, method, false);
        generate_matches_function(buffer, service, method);
        generate_take_incoming_function(buffer, service, method);
        generate_accept_function(buffer, service, method);
    }
}

static char* generate_header(
    const file_descriptor_proto* file, const service_list* services, const plugin_options* options) {
    char* header_name = output_file_name(str_or_empty(file->name), options->header_suffix);
    char* guard = header_name == NULL ? NULL : header_guard(header_name);
    char* pb_header = protobuf_c_header(str_or_empty(file->name));
    char* runtime_include = c_quote(options->runtime_include);
    char* pb_include = c_quote(pb_header);
    char* test_prefix = generated_test_prefix(file);
    if (header_name == NULL || guard == NULL || pb_header == NULL || runtime_include == NULL || pb_include == NULL ||
        test_prefix == NULL) {
        free(header_name);
        free(guard);
        free(pb_header);
        free(runtime_include);
        free(pb_include);
        free(test_prefix);
        return NULL;
    }
    string_builder buffer;
    string_builder_init(&buffer);
    string_builder_append(&buffer, "// Code generated by protoc-gen-trevrpc-c. DO NOT EDIT.\n// clang-format off\n\n");
    string_builder_appendf(&buffer, "#ifndef %s\n#define %s\n\n", guard, guard);
    string_builder_append(&buffer, "#include <stddef.h>\n#include <stdint.h>\n#include <protobuf-c/protobuf-c.h>\n");
    string_builder_appendf(&buffer, "#include %s\n#include %s\n\n", runtime_include, pb_include);
    string_builder_append(&buffer,
        "#if TREVRPC_RPC_ABI_VERSION != 1u\n#error \"Generated TrevRPC C bindings require RPC ABI "
        "1\"\n#endif\n\n#ifdef __cplusplus\nextern \"C\" {\n#endif\n\n");
    string_builder_appendf(&buffer,
        "#ifdef TREVRPC_GENERATED_TESTING\nvoid %s_test_fail_allocation_after(size_t "
        "successful_allocations);\n#endif\n\n",
        test_prefix);
    for (size_t i = 0; i < services->len; i++)
        generate_header_service(&buffer, &services->items[i]);
    string_builder_append(&buffer, "#ifdef __cplusplus\n}\n#endif\n\n#endif\n");
    free(header_name);
    free(guard);
    free(pb_header);
    free(runtime_include);
    free(pb_include);
    free(test_prefix);
    return string_builder_steal(&buffer);
}

static char* generate_source(
    const file_descriptor_proto* file, const service_list* services, const plugin_options* options) {
    char* header_name = output_file_name(str_or_empty(file->name), options->header_suffix);
    char* header_base = header_name == NULL ? NULL : duplicate_string(path_base_ptr(header_name));
    char* header_include = c_quote(header_base);
    char* test_prefix = generated_test_prefix(file);
    if (header_name == NULL || header_base == NULL || header_include == NULL || test_prefix == NULL) {
        free(header_name);
        free(header_base);
        free(header_include);
        free(test_prefix);
        return NULL;
    }
    string_builder buffer;
    string_builder_init(&buffer);
    string_builder_append(&buffer,
        "// Code generated by protoc-gen-trevrpc-c. DO NOT EDIT.\n// clang-format off\n\n#include <errno.h>\n#include "
        "<stdint.h>\n#include <stdlib.h>\n#include <string.h>\n");
    string_builder_appendf(&buffer, "#include %s\n\n", header_include);
    string_builder_append(&buffer,
        "#define TREVRPC_GENERATED_STACK_BODY_LEN 512u\n\n#ifdef TREVRPC_GENERATED_TESTING\nstatic size_t "
        "trevrpc_generated_allocation_budget = SIZE_MAX;\n#endif\n\ntypedef struct trevrpc_generated_allocator_state { "
        "int failed; } trevrpc_generated_allocator_state;\n\nstatic void* trevrpc_generated_alloc(void* "
        "allocator_data, size_t size) {\n    trevrpc_generated_allocator_state* state = "
        "(trevrpc_generated_allocator_state*)allocator_data;\n#ifdef TREVRPC_GENERATED_TESTING\n    if (size != 0 && "
        "trevrpc_generated_allocation_budget == 0) { state->failed = 1; return NULL; }\n    if (size != 0 && "
        "trevrpc_generated_allocation_budget != SIZE_MAX) { trevrpc_generated_allocation_budget--; }\n#endif\n    "
        "void* value = malloc(size);\n    if (value == NULL && size != 0) { state->failed = 1; }\n    return "
        "value;\n}\n\nstatic void trevrpc_generated_free(void* allocator_data, void* value) { (void)allocator_data; "
        "free(value); }\n\nstatic ProtobufCAllocator trevrpc_generated_allocator(trevrpc_generated_allocator_state* "
        "state) {\n    return (ProtobufCAllocator){.alloc = trevrpc_generated_alloc, .free = trevrpc_generated_free, "
        ".allocator_data = state};\n}\n");
    string_builder_appendf(&buffer,
        "#ifdef TREVRPC_GENERATED_TESTING\nvoid %s_test_fail_allocation_after(size_t successful_allocations) { "
        "trevrpc_generated_allocation_budget = successful_allocations; }\n#endif\n\n",
        test_prefix);
    for (size_t i = 0; i < services->len; i++)
        generate_source_service(&buffer, &services->items[i]);
    while (buffer.len > 1 && buffer.data[buffer.len - 1] == '\n' && buffer.data[buffer.len - 2] == '\n')
        buffer.data[--buffer.len] = '\0';
    free(header_name);
    free(header_base);
    free(header_include);
    free(test_prefix);
    return string_builder_steal(&buffer);
}

static bool generate_file(const file_descriptor_proto* file,
    const type_index* index,
    const plugin_options* options,
    generated_identifier_list* identifiers,
    generated_file_list* files,
    char** error) {
    service_list services = {0};
    if (!describe_services(file, index, &services, error)) {
        service_list_free(&services);
        return false;
    }
    if (!validate_generated_identifiers(file, &services, options, identifiers, error)) {
        service_list_free(&services);
        return false;
    }
    char* header = generate_header(file, &services, options);
    char* source = generate_source(file, &services, options);
    char* header_name = output_file_name(str_or_empty(file->name), options->header_suffix);
    char* source_name = output_file_name(str_or_empty(file->name), options->source_suffix);
    service_list_free(&services);
    if (header == NULL || source == NULL || header_name == NULL || source_name == NULL) {
        free(header);
        free(source);
        free(header_name);
        free(source_name);
        return set_oom(error);
    }
    if (!generated_file_list_append(files, header_name, header)) {
        free(header);
        free(header_name);
        free(source);
        free(source_name);
        return set_oom(error);
    }
    if (!generated_file_list_append(files, source_name, source)) {
        free(source_name);
        free(source);
        return set_oom(error);
    }
    return true;
}

static void plugin_options_free(plugin_options* options) {
    free(options->header_suffix);
    free(options->source_suffix);
    free(options->runtime_include);
    options->header_suffix = NULL;
    options->source_suffix = NULL;
    options->runtime_include = NULL;
}
static bool default_plugin_options(plugin_options* options, char** error) {
    options->header_suffix = duplicate_string(".trevrpc.h");
    options->source_suffix = duplicate_string(".trevrpc.c");
    options->runtime_include = duplicate_string("trevrpc_rpc.h");
    if (options->header_suffix == NULL || options->source_suffix == NULL || options->runtime_include == NULL) {
        plugin_options_free(options);
        return set_oom(error);
    }
    return true;
}
static bool replace_option_value(char** target, const char* value, size_t value_len, char** error) {
    char* next = duplicate_range(value, value_len);
    if (next == NULL)
        return set_oom(error);
    free(*target);
    *target = next;
    return true;
}
static bool parse_options(const char* parameter, plugin_options* options, char** error) {
    if (!default_plugin_options(options, error))
        return false;
    parameter = str_or_empty(parameter);
    if (parameter[0] == '\0')
        return true;
    const char* start = parameter;
    for (const char* p = parameter;; p++) {
        if (*p == ',' || *p == '\0') {
            if (p > start) {
                const char* equals = memchr(start, '=', (size_t)(p - start));
                if (equals == NULL)
                    return set_error(
                        error, "invalid trevrpc-c option \"%.*s\"; expected key=value", (int)(p - start), start);
                size_t key_len = (size_t)(equals - start);
                const char* value = equals + 1;
                size_t value_len = (size_t)(p - value);
                char** target = NULL;
                if (key_len == strlen("header_suffix") && strncmp(start, "header_suffix", key_len) == 0)
                    target = &options->header_suffix;
                else if (key_len == strlen("source_suffix") && strncmp(start, "source_suffix", key_len) == 0)
                    target = &options->source_suffix;
                else if (key_len == strlen("runtime_include") && strncmp(start, "runtime_include", key_len) == 0)
                    target = &options->runtime_include;
                else
                    return set_error(error, "unknown trevrpc-c option \"%.*s\"", (int)key_len, start);
                if (!replace_option_value(target, value, value_len, error))
                    return false;
            }
            if (*p == '\0')
                break;
            start = p + 1;
        }
    }
    return true;
}
static bool file_should_generate(const code_generator_request* request, const char* file_name) {
    for (size_t i = 0; i < request->n_file_to_generate; i++)
        if (strcmp(str_or_empty(request->file_to_generate[i]), file_name) == 0)
            return true;
    return false;
}
static bool generate_response_files(const code_generator_request* request, generated_file_list* files, char** error) {
    plugin_options options = {0};
    if (!parse_options(request->parameter, &options, error)) {
        plugin_options_free(&options);
        return false;
    }
    type_index index = {0};
    if (!build_type_index(request, &index, error)) {
        type_index_free(&index);
        plugin_options_free(&options);
        return false;
    }
    generated_identifier_list identifiers = {0};
    for (size_t i = 0; i < request->n_proto_file; i++) {
        const file_descriptor_proto* file = request->proto_file[i];
        if (!file_should_generate(request, str_or_empty(file->name)) || file->n_service == 0)
            continue;
        if (!generate_file(file, &index, &options, &identifiers, files, error)) {
            generated_identifier_list_free(&identifiers);
            type_index_free(&index);
            plugin_options_free(&options);
            return false;
        }
    }
    generated_identifier_list_free(&identifiers);
    type_index_free(&index);
    plugin_options_free(&options);
    return true;
}
static bool write_response(FILE* output, const generated_file_list* files, const char* error) {
    code_generator_response response = GOOGLE__PROTOBUF__COMPILER__CODE_GENERATOR_RESPONSE__INIT;
    response.error = (char*)error;
    code_generator_response_file** file_ptrs = NULL;
    code_generator_response_file* file_values = NULL;
    if (error == NULL && files->len > 0) {
        file_ptrs = calloc(files->len, sizeof(*file_ptrs));
        file_values = calloc(files->len, sizeof(*file_values));
        if (file_ptrs == NULL || file_values == NULL) {
            free(file_ptrs);
            free(file_values);
            return false;
        }
        for (size_t i = 0; i < files->len; i++) {
            google__protobuf__compiler__code_generator_response__file__init(&file_values[i]);
            file_values[i].name = files->items[i].name;
            file_values[i].content = files->items[i].content;
            file_ptrs[i] = &file_values[i];
        }
        response.n_file = files->len;
        response.file = file_ptrs;
    }
    size_t packed_len = google__protobuf__compiler__code_generator_response__get_packed_size(&response);
    uint8_t* packed = malloc(packed_len == 0 ? 1 : packed_len);
    if (packed == NULL) {
        free(file_ptrs);
        free(file_values);
        return false;
    }
    google__protobuf__compiler__code_generator_response__pack(&response, packed);
    bool ok = write_all(output, packed, packed_len);
    free(packed);
    free(file_ptrs);
    free(file_values);
    return ok;
}
int main(void) {
    uint8_t* input = NULL;
    size_t input_len = 0;
    if (!read_all(stdin, &input, &input_len)) {
        fprintf(stderr, "protoc-gen-trevrpc-c: read stdin: %s\n", strerror(errno));
        return 1;
    }
    code_generator_request* request =
        google__protobuf__compiler__code_generator_request__unpack(NULL, input_len, input);
    free(input);
    if (request == NULL) {
        fprintf(stderr, "protoc-gen-trevrpc-c: decode CodeGeneratorRequest\n");
        return 1;
    }
    generated_file_list files = {0};
    char* error = NULL;
    generate_response_files(request, &files, &error);
    google__protobuf__compiler__code_generator_request__free_unpacked(request, NULL);
    bool ok = write_response(stdout, &files, error);
    generated_file_list_free(&files);
    free(error);
    if (!ok) {
        fprintf(stderr, "protoc-gen-trevrpc-c: write CodeGeneratorResponse\n");
        return 1;
    }
    return 0;
}
