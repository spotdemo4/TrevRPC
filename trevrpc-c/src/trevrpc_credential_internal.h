#ifndef TREVRPC_CREDENTIAL_INTERNAL_H
#define TREVRPC_CREDENTIAL_INTERNAL_H

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#if defined(TREVRPC_CREDENTIAL_TESTING) && defined(__GNUC__)
extern int trevrpc_credential_test_fail_cleanup(void) __attribute__((weak));
#endif

typedef struct trevrpc_credential_files {
    char directory[PATH_MAX];
    char cert_file[PATH_MAX];
    char key_file[PATH_MAX];
    char ca_cert_file[PATH_MAX];
    unsigned char directory_created;
    unsigned char cert_created;
    unsigned char key_created;
    unsigned char ca_cert_created;
} trevrpc_credential_files;

static inline void trevrpc_credential_secure_zero(void* value, size_t length) {
    volatile unsigned char* bytes = (volatile unsigned char*)value;
    while (length-- != 0)
        *bytes++ = 0;
}

static inline int trevrpc_credential_write_file(
    const char* directory, const char* name, const uint8_t* data, uint64_t length, char output[PATH_MAX]) {
    size_t written = 0;
    int fd;
    int result;

    if (length == 0)
        return 0;
    if (directory == NULL || name == NULL || data == NULL || length > SIZE_MAX)
        return -EINVAL;
#ifndef O_CLOEXEC
    return -ENOTSUP;
#else
    result = snprintf(output, PATH_MAX, "%s/%s", directory, name);
    if (result < 0 || (size_t)result >= PATH_MAX)
        return -ENAMETOOLONG;
    fd = open(output, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR);
    if (fd < 0)
        return -errno;
    while (written < (size_t)length) {
        ssize_t count = write(fd, data + written, (size_t)length - written);
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0) {
            int cleanup_result;
            result = count < 0 ? -errno : -EIO;
            close(fd);
            cleanup_result = unlink(output) == 0 || errno == ENOENT ? 0 : -errno;
            if (cleanup_result != 0)
                result = cleanup_result;
            output[0] = '\0';
            return result;
        }
        written += (size_t)count;
    }
    if (close(fd) != 0) {
        int cleanup_result;
        result = -errno;
        cleanup_result = unlink(output) == 0 || errno == ENOENT ? 0 : -errno;
        if (cleanup_result != 0)
            result = cleanup_result;
        output[0] = '\0';
        return result;
    }
    return 0;
#endif
}

static inline int trevrpc_credential_unlink(char path[PATH_MAX], unsigned char* created) {
    if (*created == 0)
        return 0;
    if (unlink(path) != 0 && errno != ENOENT)
        return -errno;
    *created = 0;
    path[0] = '\0';
    return 0;
}

static inline int trevrpc_credential_files_cleanup(trevrpc_credential_files* files) {
    int first_error = 0;
    int result;

    if (files == NULL)
        return -EINVAL;
    result = trevrpc_credential_unlink(files->cert_file, &files->cert_created);
    if (result != 0 && first_error == 0)
        first_error = result;
    result = trevrpc_credential_unlink(files->key_file, &files->key_created);
    if (result != 0 && first_error == 0)
        first_error = result;
    result = trevrpc_credential_unlink(files->ca_cert_file, &files->ca_cert_created);
    if (result != 0 && first_error == 0)
        first_error = result;
    if (files->directory_created && !files->cert_created && !files->key_created && !files->ca_cert_created) {
        if (rmdir(files->directory) != 0 && errno != ENOENT) {
            if (first_error == 0)
                first_error = -errno;
        } else {
            files->directory_created = 0;
            files->directory[0] = '\0';
        }
    }
    if (first_error == 0) {
        memset(files, 0, sizeof(*files));
#if defined(TREVRPC_CREDENTIAL_TESTING) && defined(__GNUC__)
        if (trevrpc_credential_test_fail_cleanup != NULL && trevrpc_credential_test_fail_cleanup() != 0)
            return -EIO;
#endif
    }
    return first_error;
}

static inline int trevrpc_credential_files_prepare(trevrpc_credential_files* files,
    const uint8_t* cert_data,
    uint64_t cert_data_len,
    const uint8_t* key_data,
    uint64_t key_data_len,
    const uint8_t* ca_cert_data,
    uint64_t ca_cert_data_len) {
    static const char directory_template[] = "/tmp/trevrpc-credentials-XXXXXX";
    int result;

    if (files == NULL)
        return -EINVAL;
    memset(files, 0, sizeof(*files));
    if (cert_data_len == 0 && key_data_len == 0 && ca_cert_data_len == 0)
        return 0;
    if ((cert_data == NULL && cert_data_len != 0) || (key_data == NULL && key_data_len != 0) ||
        (ca_cert_data == NULL && ca_cert_data_len != 0))
        return -EINVAL;
    if (sizeof(directory_template) > sizeof(files->directory))
        return -ENAMETOOLONG;
    memcpy(files->directory, directory_template, sizeof(directory_template));
    if (mkdtemp(files->directory) == NULL)
        return -errno;
    files->directory_created = 1;
    result =
        trevrpc_credential_write_file(files->directory, "certificate.pem", cert_data, cert_data_len, files->cert_file);
    if (result != 0)
        goto fail;
    files->cert_created = cert_data_len != 0;
    result =
        trevrpc_credential_write_file(files->directory, "private-key.pem", key_data, key_data_len, files->key_file);
    if (result != 0)
        goto fail;
    files->key_created = key_data_len != 0;
    result = trevrpc_credential_write_file(
        files->directory, "ca-certificate.pem", ca_cert_data, ca_cert_data_len, files->ca_cert_file);
    if (result != 0)
        goto fail;
    files->ca_cert_created = ca_cert_data_len != 0;
    return 0;
fail: {
    int cleanup_result = trevrpc_credential_files_cleanup(files);
    return cleanup_result != 0 ? cleanup_result : result;
}
}

static inline int trevrpc_credential_validate_pointers(const char* cert_file,
    uint32_t cert_file_len,
    const char* key_file,
    uint32_t key_file_len,
    const char* ca_cert_file,
    uint32_t ca_cert_file_len,
    const uint8_t* cert_data,
    uint64_t cert_data_len,
    const uint8_t* key_data,
    uint64_t key_data_len,
    const uint8_t* ca_cert_data,
    uint64_t ca_cert_data_len) {
    if ((cert_file == NULL && cert_file_len != 0) || (key_file == NULL && key_file_len != 0) ||
        (ca_cert_file == NULL && ca_cert_file_len != 0) || (cert_data == NULL && cert_data_len != 0) ||
        (key_data == NULL && key_data_len != 0) || (ca_cert_data == NULL && ca_cert_data_len != 0))
        return -EINVAL;
    if ((cert_file_len != 0 && cert_data_len != 0) || (key_file_len != 0 && key_data_len != 0) ||
        (ca_cert_file_len != 0 && ca_cert_data_len != 0))
        return -EINVAL;
    return 0;
}

static inline int trevrpc_credential_pair_present(const char* cert_file,
    uint32_t cert_file_len,
    const char* key_file,
    uint32_t key_file_len,
    const uint8_t* cert_data,
    uint64_t cert_data_len,
    const uint8_t* key_data,
    uint64_t key_data_len) {
    int cert_present = (cert_file != NULL && cert_file_len != 0) || (cert_data != NULL && cert_data_len != 0);
    int key_present = (key_file != NULL && key_file_len != 0) || (key_data != NULL && key_data_len != 0);
    return cert_present == key_present ? 0 : -EINVAL;
}

static inline int trevrpc_credential_validate_endpoint(const char* cert_file,
    uint32_t cert_file_len,
    const char* key_file,
    uint32_t key_file_len,
    const char* ca_cert_file,
    uint32_t ca_cert_file_len,
    const uint8_t* cert_data,
    uint64_t cert_data_len,
    const uint8_t* key_data,
    uint64_t key_data_len,
    const uint8_t* ca_cert_data,
    uint64_t ca_cert_data_len,
    int server) {
    int result = trevrpc_credential_validate_pointers(cert_file,
        cert_file_len,
        key_file,
        key_file_len,
        ca_cert_file,
        ca_cert_file_len,
        cert_data,
        cert_data_len,
        key_data,
        key_data_len,
        ca_cert_data,
        ca_cert_data_len);
    if (result != 0)
        return result;
    result = trevrpc_credential_pair_present(
        cert_file, cert_file_len, key_file, key_file_len, cert_data, cert_data_len, key_data, key_data_len);
    if (result != 0)
        return result;
    if (server && cert_file_len == 0 && cert_data_len == 0)
        return -EINVAL;
    if (server && (ca_cert_file_len != 0 || ca_cert_data_len != 0))
        return -ENOTSUP;
    return 0;
}

#endif
