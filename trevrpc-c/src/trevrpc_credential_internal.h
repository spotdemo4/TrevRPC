#ifndef TREVRPC_CREDENTIAL_INTERNAL_H
#define TREVRPC_CREDENTIAL_INTERNAL_H

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
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

#if defined(TREVRPC_CREDENTIAL_TESTING) && defined(__GNUC__)
extern int trevrpc_credential_test_fail_cleanup(void) __attribute__((weak));
extern void trevrpc_credential_test_before_cleanup(trevrpc_credential_files*) __attribute__((weak));
#endif

typedef struct trevrpc_credential_cleanup_lease {
    trevrpc_credential_files files;
    struct trevrpc_credential_cleanup_lease* next;
} trevrpc_credential_cleanup_lease;

typedef struct trevrpc_credential_cleanup_owner {
    trevrpc_credential_cleanup_lease* head;
    trevrpc_credential_cleanup_lease* tail;
    size_t in_flight;
    _Atomic size_t pending;
    pthread_mutex_t mutex;
} trevrpc_credential_cleanup_owner;

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
    if (fd < 0) {
        result = -errno;
        output[0] = '\0';
        return result;
    }
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
            else
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
        else
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
#if defined(TREVRPC_CREDENTIAL_TESTING) && defined(__GNUC__)
    if (trevrpc_credential_test_before_cleanup != NULL)
        trevrpc_credential_test_before_cleanup(files);
#endif
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

static inline int trevrpc_credential_files_have_pending_cleanup(const trevrpc_credential_files* files) {
    return files != NULL &&
           (files->directory_created || files->cert_created || files->key_created || files->ca_cert_created);
}

static inline int trevrpc_credential_cleanup_required(
    uint64_t cert_data_len, uint64_t key_data_len, uint64_t ca_cert_data_len) {
    return cert_data_len != 0 || key_data_len != 0 || ca_cert_data_len != 0;
}

static inline int trevrpc_credential_cleanup_lease_prepare(int required, trevrpc_credential_cleanup_lease** out_lease) {
    trevrpc_credential_cleanup_lease* lease;
    if (out_lease == NULL)
        return -EINVAL;
    *out_lease = NULL;
    if (!required)
        return 0;
    lease = calloc(1, sizeof(*lease));
    if (lease == NULL)
        return -ENOMEM;
    *out_lease = lease;
    return 0;
}

static inline int trevrpc_credential_cleanup_owner_init(trevrpc_credential_cleanup_owner* owner) {
    if (owner == NULL)
        return -EINVAL;
    memset(owner, 0, sizeof(*owner));
    atomic_init(&owner->pending, 0);
    return pthread_mutex_init(&owner->mutex, NULL) == 0 ? 0 : -ENOMEM;
}

static inline int trevrpc_credential_cleanup_owner_progress_locked(trevrpc_credential_cleanup_owner* owner);

static inline int trevrpc_credential_cleanup_owner_prepare_lease(
    trevrpc_credential_cleanup_owner* owner, int required, trevrpc_credential_cleanup_lease** out_lease) {
    int result;
    if (owner == NULL)
        return -EINVAL;
    result = trevrpc_credential_cleanup_lease_prepare(required, out_lease);
    if (result != 0 || !required)
        return result;
    pthread_mutex_lock(&owner->mutex);
    result = trevrpc_credential_cleanup_owner_progress_locked(owner);
    if (owner->head != NULL && result == 0)
        result = -EIO;
    if (owner->head == NULL && result != 0)
        result = 0;
    if (result == 0)
        ++owner->in_flight;
    pthread_mutex_unlock(&owner->mutex);
    if (result != 0) {
        free(*out_lease);
        *out_lease = NULL;
    }
    return result;
}

static inline void trevrpc_credential_cleanup_owner_adopt_locked(
    trevrpc_credential_cleanup_owner* owner, trevrpc_credential_cleanup_lease* lease, trevrpc_credential_files* files) {
    if (owner == NULL || lease == NULL || files == NULL || !trevrpc_credential_files_have_pending_cleanup(files))
        abort();
    lease->files = *files;
    memset(files, 0, sizeof(*files));
    lease->next = NULL;
    if (owner->tail != NULL)
        owner->tail->next = lease;
    else
        owner->head = lease;
    owner->tail = lease;
    atomic_fetch_add_explicit(&owner->pending, 1u, memory_order_release);
}

static inline void trevrpc_credential_cleanup_owner_finish(trevrpc_credential_cleanup_owner* owner,
    trevrpc_credential_files* files,
    trevrpc_credential_cleanup_lease** lease) {
    trevrpc_credential_cleanup_lease* completed_lease;
    if (owner == NULL || files == NULL || lease == NULL)
        abort();
    (void)trevrpc_credential_files_cleanup(files);
    completed_lease = *lease;
    if (completed_lease == NULL) {
        if (trevrpc_credential_files_have_pending_cleanup(files))
            abort();
        return;
    }
    pthread_mutex_lock(&owner->mutex);
    if (owner->in_flight == 0)
        abort();
    --owner->in_flight;
    if (trevrpc_credential_files_have_pending_cleanup(files)) {
        trevrpc_credential_cleanup_owner_adopt_locked(owner, completed_lease, files);
        completed_lease = NULL;
    }
    pthread_mutex_unlock(&owner->mutex);
    free(completed_lease);
    *lease = NULL;
}

static inline int trevrpc_credential_cleanup_owner_progress_locked(trevrpc_credential_cleanup_owner* owner) {
    trevrpc_credential_cleanup_lease** link = &owner->head;
    trevrpc_credential_cleanup_lease* tail = NULL;
    int first_error = 0;
    while (*link != NULL) {
        trevrpc_credential_cleanup_lease* lease = *link;
        int result = trevrpc_credential_files_cleanup(&lease->files);
        if (result != 0 && first_error == 0)
            first_error = result;
        if (!trevrpc_credential_files_have_pending_cleanup(&lease->files)) {
            *link = lease->next;
            atomic_fetch_sub_explicit(&owner->pending, 1u, memory_order_release);
            free(lease);
            continue;
        }
        tail = lease;
        link = &lease->next;
    }
    owner->tail = tail;
    return first_error;
}

static inline void trevrpc_credential_cleanup_owner_progress(trevrpc_credential_cleanup_owner* owner) {
    if (owner == NULL || atomic_load_explicit(&owner->pending, memory_order_acquire) == 0)
        return;
    pthread_mutex_lock(&owner->mutex);
    (void)trevrpc_credential_cleanup_owner_progress_locked(owner);
    pthread_mutex_unlock(&owner->mutex);
}

static inline int trevrpc_credential_cleanup_owner_prepare_release(trevrpc_credential_cleanup_owner* owner) {
    int result;
    if (owner == NULL)
        return -EINVAL;
    pthread_mutex_lock(&owner->mutex);
    result = trevrpc_credential_cleanup_owner_progress_locked(owner);
    if (owner->in_flight != 0)
        result = -EBUSY;
    else if (owner->head != NULL && result == 0)
        result = -EIO;
    pthread_mutex_unlock(&owner->mutex);
    return result;
}

static inline void trevrpc_credential_cleanup_owner_destroy(trevrpc_credential_cleanup_owner* owner) {
    if (owner == NULL)
        return;
    if (trevrpc_credential_cleanup_owner_prepare_release(owner) != 0)
        abort();
    pthread_mutex_destroy(&owner->mutex);
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
    files->cert_created = files->cert_file[0] != '\0';
    if (result != 0)
        goto fail;
    result =
        trevrpc_credential_write_file(files->directory, "private-key.pem", key_data, key_data_len, files->key_file);
    files->key_created = files->key_file[0] != '\0';
    if (result != 0)
        goto fail;
    result = trevrpc_credential_write_file(
        files->directory, "ca-certificate.pem", ca_cert_data, ca_cert_data_len, files->ca_cert_file);
    files->ca_cert_created = files->ca_cert_file[0] != '\0';
    if (result != 0)
        goto fail;
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
