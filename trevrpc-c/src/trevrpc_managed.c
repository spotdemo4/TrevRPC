#define _POSIX_C_SOURCE 200809L

#include "trevrpc_runtime_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TREVRPC_MANAGED_DEFAULT_INITIAL_BACKOFF_MS 100u
#define TREVRPC_MANAGED_DEFAULT_MAX_BACKOFF_MS 30000u
#define TREVRPC_MANAGED_DEFAULT_JITTER_PERCENT 20u
#define TREVRPC_MANAGED_NANOS_PER_MILLI 1000000ull
#define TREVRPC_MANAGED_CANCEL_POLL_NANOS (10ull * TREVRPC_MANAGED_NANOS_PER_MILLI)
#define TREVRPC_MANAGED_EVENT_QUEUE_CAPACITY 64u

typedef struct trevrpc_client_generation trevrpc_client_generation;

struct trevrpc_managed_client_options {
    uint64_t initial_backoff_ms;
    uint64_t max_backoff_ms;
    uint32_t jitter_percent;
    trevrpc_managed_client_lifecycle_callback lifecycle_callback;
    void* lifecycle_user_data;
};

struct trevrpc_client_generation {
    trevrpc_managed_client* owner;
    trevrpc_client* client;
    atomic_size_t refs;
    bool shutdown_complete;
};

struct trevrpc_managed_client {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_t worker;
    pthread_t dispatcher;
    char* host;
    trevrpc_config config;
    char* cert_file;
    char* key_file;
    char* ca_cert_file;
    uint8_t* resumption_ticket;
    size_t resumption_ticket_len;
    trevrpc_client_generation* active;
    uint64_t generation;
    uint64_t initial_backoff_ms;
    uint64_t max_backoff_ms;
    uint64_t random_state;
    uint32_t jitter_percent;
    uint32_t state;
    trevrpc_managed_client_event events[TREVRPC_MANAGED_EVENT_QUEUE_CAPACITY];
    size_t event_head;
    size_t event_count;
    size_t active_calls;
    trevrpc_managed_client_lifecycle_callback lifecycle_callback;
    void* lifecycle_user_data;
    uint16_t port;
    bool stop;
    bool worker_started;
    bool dispatcher_started;
    bool dispatcher_stop;
    bool releasing;
    bool has_connected;
};

static char* trevrpc_managed_copy_string(const char* value) {
    if (value == NULL) {
        return NULL;
    }
    size_t len = strlen(value);
    char* copy = malloc(len + 1);
    if (copy != NULL) {
        memcpy(copy, value, len + 1);
    }
    return copy;
}

static bool trevrpc_managed_copy_config(trevrpc_managed_client* client, const trevrpc_config* config) {
    client->config = config == NULL ? trevrpc_default_config() : *config;
    client->cert_file = trevrpc_managed_copy_string(client->config.cert_file);
    client->key_file = trevrpc_managed_copy_string(client->config.key_file);
    client->ca_cert_file = trevrpc_managed_copy_string(client->config.ca_cert_file);
    if ((client->config.cert_file != NULL && client->cert_file == NULL) ||
        (client->config.key_file != NULL && client->key_file == NULL) ||
        (client->config.ca_cert_file != NULL && client->ca_cert_file == NULL)) {
        return false;
    }
    client->config.cert_file = client->cert_file;
    client->config.key_file = client->key_file;
    client->config.ca_cert_file = client->ca_cert_file;
    return true;
}

static void trevrpc_managed_generation_retain(trevrpc_client_generation* generation) {
    (void)atomic_fetch_add_explicit(&generation->refs, 1, memory_order_relaxed);
}

static void trevrpc_managed_generation_release(void* context) {
    trevrpc_client_generation* generation = context;
    if (atomic_fetch_sub_explicit(&generation->refs, 1, memory_order_acq_rel) != 1) {
        return;
    }
    trevrpc_client_close(generation->client);
    free(generation);
}

static void trevrpc_managed_queue_event_locked(trevrpc_managed_client* client, uint32_t kind, int error_code) {
    if (client->lifecycle_callback == NULL) {
        return;
    }
    trevrpc_managed_client_event event = {
        .kind = kind,
        .state = client->state,
        .generation = client->generation,
        .error_code = error_code,
    };
    if (client->event_count == TREVRPC_MANAGED_EVENT_QUEUE_CAPACITY) {
        for (size_t offset = client->event_count; offset > 0; offset--) {
            size_t index = (client->event_head + offset - 1) % TREVRPC_MANAGED_EVENT_QUEUE_CAPACITY;
            if (client->events[index].kind == kind) {
                client->events[index] = event;
                return;
            }
        }
        return;
    }
    size_t tail = (client->event_head + client->event_count) % TREVRPC_MANAGED_EVENT_QUEUE_CAPACITY;
    client->events[tail] = event;
    client->event_count++;
    pthread_cond_broadcast(&client->cond);
}

static void trevrpc_managed_set_state_locked(trevrpc_managed_client* client, uint32_t state, int error_code) {
    if (client->state != state) {
        client->state = state;
        trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_STATE_CHANGED, error_code);
    }
}

static void* trevrpc_managed_dispatcher(void* context) {
    trevrpc_managed_client* client = context;
    for (;;) {
        pthread_mutex_lock(&client->mutex);
        while (client->event_count == 0 && !client->dispatcher_stop) {
            pthread_cond_wait(&client->cond, &client->mutex);
        }
        if (client->event_count == 0) {
            pthread_mutex_unlock(&client->mutex);
            break;
        }
        trevrpc_managed_client_event event = client->events[client->event_head];
        client->event_head = (client->event_head + 1) % TREVRPC_MANAGED_EVENT_QUEUE_CAPACITY;
        client->event_count--;
        trevrpc_managed_client_lifecycle_callback callback = client->lifecycle_callback;
        void* user_data = client->lifecycle_user_data;
        pthread_mutex_unlock(&client->mutex);
        if (callback != NULL) {
            callback(user_data, &event);
        }
    }
    return NULL;
}

static void trevrpc_managed_msquic_event(void* context, const trevrpc_msquic_conn_event* event) {
    trevrpc_client_generation* generation = context;
    trevrpc_managed_client* client = generation->owner;
    uint8_t* ticket = NULL;
    if (event->kind == TREV_MSQUIC_CONN_EVENT_RESUMPTION_TICKET_RECEIVED && event->resumption_ticket_len > 0 &&
        event->resumption_ticket != NULL) {
        ticket = malloc(event->resumption_ticket_len);
        if (ticket != NULL) {
            memcpy(ticket, event->resumption_ticket, event->resumption_ticket_len);
        }
    }

    pthread_mutex_lock(&client->mutex);
    if (!client->stop) {
        switch (event->kind) {
        case TREV_MSQUIC_CONN_EVENT_CONNECTED:
            if (event->session_resumed) {
                trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_TLS_SESSION_RESUMED, 0);
            }
            break;
        case TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE:
            generation->shutdown_complete = true;
            if (client->active == generation) {
                client->active = NULL;
                trevrpc_managed_set_state_locked(client, TREVRPC_MANAGED_CLIENT_RECONNECTING, event->error_code);
            }
            trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_CONNECTION_SHUTDOWN, event->error_code);
            pthread_cond_broadcast(&client->cond);
            break;
        case TREV_MSQUIC_CONN_EVENT_LOCAL_ADDRESS_CHANGED:
            trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_LOCAL_ADDRESS_CHANGED, 0);
            break;
        case TREV_MSQUIC_CONN_EVENT_PEER_ADDRESS_CHANGED:
            trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_PEER_ADDRESS_CHANGED, 0);
            break;
        case TREV_MSQUIC_CONN_EVENT_RESUMPTION_TICKET_RECEIVED:
            if (ticket != NULL) {
                free(client->resumption_ticket);
                client->resumption_ticket = ticket;
                client->resumption_ticket_len = event->resumption_ticket_len;
                ticket = NULL;
                trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_RESUMPTION_TICKET_RECEIVED, 0);
            } else if (event->resumption_ticket_len > 0) {
                trevrpc_managed_queue_event_locked(client,
                    TREVRPC_MANAGED_EVENT_RESUMPTION_TICKET_RETAIN_FAILED,
                    event->resumption_ticket == NULL ? -EINVAL : -ENOMEM);
            }
            break;
        default:
            break;
        }
    }
    pthread_mutex_unlock(&client->mutex);
    free(ticket);
}

static int trevrpc_managed_connect_cancelled(void* context) {
    trevrpc_managed_client* client = context;
    pthread_mutex_lock(&client->mutex);
    bool stop = client->stop;
    pthread_mutex_unlock(&client->mutex);
    return stop ? 1 : 0;
}

static uint64_t trevrpc_managed_random(trevrpc_managed_client* client) {
    uint64_t value = client->random_state;
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    client->random_state = value;
    return value;
}

static uint64_t trevrpc_managed_backoff_ms(trevrpc_managed_client* client, uint32_t attempt) {
    uint64_t delay = client->initial_backoff_ms;
    for (uint32_t i = 0; i < attempt && delay < client->max_backoff_ms; i++) {
        delay = delay > client->max_backoff_ms / 2 ? client->max_backoff_ms : delay * 2;
    }
    if (client->jitter_percent == 0 || delay == 0) {
        return delay;
    }
    uint64_t range = (delay / 100) * client->jitter_percent + (delay % 100) * client->jitter_percent / 100;
    if (range == 0) {
        return delay;
    }
    uint64_t span = range > (UINT64_MAX - 1) / 2 ? UINT64_MAX : range * 2 + 1;
    uint64_t offset = trevrpc_managed_random(client) % span;
    uint64_t jittered = offset < range ? delay - offset : delay + (offset - range);
    return jittered > client->max_backoff_ms ? client->max_backoff_ms : jittered;
}

static int trevrpc_managed_realtime_after(uint64_t nanos, struct timespec* deadline) {
    if (clock_gettime(CLOCK_REALTIME, deadline) != 0) {
        return -errno;
    }
    uint64_t seconds = nanos / 1000000000ull;
    uint64_t remaining = nanos % 1000000000ull;
    if (seconds > (uint64_t)INT64_MAX - (uint64_t)deadline->tv_sec) {
        deadline->tv_sec = INT64_MAX;
        deadline->tv_nsec = 999999999;
        return 0;
    }
    deadline->tv_sec += (time_t)seconds;
    deadline->tv_nsec += (long)remaining;
    if (deadline->tv_nsec >= 1000000000) {
        deadline->tv_sec++;
        deadline->tv_nsec -= 1000000000;
    }
    return 0;
}

static bool trevrpc_managed_wait_backoff(trevrpc_managed_client* client, uint32_t attempt) {
    uint64_t delay_ms = trevrpc_managed_backoff_ms(client, attempt);
    struct timespec deadline = {0};
    if (trevrpc_managed_realtime_after(delay_ms * TREVRPC_MANAGED_NANOS_PER_MILLI, &deadline) != 0) {
        return false;
    }
    pthread_mutex_lock(&client->mutex);
    while (!client->stop) {
        int err = pthread_cond_timedwait(&client->cond, &client->mutex, &deadline);
        if (err == ETIMEDOUT) {
            break;
        }
    }
    bool stop = client->stop;
    pthread_mutex_unlock(&client->mutex);
    return !stop;
}

static void* trevrpc_managed_worker(void* context) {
    trevrpc_managed_client* client = context;
    uint32_t attempt = 0;

    for (;;) {
        pthread_mutex_lock(&client->mutex);
        bool stop = client->stop;
        uint64_t generation_number = client->generation + 1;
        size_t ticket_len = client->resumption_ticket_len;
        uint8_t* ticket = NULL;
        if (ticket_len > 0) {
            ticket = malloc(ticket_len);
            if (ticket != NULL) {
                memcpy(ticket, client->resumption_ticket, ticket_len);
            } else {
                ticket_len = 0;
            }
        }
        pthread_mutex_unlock(&client->mutex);
        if (stop) {
            free(ticket);
            break;
        }

        trevrpc_client_generation* generation = calloc(1, sizeof(*generation));
        if (generation == NULL) {
            free(ticket);
            pthread_mutex_lock(&client->mutex);
            trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_CONNECT_FAILED, -ENOMEM);
            pthread_mutex_unlock(&client->mutex);
            if (!trevrpc_managed_wait_backoff(client, attempt++)) {
                break;
            }
            continue;
        }
        generation->owner = client;
        atomic_init(&generation->refs, 1);

        int err = trevrpc_client_connect_observed(client->host,
            client->port,
            &client->config,
            trevrpc_managed_connect_cancelled,
            client,
            ticket,
            ticket_len,
            trevrpc_managed_msquic_event,
            generation,
            &generation->client);
        free(ticket);
        if (err != 0) {
            free(generation);
            pthread_mutex_lock(&client->mutex);
            if (!client->stop) {
                trevrpc_managed_set_state_locked(client,
                    client->has_connected ? TREVRPC_MANAGED_CLIENT_RECONNECTING : TREVRPC_MANAGED_CLIENT_CONNECTING,
                    err);
                trevrpc_managed_queue_event_locked(client, TREVRPC_MANAGED_EVENT_CONNECT_FAILED, err);
            }
            stop = client->stop;
            pthread_mutex_unlock(&client->mutex);
            if (stop || !trevrpc_managed_wait_backoff(client, attempt++)) {
                break;
            }
            continue;
        }

        pthread_mutex_lock(&client->mutex);
        if (!client->stop && !generation->shutdown_complete) {
            client->active = generation;
            client->generation = generation_number;
            client->has_connected = true;
            attempt = 0;
            trevrpc_managed_set_state_locked(client, TREVRPC_MANAGED_CLIENT_READY, 0);
            pthread_cond_broadcast(&client->cond);
        } else if (!client->stop) {
            trevrpc_managed_set_state_locked(client,
                client->has_connected ? TREVRPC_MANAGED_CLIENT_RECONNECTING : TREVRPC_MANAGED_CLIENT_CONNECTING,
                TREV_MSQUIC_ERR_CLOSED);
        }
        stop = client->stop;
        pthread_mutex_unlock(&client->mutex);

        if (!stop) {
            pthread_mutex_lock(&client->mutex);
            while (!client->stop && client->active == generation) {
                pthread_cond_wait(&client->cond, &client->mutex);
            }
            stop = client->stop;
            if (client->active == generation) {
                client->active = NULL;
            }
            pthread_mutex_unlock(&client->mutex);
        }

        trevrpc_client_clear_observer(generation->client);
        if (stop) {
            trevrpc_client_shutdown(generation->client);
        }
        generation->owner = NULL;
        trevrpc_managed_generation_release(generation);
        if (stop || !trevrpc_managed_wait_backoff(client, attempt++)) {
            break;
        }
    }

    pthread_mutex_lock(&client->mutex);
    trevrpc_managed_set_state_locked(client, TREVRPC_MANAGED_CLIENT_CLOSED, 0);
    pthread_cond_broadcast(&client->cond);
    pthread_mutex_unlock(&client->mutex);
    return NULL;
}

trevrpc_managed_client_options* trevrpc_managed_client_options_new(void) {
    trevrpc_managed_client_options* options = calloc(1, sizeof(*options));
    if (options != NULL) {
        options->initial_backoff_ms = TREVRPC_MANAGED_DEFAULT_INITIAL_BACKOFF_MS;
        options->max_backoff_ms = TREVRPC_MANAGED_DEFAULT_MAX_BACKOFF_MS;
        options->jitter_percent = TREVRPC_MANAGED_DEFAULT_JITTER_PERCENT;
    }
    return options;
}

int trevrpc_managed_client_options_set_backoff(trevrpc_managed_client_options* options,
    uint64_t initial_backoff_ms,
    uint64_t max_backoff_ms,
    uint32_t jitter_percent) {
    if (options == NULL || initial_backoff_ms == 0 || max_backoff_ms < initial_backoff_ms || jitter_percent > 100 ||
        max_backoff_ms > UINT64_MAX / TREVRPC_MANAGED_NANOS_PER_MILLI) {
        return -EINVAL;
    }
    options->initial_backoff_ms = initial_backoff_ms;
    options->max_backoff_ms = max_backoff_ms;
    options->jitter_percent = jitter_percent;
    return 0;
}

int trevrpc_managed_client_options_set_lifecycle_callback(
    trevrpc_managed_client_options* options, trevrpc_managed_client_lifecycle_callback callback, void* user_data) {
    if (options == NULL) {
        return -EINVAL;
    }
    options->lifecycle_callback = callback;
    options->lifecycle_user_data = user_data;
    return 0;
}

void trevrpc_managed_client_options_free(trevrpc_managed_client_options* options) {
    free(options);
}

static void trevrpc_managed_free_storage(trevrpc_managed_client* client) {
    free(client->resumption_ticket);
    free(client->ca_cert_file);
    free(client->key_file);
    free(client->cert_file);
    free(client->host);
    pthread_cond_destroy(&client->cond);
    pthread_mutex_destroy(&client->mutex);
    free(client);
}

int trevrpc_managed_client_create(const char* host,
    uint16_t port,
    const trevrpc_config* config,
    const trevrpc_managed_client_options* options,
    trevrpc_managed_client** out_client) {
    if (host == NULL || out_client == NULL) {
        return -EINVAL;
    }
    *out_client = NULL;
    trevrpc_managed_client_options defaults = {
        .initial_backoff_ms = TREVRPC_MANAGED_DEFAULT_INITIAL_BACKOFF_MS,
        .max_backoff_ms = TREVRPC_MANAGED_DEFAULT_MAX_BACKOFF_MS,
        .jitter_percent = TREVRPC_MANAGED_DEFAULT_JITTER_PERCENT,
    };
    if (options == NULL) {
        options = &defaults;
    }
    if (options->initial_backoff_ms == 0 || options->max_backoff_ms < options->initial_backoff_ms ||
        options->jitter_percent > 100 || options->max_backoff_ms > UINT64_MAX / TREVRPC_MANAGED_NANOS_PER_MILLI) {
        return -EINVAL;
    }

    trevrpc_managed_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return -ENOMEM;
    }
    int err = pthread_mutex_init(&client->mutex, NULL);
    if (err != 0) {
        free(client);
        return -err;
    }
    err = pthread_cond_init(&client->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&client->mutex);
        free(client);
        return -err;
    }
    client->host = trevrpc_managed_copy_string(host);
    if (client->host == NULL || !trevrpc_managed_copy_config(client, config)) {
        trevrpc_managed_free_storage(client);
        return -ENOMEM;
    }
    client->port = port;
    client->state = TREVRPC_MANAGED_CLIENT_CONNECTING;
    client->initial_backoff_ms = options->initial_backoff_ms;
    client->max_backoff_ms = options->max_backoff_ms;
    client->jitter_percent = options->jitter_percent;
    client->lifecycle_callback = options->lifecycle_callback;
    client->lifecycle_user_data = options->lifecycle_user_data;
    struct timespec now = {0};
    (void)clock_gettime(CLOCK_MONOTONIC, &now);
    client->random_state = (uint64_t)now.tv_nsec ^ ((uint64_t)now.tv_sec << 32) ^ (uintptr_t)client;
    if (client->random_state == 0) {
        client->random_state = 1;
    }
    if (client->lifecycle_callback != NULL) {
        err = pthread_create(&client->dispatcher, NULL, trevrpc_managed_dispatcher, client);
        if (err != 0) {
            trevrpc_managed_free_storage(client);
            return -err;
        }
        client->dispatcher_started = true;
    }
    err = pthread_create(&client->worker, NULL, trevrpc_managed_worker, client);
    if (err != 0) {
        pthread_mutex_lock(&client->mutex);
        client->dispatcher_stop = true;
        pthread_cond_broadcast(&client->cond);
        pthread_mutex_unlock(&client->mutex);
        if (client->dispatcher_started) {
            (void)pthread_join(client->dispatcher, NULL);
        }
        trevrpc_managed_free_storage(client);
        return -err;
    }
    client->worker_started = true;
    *out_client = client;
    return 0;
}

static int trevrpc_managed_enter(trevrpc_managed_client* client) {
    if (client == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&client->mutex);
    if (client->releasing || client->active_calls == SIZE_MAX) {
        pthread_mutex_unlock(&client->mutex);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    client->active_calls++;
    pthread_mutex_unlock(&client->mutex);
    return 0;
}

static void trevrpc_managed_leave(trevrpc_managed_client* client) {
    pthread_mutex_lock(&client->mutex);
    client->active_calls--;
    if (client->active_calls == 0) {
        pthread_cond_broadcast(&client->cond);
    }
    pthread_mutex_unlock(&client->mutex);
}

int trevrpc_managed_client_get_state(trevrpc_managed_client* client, uint32_t* state, uint64_t* generation) {
    if (client == NULL || state == NULL || generation == NULL) {
        return -EINVAL;
    }
    int err = trevrpc_managed_enter(client);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&client->mutex);
    *state = client->state;
    *generation = client->generation;
    pthread_mutex_unlock(&client->mutex);
    trevrpc_managed_leave(client);
    return 0;
}

int trevrpc_managed_client_wait_ready(
    trevrpc_managed_client* client, uint64_t timeout_nanos, trevrpc_cancellation* cancellation, uint64_t* generation) {
    if (client == NULL) {
        return -EINVAL;
    }
    struct timespec deadline = {0};
    if (timeout_nanos > 0) {
        int err = trevrpc_managed_realtime_after(timeout_nanos, &deadline);
        if (err != 0) {
            return err;
        }
    }

    int result = trevrpc_managed_enter(client);
    if (result != 0) {
        return result;
    }
    pthread_mutex_lock(&client->mutex);
    for (;;) {
        if (client->state == TREVRPC_MANAGED_CLIENT_READY && client->active != NULL) {
            if (generation != NULL) {
                *generation = client->generation;
            }
            result = 0;
            break;
        }
        if (client->state == TREVRPC_MANAGED_CLIENT_CLOSED) {
            result = TREV_MSQUIC_ERR_CLOSED;
            break;
        }
        if (trevrpc_cancellation_cancelled(cancellation)) {
            result = -ECANCELED;
            break;
        }

        struct timespec wake = deadline;
        if (cancellation != NULL) {
            struct timespec poll = {0};
            int err = trevrpc_managed_realtime_after(TREVRPC_MANAGED_CANCEL_POLL_NANOS, &poll);
            if (err != 0) {
                result = err;
                break;
            }
            if (timeout_nanos == 0 || poll.tv_sec < wake.tv_sec ||
                (poll.tv_sec == wake.tv_sec && poll.tv_nsec < wake.tv_nsec)) {
                wake = poll;
            }
        }
        int err = timeout_nanos == 0 && cancellation == NULL
                      ? pthread_cond_wait(&client->cond, &client->mutex)
                      : pthread_cond_timedwait(&client->cond, &client->mutex, &wake);
        if (err == ETIMEDOUT && timeout_nanos > 0 && wake.tv_sec == deadline.tv_sec &&
            wake.tv_nsec == deadline.tv_nsec) {
            result = TREV_MSQUIC_ERR_TIMEOUT;
            break;
        }
        if (err != 0 && err != ETIMEDOUT) {
            result = -err;
            break;
        }
    }
    pthread_mutex_unlock(&client->mutex);
    trevrpc_managed_leave(client);
    return result;
}

static int trevrpc_managed_snapshot(trevrpc_managed_client* client, trevrpc_client_generation** out_generation) {
    if (client == NULL || out_generation == NULL) {
        return -EINVAL;
    }
    *out_generation = NULL;
    int err = trevrpc_managed_enter(client);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&client->mutex);
    trevrpc_client_generation* generation = client->active;
    if (client->state == TREVRPC_MANAGED_CLIENT_READY && generation != NULL) {
        trevrpc_managed_generation_retain(generation);
    } else {
        generation = NULL;
    }
    pthread_mutex_unlock(&client->mutex);
    if (generation == NULL) {
        trevrpc_managed_leave(client);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    *out_generation = generation;
    return 0;
}

#define TREVRPC_MANAGED_UNARY_WRAPPER(name, low_name, parameters, arguments)                                           \
    int name parameters {                                                                                              \
        trevrpc_client_generation* generation = NULL;                                                                  \
        int err = trevrpc_managed_snapshot(client, &generation);                                                       \
        if (err != 0) {                                                                                                \
            return err;                                                                                                \
        }                                                                                                              \
        err = low_name arguments;                                                                                      \
        trevrpc_managed_generation_release(generation);                                                                \
        trevrpc_managed_leave(client);                                                                                 \
        return err;                                                                                                    \
    }

TREVRPC_MANAGED_UNARY_WRAPPER(trevrpc_managed_client_call_unary,
    trevrpc_client_call_unary,
    (trevrpc_managed_client * client,
        const char* service,
        const char* method,
        const uint8_t* body,
        size_t body_len,
        trevrpc_response** response),
    (generation->client, service, method, body, body_len, response))

TREVRPC_MANAGED_UNARY_WRAPPER(trevrpc_managed_client_call_unary_with_options,
    trevrpc_client_call_unary_with_options,
    (trevrpc_managed_client * client,
        const char* service,
        const char* method,
        const uint8_t* body,
        size_t body_len,
        const trevrpc_call_options* options,
        trevrpc_response** response),
    (generation->client, service, method, body, body_len, options, response))

TREVRPC_MANAGED_UNARY_WRAPPER(trevrpc_managed_client_call_request,
    trevrpc_client_call_request,
    (trevrpc_managed_client * client, const trevrpc_request* request, trevrpc_response** response),
    (generation->client, request, response))

TREVRPC_MANAGED_UNARY_WRAPPER(trevrpc_managed_client_call_request_cancellable,
    trevrpc_client_call_request_cancellable,
    (trevrpc_managed_client * client,
        const trevrpc_request* request,
        trevrpc_cancellation* cancellation,
        trevrpc_response** response),
    (generation->client, request, cancellation, response))

TREVRPC_MANAGED_UNARY_WRAPPER(trevrpc_managed_client_call_request_borrowed_cancellable,
    trevrpc_client_call_request_borrowed_cancellable,
    (trevrpc_managed_client * client,
        const trevrpc_request* request,
        trevrpc_cancellation* cancellation,
        trevrpc_response** response),
    (generation->client, request, cancellation, response))

TREVRPC_MANAGED_UNARY_WRAPPER(trevrpc_managed_client_call_request_with_options,
    trevrpc_client_call_request_with_options,
    (trevrpc_managed_client * client,
        const trevrpc_request* request,
        const trevrpc_call_options* options,
        trevrpc_response** response),
    (generation->client, request, options, response))

#define TREVRPC_MANAGED_STREAM_WRAPPER(name, low_name, parameters, arguments)                                          \
    int name parameters {                                                                                              \
        trevrpc_client_generation* generation = NULL;                                                                  \
        int err = trevrpc_managed_snapshot(client, &generation);                                                       \
        if (err != 0) {                                                                                                \
            return err;                                                                                                \
        }                                                                                                              \
        err = low_name arguments;                                                                                      \
        if (err == 0) {                                                                                                \
            trevrpc_stream_set_release(*stream, trevrpc_managed_generation_release, generation);                       \
        } else {                                                                                                       \
            trevrpc_managed_generation_release(generation);                                                            \
        }                                                                                                              \
        trevrpc_managed_leave(client);                                                                                 \
        return err;                                                                                                    \
    }

TREVRPC_MANAGED_STREAM_WRAPPER(trevrpc_managed_client_start_stream,
    trevrpc_client_start_stream,
    (trevrpc_managed_client * client,
        const char* service,
        const char* method,
        uint32_t kind,
        const uint8_t* body,
        size_t body_len,
        trevrpc_stream** stream),
    (generation->client, service, method, kind, body, body_len, stream))

TREVRPC_MANAGED_STREAM_WRAPPER(trevrpc_managed_client_start_stream_with_options,
    trevrpc_client_start_stream_with_options,
    (trevrpc_managed_client * client,
        const char* service,
        const char* method,
        uint32_t kind,
        const uint8_t* body,
        size_t body_len,
        const trevrpc_call_options* options,
        trevrpc_stream** stream),
    (generation->client, service, method, kind, body, body_len, options, stream))

TREVRPC_MANAGED_STREAM_WRAPPER(trevrpc_managed_client_start_stream_request,
    trevrpc_client_start_stream_request,
    (trevrpc_managed_client * client, const trevrpc_request* request, trevrpc_stream** stream),
    (generation->client, request, stream))

TREVRPC_MANAGED_STREAM_WRAPPER(trevrpc_managed_client_start_stream_request_cancellable,
    trevrpc_client_start_stream_request_cancellable,
    (trevrpc_managed_client * client,
        const trevrpc_request* request,
        trevrpc_cancellation* cancellation,
        trevrpc_stream** stream),
    (generation->client, request, cancellation, stream))

TREVRPC_MANAGED_STREAM_WRAPPER(trevrpc_managed_client_start_stream_request_borrowed_cancellable,
    trevrpc_client_start_stream_request_borrowed_cancellable,
    (trevrpc_managed_client * client,
        const trevrpc_request* request,
        trevrpc_cancellation* cancellation,
        trevrpc_stream** stream),
    (generation->client, request, cancellation, stream))

TREVRPC_MANAGED_STREAM_WRAPPER(trevrpc_managed_client_start_stream_request_with_options,
    trevrpc_client_start_stream_request_with_options,
    (trevrpc_managed_client * client,
        const trevrpc_request* request,
        const trevrpc_call_options* options,
        trevrpc_stream** stream),
    (generation->client, request, options, stream))

void trevrpc_managed_client_close(trevrpc_managed_client* client) {
    if (client == NULL) {
        return;
    }
    pthread_mutex_lock(&client->mutex);
    client->stop = true;
    trevrpc_managed_set_state_locked(client, TREVRPC_MANAGED_CLIENT_CLOSED, 0);
    pthread_cond_broadcast(&client->cond);
    pthread_mutex_unlock(&client->mutex);
}

void trevrpc_managed_client_release(trevrpc_managed_client* client) {
    if (client == NULL) {
        return;
    }
    trevrpc_managed_client_close(client);
    if (client->worker_started) {
        (void)pthread_join(client->worker, NULL);
    }
    pthread_mutex_lock(&client->mutex);
    client->dispatcher_stop = true;
    pthread_cond_broadcast(&client->cond);
    pthread_mutex_unlock(&client->mutex);
    if (client->dispatcher_started) {
        (void)pthread_join(client->dispatcher, NULL);
    }
    pthread_mutex_lock(&client->mutex);
    client->releasing = true;
    while (client->active_calls > 0) {
        pthread_cond_wait(&client->cond, &client->mutex);
    }
    pthread_mutex_unlock(&client->mutex);
    trevrpc_managed_free_storage(client);
}
