#ifndef RDMA_INFRASTRUCTURE_H
#define RDMA_INFRASTRUCTURE_H

#include <argp.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>

#include <endian.h>
#include <infiniband/verbs.h>
#include <rdma_track.hpp>

    struct rdma_thread_param {
        struct rdma_context *rdma_ctx;
        unsigned ctx_index;

        struct rdma_endpoint *remote_endpoint;

        unsigned long message_count;
        unsigned long message_size;
        unsigned long buffer_size;
        unsigned long mem_offset;
        unsigned long used_size;
        unsigned long used_size_timed;
        unsigned long received_size;

        int control_socket;
        unsigned int backpressure;
        unsigned int backpressure_threshold_up;
        unsigned int backpressure_threshold_down;

        long int start_ts;

        sem_t *sem_recv_data;
        pthread_mutex_t *backpressure_mutex;

        int client_id;
        int stream;
    };

    /*struct linked_list_node {
        unsigned int sent_size;
        struct linked_list_node *next;
    };*/

    struct rdma_notification_thread_param {
        int sender_thread_socket;
        unsigned int sent_size;
        struct linked_list_node *first, *last;
        sem_t *sem_send_data;
        pthread_mutex_t *notification_mutex;
        int client_id;
    };

    struct rdma_backpressure_thread_param {
        int sender_thread_socket;
        unsigned int backpressure;
        long int start_ts;
        int client_id;
    };

    void *
    sender_notification_thread(void *arg)
    {
        char buf[32];
        struct rdma_notification_thread_param *notification_thread_args = (struct rdma_notification_thread_param *)arg;

        while(1) {
            sem_wait(notification_thread_args->sem_send_data);
            bzero(buf, 32);
            sprintf(buf, "%u", notification_thread_args->sent_size);
            write(notification_thread_args->sender_thread_socket, buf, 32);
        }
    }

    void *
    sender_control_thread(void *arg)
    {
        char buf[32];
        long int timestamp_ns;
        char *buffer_token;

        struct rdma_backpressure_thread_param *backpressure_thread_args = (struct rdma_backpressure_thread_param *)arg;

        while(1) {
            bzero(buf, 32);
            read(backpressure_thread_args->sender_thread_socket, buf, 32);

            buffer_token = strtok(buf, ":");
            timestamp_ns = (rdma_utils::get_current_timestamp_ns() - backpressure_thread_args->start_ts) / 1E6;

            // debug_print("(sender_control_thread) received: >>>%s<<<\n", buf);
            if (strcmp(buffer_token, "WAIT") == 0) {
                backpressure_thread_args->backpressure = 1;
                buffer_token = strtok(NULL, ":");
                printf("b1:%d:%ld:%d:%s\n", backpressure_thread_args->client_id, timestamp_ns, 0, buffer_token);
            } else if (strcmp(buffer_token, "GO") == 0) {
                backpressure_thread_args->backpressure = 0;
                buffer_token = strtok(NULL, ":");
                printf("b1:%d:%ld:%d:%s\n", backpressure_thread_args->client_id, timestamp_ns, 1, buffer_token);
            }
        }
    }

    /*
    void *
    client_thread(void *arg)
    {
        struct ibv_send_wr *wr, **bad_wr;
        struct ibv_sge *list;

        int i, j, l, total, full_queue_count, remainder_queue_size, num_cq_events, loop_num_cq_events, done, last, left, ne;
        struct ibv_wc wc[RDMA_MAX_SEND_WR];

        char buf[32];
        long int timestamp_ns;
        unsigned long chunk_size;

        pthread_t *notification_thread = (pthread_t *)malloc(sizeof(pthread_t));
        pthread_t *backpressure_thread = (pthread_t *)malloc(sizeof(pthread_t));

        struct rdma_thread_param *thread_args = (struct rdma_thread_param *)arg;
        struct rdma_notification_thread_param *notification_thread_args = (struct rdma_notification_thread_param *)malloc(sizeof(struct rdma_notification_thread_param));
        struct rdma_backpressure_thread_param *backpressure_thread_args = (struct rdma_backpressure_thread_param *)malloc(sizeof(struct rdma_backpressure_thread_param));

        long int timestamp_ns_thread_cpu_start, timestamp_ns_thread_cpu_now;


        full_queue_count = thread_args->message_count / RDMA_MAX_SEND_WR;
        remainder_queue_size = thread_args->message_count % RDMA_MAX_SEND_WR;
        chunk_size = thread_args->message_count * thread_args->message_size;


        if (thread_args->stream) {
            notification_thread_args->sender_thread_socket = thread_args->control_socket;
            notification_thread_args->sent_size = chunk_size;
            notification_thread_args->first = NULL;
            notification_thread_args->last = NULL;
            notification_thread_args->sem_send_data = (sem_t *)malloc(sizeof(sem_t));
            notification_thread_args->notification_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
            notification_thread_args->client_id = thread_args->client_id;

            sem_init(notification_thread_args->sem_send_data, 0, 0);
            pthread_mutex_init(notification_thread_args->notification_mutex, NULL);

            backpressure_thread_args->sender_thread_socket = thread_args->control_socket;
            backpressure_thread_args->backpressure = 0;
            backpressure_thread_args->start_ts = thread_args->start_ts;
            backpressure_thread_args->client_id = thread_args->client_id;

            if (pthread_create(notification_thread, NULL, sender_notification_thread, notification_thread_args) != 0) {
                debug_print("(RDMA_SEND_MT_STREAM) pthread_create() error - notification_threadm, client #%d\n", thread_args->client_id);
            }

            if (pthread_create(backpressure_thread, NULL, sender_control_thread, backpressure_thread_args) != 0) {
                debug_print("(RDMA_SEND_MT_STREAM) pthread_create() error - backpressure_thread, client #%d\n", thread_args->client_id);
            }
        }

        while (1) {
            timestamp_ns = (rdma_utils::get_current_timestamp_ns() - thread_args->start_ts) / 1E6;

            if (!backpressure_thread_args->backpressure) {
                timestamp_ns_thread_cpu_start = rdma_utils::get_current_timestamp_ns_thread_cpu();

                total = 0;
                num_cq_events = 0;
                for (j = 0; j < full_queue_count; j++) {
                    wr = (struct ibv_send_wr *)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_send_wr));
                    bad_wr = (struct ibv_send_wr **)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_send_wr *));
                    list = (struct ibv_sge *)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_sge));

                    done = 0;
                    last = 0;
                    while (!done) {
                        for (i = last; i < RDMA_MAX_SEND_WR; i++) {
                            *(bad_wr + i) = NULL;
                            memset(wr + i, 0, sizeof(struct ibv_send_wr));
                            memset(list + i, 0, sizeof(struct ibv_send_wr *));

                            (wr + i)->wr_id = (j * RDMA_MAX_SEND_WR + i);
                            (wr + i)->next = NULL;
                            if (i > 0) {
                                (wr + i - 1)->next = wr + i;
                            }
                            (wr + i)->opcode = IBV_WR_RDMA_WRITE;
                            (wr + i)->sg_list = list + i;
                            (wr + i)->num_sge = 1;

                            (wr + i)->send_flags = thread_args->rdma_ctx->send_flags;
                            (wr + i)->wr.rdma.remote_addr = thread_args->remote_endpoint->addr + thread_args->mem_offset + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size;
                            (wr + i)->wr.rdma.rkey = thread_args->remote_endpoint->rkey;

                            (list + i)->length = thread_args->message_size;
                            (list + i)->addr = (uint64_t)(*(thread_args->rdma_ctx->buf + thread_args->ctx_index) + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                            (list + i)->lkey = (*(thread_args->rdma_ctx->mr + thread_args->ctx_index))->lkey;
                        }
                        if (ibv_post_send(*(thread_args->rdma_ctx->qp + thread_args->ctx_index), wr, bad_wr)) {
                            fprintf(stderr, "rdma_post_send_mt: Couldn't post sends, client #%d\n", thread_args->client_id);
                        }
                        done = 1;

                        left = RDMA_MAX_SEND_WR;
                        do {
                            ne = ibv_poll_cq(*(thread_args->rdma_ctx->cq + thread_args->ctx_index), left, wc);
                            if (ne < 0) {
                                debug_print("(client %d, loop %d) poll CQ failed %d, client #%d\n", thread_args->ctx_index + 1, j, ne, thread_args->client_id);
                            } else {
                                left -= ne;
                                // debug_print("(client %d, loop %d) ne=%d, left=%d\n", thread_args->ctx_index + 1, j, ne, left);
                            }
                        } while (left > 0);
                        // debug_print("(client %d, loop %d) all ACK\n", thread_args->ctx_index + 1, j);
                    }

                    free(wr);
                    free(bad_wr);
                    free(list);
                }

                wr = (struct ibv_send_wr *)malloc(remainder_queue_size * sizeof(struct ibv_send_wr));
                bad_wr = (struct ibv_send_wr **)malloc(remainder_queue_size * sizeof(struct ibv_send_wr *));
                list = (struct ibv_sge *)malloc(remainder_queue_size * sizeof(struct ibv_sge));

                done = 0;
                last = 0;
                while (!done) {
                    for (i = 0; i < remainder_queue_size; i++) {
                        *(bad_wr + i) = NULL;
                        memset(wr + i, 0, sizeof(struct ibv_send_wr));
                        memset(list + i, 0, sizeof(struct ibv_send_wr *));

                        (wr + i)->wr_id = (j * RDMA_MAX_SEND_WR + i);
                        (wr + i)->next = NULL;
                        if (i > 0) {
                            (wr + i - 1)->next = wr + i;
                        }
                        (wr + i)->opcode = IBV_WR_RDMA_WRITE;
                        (wr + i)->sg_list = list + i;
                        (wr + i)->num_sge = 1;

                        (wr + i)->send_flags = thread_args->rdma_ctx->send_flags;
                        (wr + i)->wr.rdma.remote_addr = thread_args->remote_endpoint->addr + thread_args->mem_offset + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size;
                        (wr + i)->wr.rdma.rkey = thread_args->remote_endpoint->rkey;

                        (list + i)->length = thread_args->message_size;
                        (list + i)->addr = (uint64_t)(*(thread_args->rdma_ctx->buf + thread_args->ctx_index) + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                        (list + i)->lkey = (*(thread_args->rdma_ctx->mr + thread_args->ctx_index))->lkey;
                    }
                    if (ibv_post_send(*(thread_args->rdma_ctx->qp + thread_args->ctx_index), wr, bad_wr)) {
                        fprintf(stderr, "rdma_post_send_mt: Couldn't post sends, client #%d\n", thread_args->client_id);
                    }
                    done = 1;


                    left = remainder_queue_size;
                    do {
                        // sleep(1);
                        ne = ibv_poll_cq(*(thread_args->rdma_ctx->cq + thread_args->ctx_index), left, wc);
                        if (ne < 0) {
                            debug_print("(client %d, final loop) poll CQ failed %d, client #%d\n", thread_args->ctx_index + 1, ne, thread_args->client_id);
                        } else {
                            left -= ne;
                        }
                    } while (left > 0);
                }

                free(wr);
                free(bad_wr);
                free(list);

                timestamp_ns_thread_cpu_now = rdma_utils::get_current_timestamp_ns_thread_cpu();

                debug_print("t1:%d:%ld:%lu\n", thread_args->client_id, timestamp_ns_thread_cpu_now - timestamp_ns_thread_cpu_start, chunk_size);

                if (!thread_args->stream) {
                    break;
                }

                thread_args->mem_offset += chunk_size;
                thread_args->mem_offset %= thread_args->buffer_size;

                if (thread_args->stream) {
                    sem_post(notification_thread_args->sem_send_data);
                }

            } else {
                // debug_print("(RDMA_SEND_MT_STREAM) backpressure\n");
                printf("s1:%d:%ld\n", thread_args->client_id, timestamp_ns);
                usleep(1000);
            }
        }

        if (thread_args->stream) {
            if (pthread_join(*backpressure_thread, NULL) != 0) {
                debug_print("(RDMA_SEND_MT_STREAM) pthread_join() error - backpressure_thread, client #%d\n", thread_args->client_id);
            }

            if (pthread_join(*notification_thread, NULL) != 0) {
                debug_print("(RDMA_SEND_MT_STREAM) pthread_join() error - notification_thread, client #%d\n", thread_args->client_id);
            }

            free(backpressure_thread_args);
            free(notification_thread_args);
            free(notification_thread_args->sem_send_data);
            free(notification_thread_args->notification_mutex);
        }

        return NULL;
    }

    int
    rdma_post_send_mt(struct rdma_context *ctx, struct rdma_endpoint **remote_endpoint, unsigned long *message_count, unsigned long *message_size, unsigned long *mem_offset, unsigned count)
    {
        int i;
        pthread_t *client_thread_list;
        struct rdma_thread_param *thread_arg_list;

        client_thread_list = (pthread_t *)calloc(count, sizeof(pthread_t));
        thread_arg_list = (struct rdma_thread_param *)calloc(count, sizeof(struct rdma_thread_param));

        for (i = 0; i < count; i++) {
            (thread_arg_list + i)->rdma_ctx = ctx;
            (thread_arg_list + i)->ctx_index = i;
            (thread_arg_list + i)->remote_endpoint = *(remote_endpoint + i);
            (thread_arg_list + i)->message_count = *(message_count + i);
            (thread_arg_list + i)->message_size = *(message_size + i);
            (thread_arg_list + i)->buffer_size = *(message_count + i) * *(message_size + i);
            (thread_arg_list + i)->mem_offset = *(mem_offset + i);

            (thread_arg_list + i)->client_id = i;
            (thread_arg_list + i)->stream = 0;
        }

        for (i = 0; i < count; i++) {
            if (pthread_create(client_thread_list + i, NULL, client_thread, thread_arg_list + i) != 0) {
                debug_print("(RDMA_SEND_MT) pthread_create() error\n");
            }
        }

        for (i = 0; i < count; i++) {
            if (pthread_join(*(client_thread_list + i), NULL) != 0) {
                debug_print("(RDMA_SEND_MT) pthread_join() error\n");
            }
        }

        free(thread_arg_list);
        free(client_thread_list);

        return 0;
    }

    int
    rdma_post_send_mt_stream(int *control_socket_list, struct rdma_context *ctx, struct rdma_endpoint **remote_endpoint, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned long *mem_offset, unsigned count)
    {
        int i;
        pthread_t *client_thread_list;
        struct rdma_thread_param *thread_arg_list;

        client_thread_list = (pthread_t *)calloc(count, sizeof(pthread_t));
        thread_arg_list = (struct rdma_thread_param *)calloc(count, sizeof(struct rdma_thread_param));

        for (i = 0; i < count; i++) {
            (thread_arg_list + i)->rdma_ctx = ctx;
            (thread_arg_list + i)->ctx_index = i;
            (thread_arg_list + i)->remote_endpoint = *(remote_endpoint + i);
            (thread_arg_list + i)->message_count = *(message_count + i);
            (thread_arg_list + i)->message_size = *(message_size + i);
            (thread_arg_list + i)->buffer_size = *(buffer_size + i);
            (thread_arg_list + i)->mem_offset = *(mem_offset + i);

            (thread_arg_list + i)->control_socket = *(control_socket_list + i);

            (thread_arg_list + i)->start_ts = rdma_utils::get_current_timestamp_ns();

            (thread_arg_list + i)->client_id = i;
            (thread_arg_list + i)->stream = 1;
        }

        for (i = 0; i < count; i++) {
            if (pthread_create(client_thread_list + i, NULL, client_thread, thread_arg_list + i) != 0) {
                debug_print("(RDMA_SEND_MT) pthread_create() error\n");
            } else {
                debug_print("(RDMA_SEND_MT) pthread_create() successful for client #%d\n", i);
            }
        }

        for (i = 0; i < count; i++) {
            if (pthread_join(*(client_thread_list + i), NULL) != 0) {
                debug_print("(RDMA_SEND_MT) pthread_join() error\n");
            }
        }

        free(thread_arg_list);
        free(client_thread_list);

        return 0;
    }

    void *
    receiver_data_thread(void *arg)
    {
        int i, sval;
        char buf[32];
        char *devnull;
        unsigned long chunk_size;
        unsigned int new_mem_offset_total;
        unsigned int new_mem_offset_circular;
        long int timestamp_ns;
        long int crt = 0;

        struct rdma_thread_param *thread_args = (struct rdma_thread_param *)arg;

        while (1) {
            sem_wait(thread_args->sem_recv_data);

            sem_getvalue(thread_args->sem_recv_data, &sval);

            timestamp_ns = rdma_utils::get_current_timestamp_ns() - thread_args->start_ts;

            // printf("receiver_data_thread: semaphore value: %d\n", sval);
            printf("d1:%ld:%ld:%d\n", timestamp_ns, (timestamp_ns / 1000000), sval);

            // printf("receiver_data_thread: used size: %d\n", thread_args->used_size);
            printf("d2:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), thread_args->used_size);

            // Print data in the reserved memory when the sem_recv_data semaphore unlocks, meaning that data has been received
            devnull = (char *)malloc(thread_args->received_size);
            chunk_size = thread_args->received_size;
            // chunk_size = thread_args->message_count * thread_args->message_size;
            // if (chunk_size > thread_args->used_size) {
            //     chunk_size = thread_args->used_size;
            // }

            // printf("receiver_data_thread: last transfer - received %d bytes\n", thread_args->received_size);
            new_mem_offset_total = thread_args->mem_offset + chunk_size;
            new_mem_offset_circular = new_mem_offset_total % thread_args->buffer_size;
            // printf("receiver_data_thread: buffer addr: %d\n", (char *)(*thread_args->rdma_ctx->buf));
            // printf("receiver_data_thread: buffer offset: %d\n", thread_args->mem_offset);
            // printf("receiver_data_thread: buffer addr + offset: %d %d %d\n", (char *)(*thread_args->rdma_ctx->buf) + thread_args->mem_offset, (char *)(*thread_args->rdma_ctx->buf), thread_args->mem_offset);
            if (new_mem_offset_total > thread_args->buffer_size) {
                // printf("taped together\n");
                memcpy(devnull, (char *)(*thread_args->rdma_ctx->buf) + thread_args->mem_offset, chunk_size - new_mem_offset_circular);
                // for (i = 0; i < chunk_size - new_mem_offset_circular; i++) {
                //     printf("%d:", *(devnull + i));
                // }
                memcpy(devnull, (char *)(*thread_args->rdma_ctx->buf), new_mem_offset_circular);
                // for (i = 0; i < new_mem_offset_circular; i++) {
                //     printf("%d:", *(devnull + i));
                // }
                // printf("\n");
            } else {
                // printf("single chunk\n");
                memcpy(devnull, (char *)(*thread_args->rdma_ctx->buf) + thread_args->mem_offset, chunk_size);
                // for (i = 0; i < chunk_size; i++) {
                //     printf("%d:", *(devnull + i));
                // }
                // printf("\n");
            }
            thread_args->mem_offset = new_mem_offset_circular;
            thread_args->used_size -= chunk_size;
            // printf("\nreceiver_data_thread: finished processing %d bytes chunk\n", chunk_size);

            // usleep(50);

            free(devnull);
            // End of data check

            // printf("receiver_data_thread: new used size: %d\n", thread_args->used_size);
            printf("d3:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), thread_args->used_size);

            bzero(buf, 32);
            sprintf(buf, "GO:%ld", crt);
            // pthread_mutex_lock(thread_args->backpressure_mutex);
            if (thread_args->backpressure == 1 && thread_args->used_size < (thread_args->buffer_size * thread_args->backpressure_threshold_down / 100)) {
                write(thread_args->control_socket, buf, 32);
                thread_args->backpressure = 0;
                // printf("receiver_data_thread: backpressure change: GO\n");
                printf("d4:%ld:%ld:%d:%ld\n", timestamp_ns, (timestamp_ns / 1000000), 1, crt++);
            } else if (thread_args->backpressure == 0) {
                // printf("receiver_control_thread: backpressure already on: GO\n");
                printf("d4:%ld:%ld:%d\n", timestamp_ns, (timestamp_ns / 1000000), 0);
            }
            // pthread_mutex_unlock(thread_args->backpressure_mutex);
        }
    }

    void *
    receiver_control_thread(void *arg)
    {
        char buf[32];
        char* end;
        int backpressure = 0, backpressure_change = 0;
        long int timestamp_ns;
        long int crt = 0;

        struct rdma_thread_param *thread_args = (struct rdma_thread_param *)arg;

        while (1) {
            // debug_print("receiver_control_thread: waiting to receive bytes\n");
            bzero(buf, 32);
            read(thread_args->control_socket, buf, 32);

            timestamp_ns = rdma_utils::get_current_timestamp_ns() - thread_args->start_ts;

            thread_args->received_size = strtol(buf, &end, 0);
            if (end == buf) {
                debug_print("receiver_control_thread: failed to convert received string \"%s\" to unsigned int\n", buf);
            }
            // debug_print("receiver_control_thread: received bytes: %d\n", thread_args->received_size);
            thread_args->used_size += thread_args->received_size;
            thread_args->used_size_timed += thread_args->received_size;

            sem_post(thread_args->sem_recv_data);

            // printf("receiver_control_thread: new used size: %d\n", thread_args->used_size);
            printf("c1:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), thread_args->used_size);

            bzero(buf, 32);
            sprintf(buf, "WAIT:%ld", crt);
            // pthread_mutex_lock(thread_args->backpressure_mutex);
            if (thread_args->backpressure == 0 && thread_args->used_size >= (thread_args->buffer_size * thread_args->backpressure_threshold_up / 100)) {
                write(thread_args->control_socket, buf, 32);
                thread_args->backpressure = 1;
                // printf("receiver_control_thread: backpressure change: WAIT\n");
                printf("c2:%ld:%ld:%d:%ld\n", timestamp_ns, (timestamp_ns / 1000000), 1, crt++);
            } else if (thread_args->backpressure == 1) {
                // printf("receiver_control_thread: backpressure already on: WAIT\n");
                printf("c2:%ld:%ld:%d\n", timestamp_ns, (timestamp_ns / 1000000), 0);
            }
            // pthread_mutex_unlock(thread_args->backpressure_mutex);
        }
    }

    void *
    receiver_instrumentation_thread(void *arg)
    {
        int tfd = timerfd_create(CLOCK_MONOTONIC, 0);
        unsigned long timestamp_s = 0;
        ssize_t s;
        uint64_t exp;
        struct rdma_thread_param *thread_args = (struct rdma_thread_param *)arg;

        if (tfd == -1) {
            printf("receiver_instrumentation_thread: timerfd_create failed\n");
        }

        rdma_utils::set_timerfd(tfd, 1, 0);
        // set_timerfd(tfd, 0, 1000000);

        while (1) {
            s = read(tfd, &exp, sizeof(uint64_t));
            if (s != sizeof(uint64_t)) {
                printf("receiver_instrumentation_thread: read failed\n");
            }

            printf("i1:%lu:%lu:%lu\n", timestamp_s * 1000000000, timestamp_s, thread_args->used_size_timed);
            // printf("i1:%lu:%lu:%lu\n", timestamp_s * 1000000, timestamp_s, thread_args->used_size_timed);
            timestamp_s++;

            thread_args->used_size_timed = 0;
        }
    }*/

    int
    rdma_consume(int control_socket, unsigned int backpressure_threshold_up, unsigned int backpressure_threshold_down, struct rdma_context *ctx,
            unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned long *mem_offset)
    {
        pthread_t *data_thread, *control_thread, *instrumentation_thread;
        struct rdma_thread_param *thread_args;

        data_thread = (pthread_t *)malloc(sizeof(pthread_t));
        control_thread = (pthread_t *)malloc(sizeof(pthread_t));
        instrumentation_thread = (pthread_t *)malloc(sizeof(pthread_t));
        thread_args = (struct rdma_thread_param *)malloc(sizeof(struct rdma_thread_param));

        printf("rdma_consume before: buffer addr: %ld\n", (uint64_t)(ctx->buf));
        thread_args->rdma_ctx = ctx;
        printf("rdma_consume after: buffer addr: %ld\n", (uint64_t)(thread_args->rdma_ctx->buf));
        thread_args->message_count = *message_count;
        thread_args->message_size = *message_size;
        thread_args->buffer_size = *buffer_size;
        thread_args->mem_offset = *mem_offset;
        thread_args->used_size = 0;
        thread_args->used_size_timed = 0;
        thread_args->received_size = 0;

        thread_args->control_socket = control_socket;
        thread_args->backpressure = 0;
        thread_args->backpressure_threshold_up = backpressure_threshold_up;
        thread_args->backpressure_threshold_down = backpressure_threshold_down;

        thread_args->start_ts = rdma_utils::get_current_timestamp_ns();

        thread_args->sem_recv_data = (sem_t *)malloc(sizeof(sem_t));
        thread_args->backpressure_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));

        sem_init(thread_args->sem_recv_data, 0, 0);
        pthread_mutex_init(thread_args->backpressure_mutex, NULL);

        #warning receiver_threads not implemented
        #define receiver_data_thread 0
        #define receiver_control_thread 0
        #define receiver_instrumentation_thread 0

        if (pthread_create(data_thread, NULL, receiver_data_thread, thread_args) != 0) {
            debug_print("(RDMA_CONSUME) pthread_create() error - data_thread\n");
        }

        if (pthread_create(control_thread, NULL, receiver_control_thread, thread_args) != 0) {
            debug_print("(RDMA_CONSUME) pthread_create() error - control_thread\n");
        }

        if (pthread_create(instrumentation_thread, NULL, receiver_instrumentation_thread, thread_args) != 0) {
            debug_print("(RDMA_CONSUME) pthread_create() error - instrumentation_thread\n");
        }

        if (pthread_join(*data_thread, NULL) != 0) {
            debug_print("(RDMA_CONSUME) pthread_join() error - data_thread\n");
        }

        if (pthread_join(*control_thread, NULL) != 0) {
            debug_print("(RDMA_CONSUME) pthread_join() error - control_thread\n");
        }

        if (pthread_join(*instrumentation_thread, NULL) != 0) {
            debug_print("(RDMA_CONSUME) pthread_join() error - instrumentation_thread\n");
        }

        pthread_mutex_destroy(thread_args->backpressure_mutex);

        free(thread_args->sem_recv_data);
        free(thread_args->backpressure_mutex);
        free(thread_args);

        return 0;
    }
#endif /* RDMA_INFRASTRUCTURE_H */
