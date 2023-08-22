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

#define debug_print(fmt, ...) do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

// #define RDMA_MAX_SEND_WR (32)
// #define RDMA_MAX_RECV_WR (32)
#define RDMA_MAX_SEND_WR (8192)
#define RDMA_MAX_RECV_WR (8192)


enum rdma_role {
    RDMA_SENDER,
    RDMA_RECEIVER,
};

enum rdma_function {
    RDMA_SEND,
    RDMA_WRITE,
};

struct rdma_context {
    struct ibv_context *context;
    struct ibv_pd *pd;
    struct ibv_mr **mr;
    struct ibv_cq **cq;
    struct ibv_qp **qp;
    char **buf;
    unsigned long *size;
    int send_flags;
    struct ibv_port_attr portinfo;
};

struct rdma_endpoint {
    uint16_t lid;
    uint32_t qpn;
    uint32_t psn;
    uint32_t rkey;
    uint64_t addr;
    char gid_string[32];
    union ibv_gid gid;
};

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

struct linked_list_node {
    unsigned int sent_size;
    struct linked_list_node *next;
};

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

struct rdma_config {
    int function;

    const char *local_hostname;
    unsigned local_port;

    const char *remote_hostname;
    unsigned remote_port;

    char *ib_devname;
    uint32_t gidx;
    enum ibv_mtu mtu;
  
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;

    struct rdma_context *rdma_ctx;
    struct rdma_endpoint **local_endpoint;
    struct rdma_endpoint **remote_endpoint;

    unsigned long *message_count;
    unsigned long *message_size;
    unsigned long *buffer_size;
    unsigned long *mem_offset;

    unsigned remote_count; // = <client count> on sender and = 1 on receiver
};


int rdma_get_port_info(struct ibv_context *context, int port, struct ibv_port_attr *attr);
void wire_gid_to_gid(const char *wgid, union ibv_gid *gid);
void gid_to_wire_gid(const union ibv_gid *gid, char wgid[]);

char ** rdma_prepare(struct rdma_config *config, int role);
struct rdma_context * rdma_init_ctx(struct ibv_device *ib_dev, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned count, int port, int role);
int rdma_connect_ctx(struct rdma_context *ctx, int port, enum ibv_mtu mtu, struct rdma_endpoint **local_endpoint, struct rdma_endpoint **remote_endpoint, unsigned count, int sgid_idx, int role);
int rdma_close_ctx(struct rdma_context *ctx, unsigned count);

int rdma_post_send(struct rdma_context *ctx, struct rdma_endpoint **remote_endpoint, unsigned long *message_count, unsigned long *message_size, unsigned long *mem_offset, unsigned count);
int rdma_post_send_mt(struct rdma_context *ctx, struct rdma_endpoint **remote_endpoint, unsigned long *message_count, unsigned long *message_size, unsigned long *mem_offset, unsigned count);
int rdma_post_send_mt_stream(int *control_socket_list, struct rdma_context *ctx, struct rdma_endpoint **remote_endpoint, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned long *mem_offset, unsigned count);
int rdma_consume(int control_socket, unsigned int backpressure_threshold_up, unsigned int backpressure_threshold_down, struct rdma_context *ctx, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned long *mem_offset);

#endif /* RDMA_INFRASTRUCTURE_H */
