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

#define DEBUG 0
#define debug_print(fmt, ...) do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

// #define RDMA_MAX_SEND_WR (32)
// #define RDMA_MAX_RECV_WR (32)
// #define RDMA_MAX_SEND_WR (64)
// #define RDMA_MAX_RECV_WR (64)
// #define RDMA_MAX_SEND_WR (128)
// #define RDMA_MAX_RECV_WR (128)
// #define RDMA_MAX_SEND_WR (160)
// #define RDMA_MAX_RECV_WR (160)
#define RDMA_MAX_SEND_WR (8192)
#define RDMA_MAX_RECV_WR (8192)

namespace rdma {
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
        unsigned long *psize;
        int send_flags;
        struct ibv_port_attr portinfo;

    public:
        void clear_data_structures() {
            context = 0;
            pd = 0;
            mr = 0;
            cq = 0;
            qp = 0;
            buf = 0;
            psize = 0;
            send_flags = 0;
            {
                portinfo.state = IBV_PORT_NOP;
                portinfo.max_mtu = static_cast<ibv_mtu>(0);
                portinfo.active_mtu = static_cast<ibv_mtu>(0);
                portinfo.gid_tbl_len = 0;
                portinfo.port_cap_flags = 0;
                portinfo.max_msg_sz = 0;
                portinfo.bad_pkey_cntr = 0;
                portinfo.qkey_viol_cntr = 0;
                portinfo.pkey_tbl_len = 0;
                portinfo.lid = 0;
                portinfo.sm_lid = 0;
                portinfo.lmc = 0;
                portinfo.max_vl_num = 0;
                portinfo.sm_sl = 0;
                portinfo.subnet_timeout = 0;
                portinfo.init_type_reply = 0;
                portinfo.active_width = 0;
                portinfo.active_speed = 0;
                portinfo.phys_state = 0;
                portinfo.link_layer = 0;
                portinfo.flags = 0;
                portinfo.port_cap_flags2 = 0;
            }
        }
        int rdma_get_port_info(int port, struct ibv_port_attr *attr)
        {
            return ibv_query_port(context, port, attr);
        }

        bool init(struct ibv_device *ib_dev, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned count, int port, int role)
        {
            int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
            int i, j;

            clear_data_structures();
            psize = new unsigned long[count];
            if (!psize) {
                fprintf(stderr, "rdma_init_ctx: Failed to create work buffer size(s).\n");
                return false;
            }
            for (i = 0; i < count; i++) {
                if (role == RDMA_SENDER) {
                    printf("message_count: %ld, message_size: %ld\n", message_count[i], message_size[i]);
                    psize[i] = message_count[i] * message_size[i];
                } else {
                    printf("buffer_size: %ld\n", (uint64_t)buffer_size[i]);
                    psize[i] = buffer_size[i];
                }
            }
            send_flags = IBV_SEND_SIGNALED;

            buf = new char* [count];
            if (!buf) {
                fprintf(stderr, "rdma_init_ctx: Couldn't allocate array of work buffers.\n");
                goto clean_ctx;
            }
            printf("rdma_init_ctx 1: buffer addr: %ld\n", (uint64_t)buf);
            for (i = 0; i < count; i++) {
                buf[i] = new char[psize[i]];
                if (!buf[i]) {
                    fprintf(stderr, "rdma_init_ctx: Couldn't allocate work buffer #%d out of %d.\n", i + 1, count);
                    goto clean_ctx;
                }
            }

            for (i = 0; i < count; i++) {
                if (role == RDMA_SENDER) {
                    for (j = 0; j < *(message_count + i); j++) {
                        memset(buf[i] + j * *(message_size + i), j+43+i*(*(message_count + i)), *(message_size + i));
                    }
                } else {
                    memset(buf[i], 0x7b, psize[i]);
                }
            }

            // open device
            context = ibv_open_device(ib_dev);
            if (!context) {
                fprintf(stderr, "rdma_init_ctx: Couldn't get context for %s\n", ibv_get_device_name(ib_dev));
                goto clean_buffer;
            }

            // create PD (Protection Domani) - a single one
            pd = ibv_alloc_pd(context);
            if (!pd) {
                fprintf(stderr, "rdma_init_ctx: Couldn't allocate PD\n");
            }

            // create MR (Memory Region) - one for each client
            mr = (struct ibv_mr **)calloc(count, sizeof(struct ibv_mr *));
            if (!mr) {
                fprintf(stderr, "rdma_init_ctx: Couldn't allocate MR array\n");
                goto clean_pd;
            }
            printf("rdma_init_ctx 2: buffer addr: %ld\n", (uint64_t)buf);
            printf("rdma_init_ctx 3: buffer addr: %ld\n", (uint64_t)(*buf)); // DUBIOS
            for (i = 0; i < count; i++) {
                mr[i] = ibv_reg_mr(pd, buf[i], psize[i], access_flags);
                fprintf(stderr, "rdma_init_ctx: MR addr: %ld\n", (uint64_t)(mr[i]->addr));
                fprintf(stderr, "rdma_init_ctx: buffer addr: %d\n", *buf[i]);
                if (!mr[i]) {
                    fprintf(stderr, "rdma_init_ctx: Couldn't register MR #%d out of %d.\n", i + 1, count);
                    goto clean_mr;
                }
            }

            // create CQ (Completion Queue) - one for each client
            cq = (struct ibv_cq **)calloc(count, sizeof(struct ibv_cq *));
            if (!cq) {
                fprintf(stderr, "rdma_init_ctx: Couldn't allocate CQ array\n");
                goto clean_mr;
            }
            for (i = 0; i < count; i++) {
                cq[i] = ibv_create_cq(context, RDMA_MAX_SEND_WR, NULL, NULL, 0);
                if (!cq[i]) {
                    fprintf(stderr, "rdma_init_ctx: Couldn't create CQ #%d out of %d.\n", i + 1, count);
                    goto clean_cq;
                }
            }

            // create QP (Queue Pair) - one for each client
            qp = (struct ibv_qp **)calloc(count, sizeof(struct ibv_qp *));
            if (!qp) {
                fprintf(stderr, "rdma_init_ctx: Couldn't allocate QP array\n");
                goto clean_cq;
            }
            for (i = 0; i < count; i++) {
                {
                    struct ibv_qp_init_attr init_attr = {
                            .send_cq = cq[i],
                            .recv_cq = cq[i],
                            .cap     = {
                                    .max_send_wr  = RDMA_MAX_SEND_WR,
                                    .max_recv_wr  = RDMA_MAX_RECV_WR,
                                    .max_send_sge = 1,
                                    .max_recv_sge = 1
                            },
                            .qp_type = IBV_QPT_RC,
                            .sq_sig_all = 1
                    };

                    qp[i] = ibv_create_qp(pd, &init_attr);
                    if (!qp[i])  {
                        fprintf(stderr, "rdma_init_ctx: Couldn't create QP #%d out of %d.\n", i + 1, count);
                        goto clean_qp;
                    }
                }

                {
                    struct ibv_qp_attr attr = {
                            .qp_state        = IBV_QPS_INIT,
                            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE,
                            .pkey_index      = 0,
                            .port_num        = static_cast<uint8_t>(port)
                    };

                    if (ibv_modify_qp(qp[i], &attr,
                                      IBV_QP_STATE              |
                                      IBV_QP_PKEY_INDEX         |
                                      IBV_QP_PORT               |
                                      IBV_QP_ACCESS_FLAGS)) {
                        fprintf(stderr, "rdma_init_ctx: Failed to modify QP (#%d out of %d) to INIT.\n", i + 1, count);
                        goto clean_qp;
                    } else {
                        fprintf(stdout, "rdma_init_ctx: QP (#%d out of %d) state set to INIT\n", i + 1, count);
                    }
                }
            }
            return true;

            clean_qp:
            for (i = 0; i < count; i++) {
                if (qp[i]) {
                    ibv_destroy_qp(qp[i]);
                }
            }
            delete(qp);

            clean_cq:
            for (i = 0; i < count; i++) {
                if (cq[i]) {
                    ibv_destroy_cq(cq[i]);
                }
            }
            delete(cq);

            clean_mr:
            for (i = 0; i < count; i++) {
                if (mr[i]) {
                    ibv_dereg_mr(mr[i]);
                }
            }
            delete(mr);

            clean_pd:
            ibv_dealloc_pd(pd);

            clean_buffer:
            for (i = 0; i < count; i++) {
                if (buf[i]) {
                    delete(buf[i]);
                }
            }
            delete buf;

            clean_ctx:
            return false;
        }
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

    class rdma_config {
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

    public:
        char ** rdma_prepare(int role)
        {
            int i;
            char **rdma_metadata;
            struct timespec now;

            dev_list = ibv_get_device_list(NULL);
            if (!dev_list) {
                fprintf(stderr, "rdma_prepare: Failed to get IB devices list\n");
                return NULL;
            }

            for (i = 0; dev_list[i]; ++i) {
                if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname)) {
                    break;
                }
            }
            ib_dev = dev_list[i];
            if (!ib_dev) {
                fprintf(stderr, "rdma_prepare: IB device %s not found\n", ib_devname);
                return NULL;
            }

            rdma_ctx->init(ib_dev, message_count, message_size, buffer_size, remote_count, 1, role);
            if (!rdma_ctx) {
                fprintf(stderr, "rdma_prepare: Failed to create RDMA context\n");
                return NULL;
            }

            printf("rdma_prepare: buffer addr: %ld\n", (uint64_t)(rdma_ctx->buf));

            if (rdma_get_port_info(rdma_ctx->context, 1, &rdma_ctx->portinfo)) {
                fprintf(stderr, "rdma_prepare: Couldn't get port info\n");
                return NULL;
            }

            rdma_metadata = (char **)calloc(remote_count, sizeof(char *));
            for (i=0; i < remote_count; i++) {
                (*(local_endpoint + i))->lid = rdma_ctx->portinfo.lid;
                if (rdma_ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET && !(*(local_endpoint + i))->lid) {
                    fprintf(stderr, "rdma_prepare: Couldn't get local LID\n");
                    return NULL;
                }

                if (gidx >= 0) {
                    if (ibv_query_gid(rdma_ctx->context, 1, gidx, &(*(local_endpoint + i))->gid)) {
                        fprintf(stderr, "rdma_prepare: Can't read sgid of index %d\n", gidx);
                        return NULL;
                    }
                } else {
                    memset(&(*(local_endpoint + i))->gid, 0, sizeof((*(local_endpoint + i))->gid));
                }

                inet_ntop(AF_INET6, &(*(local_endpoint + i))->gid, (*(local_endpoint + i))->gid_string, sizeof((*(local_endpoint + i))->gid_string));
                gid_to_wire_gid(&(*(local_endpoint + i))->gid, (*(local_endpoint + i))->gid_string);

                if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
                    srand(time(NULL));
                } else {
                    srand((int)now.tv_nsec);
                }
                (*(local_endpoint + i))->qpn = (*(rdma_ctx->qp + i))->qp_num;
                (*(local_endpoint + i))->psn = rand() & 0xffffff;

                if (function == RDMA_WRITE && role == RDMA_RECEIVER) {
                    (*(local_endpoint + i))->rkey = (*(rdma_ctx->mr + i))->rkey;
                    (*(local_endpoint + i))->addr = (uint64_t)(*(rdma_ctx->mr + i))->addr;
                    *(rdma_metadata + i) = (char *)malloc(78); // 4+1+6+1+6+1+8+1+16+1+32+1 (last one is the string terminator)
                    memset(*(rdma_metadata + i), 0, 78);
                    snprintf(*(rdma_metadata + i), 78, "%04x:%06x:%06x:%08x:%016lx:%s", local_endpoint[i]->lid, local_endpoint[i]->qpn, local_endpoint[i]->psn, local_endpoint[i]->rkey, local_endpoint[i]->addr, local_endpoint[i]->gid_string);
                    debug_print("(RDMA_WRITE) local RDMA metadata for remote #%d: %s\n", i, *(rdma_metadata + i));
                } else {
                    *(rdma_metadata + i) = (char *)malloc(52); // 4+1+6+1+6+1++32+1 (last one is the string terminator)
                    memset(*(rdma_metadata + i), 0, 52);
                    snprintf(*(rdma_metadata + i), 52, "%04x:%06x:%06x:%s", local_endpoint[i]->lid, local_endpoint[i]->qpn, local_endpoint[i]->psn, local_endpoint[i]->gid_string);
                    debug_print("(RDMA_SEND) local RDMA metadata for remote #%d: %s\n", i, *(rdma_metadata + i));
                }
            }

            return(rdma_metadata);
        }
    };

    void
    wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
    {
        char tmp[9];
        __be32 v32;
        int i;
        uint32_t tmp_gid[4];

        for (tmp[8] = 0, i = 0; i < 4; ++i) {
            memcpy(tmp, wgid + i * 8, 8);
            sscanf(tmp, "%x", &v32);
            tmp_gid[i] = be32toh(v32);
        }
        memcpy(gid, tmp_gid, sizeof(*gid));
    }

    void
    gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
    {
        uint32_t tmp_gid[4];
        int i;

        memcpy(tmp_gid, gid, sizeof(tmp_gid));
        for (i = 0; i < 4; ++i) {
            sprintf(&wgid[i * 8], "%08x", htobe32(tmp_gid[i]));
        }
    }

    static void
    set_timerfd(int fd, unsigned s, unsigned ns)
    {
        struct itimerspec it;

        it.it_interval.tv_sec = s;
        it.it_interval.tv_nsec = ns;
        it.it_value.tv_sec = s;
        it.it_value.tv_nsec = ns;

        if (timerfd_settime(fd, 0, &it, NULL)) {
            printf("set_timerfd: timerfd_settime failed for fd %d. The timer will not fire.", fd);
            return;
        }
    }

    static long int
    get_current_timestamp_ns()
    {
        struct timespec now;

        if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
            printf("get_current_timestamp_ns: clock_gettime failed");
            return -1;
        } else {
            return now.tv_nsec + now.tv_sec * 1E9;
        }
    }

    static long int
    get_current_timestamp_ns_thread_cpu()
    {
        struct timespec now;

        if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now) == -1) {
            printf("get_current_timestamp_ns: clock_gettime failed");
            return -1;
        } else {
            return now.tv_nsec + now.tv_sec * 1E9;
        }
    }

    int
    rdma_connect_ctx(struct rdma_context *ctx, int port, enum ibv_mtu mtu, struct rdma_endpoint **local_endpoint, struct rdma_endpoint **remote_endpoint, unsigned count, int sgid_idx, int role)
    {
        int i;

        for (i = 0; i < count; i++) {
            struct ibv_qp_attr attr = {
                    .qp_state           = IBV_QPS_RTR,
                    .path_mtu           = mtu,
                    .rq_psn             = (*(remote_endpoint + i))->psn,
                    .dest_qp_num        = (*(remote_endpoint + i))->qpn,
                    .ah_attr			= {
                            .dlid           = (*(remote_endpoint + i))->lid,
                            .sl             = 0,
                            .src_path_bits  = 0,
                            .is_global      = 0,
                            .port_num       = static_cast<uint8_t>(port)
                    },
                    .max_dest_rd_atomic	= 1,
                    .min_rnr_timer      = 12
            };

            if ((*(remote_endpoint + i))->gid.global.interface_id) {
                attr.ah_attr.is_global      = 1;
                attr.ah_attr.grh.hop_limit  = 1;
                attr.ah_attr.grh.dgid       = (*(remote_endpoint + i))->gid;
                attr.ah_attr.grh.sgid_index = sgid_idx;
            }

            if (ibv_modify_qp(*(ctx->qp + i), &attr,
                              IBV_QP_STATE              |
                              IBV_QP_AV                 |
                              IBV_QP_PATH_MTU           |
                              IBV_QP_DEST_QPN           |
                              IBV_QP_RQ_PSN             |
                              IBV_QP_MAX_DEST_RD_ATOMIC |
                              IBV_QP_MIN_RNR_TIMER)) {
                fprintf(stderr, "rdma_connect_ctx: Failed to modify QP (#%d out of %d) to RTR\n", i + 1, count);
                return 1;
            } else {
                fprintf(stdout, "rdma_connect_ctx: QP (#%d out of %d) state set to RTR\n", i + 1, count);
            }

            if (role == RDMA_SENDER) {
                attr.qp_state       = IBV_QPS_RTS;
                attr.timeout        = 16;
                attr.retry_cnt      = 7;
                attr.rnr_retry      = 6;
                attr.sq_psn         = (*(local_endpoint + i))->psn;
                attr.max_rd_atomic  = 1;

                if (ibv_modify_qp(*(ctx->qp + i), &attr,
                                  IBV_QP_STATE              |
                                  IBV_QP_TIMEOUT            |
                                  IBV_QP_RETRY_CNT          |
                                  IBV_QP_RNR_RETRY          |
                                  IBV_QP_SQ_PSN             |
                                  IBV_QP_MAX_QP_RD_ATOMIC)) {
                    fprintf(stderr, "rdma_connect_ctx: Failed to modify QP (#%d out of %d) to RTS\n", i + 1, count);
                    return 1;
                } else {
                    fprintf(stdout, "rdma_connect_ctx: QP (#%d out of %d) state set to RTS\n", i + 1, count);
                }
            }
        }

        return 0;
    }

    int
    rdma_close_ctx(struct rdma_context *ctx, unsigned count)
    {
        int i;

        for (i = 0; i < count; i++) {
            if (*(ctx->qp + i)) {
                if (ibv_destroy_qp(*(ctx->qp + i))) {
                    fprintf(stderr, "rdma_close_ctx: Couldn't destroy QP (#%d out of %d)\n", i + 1, count);
                    return 1;
                }
            }
        }
        free(ctx->qp);

        for (i = 0; i < count; i++) {
            if (*(ctx->cq + i)) {
                if (ibv_destroy_cq(*(ctx->cq + i))) {
                    fprintf(stderr, "rdma_close_ctx: Couldn't destroy CQ (#%d out of %d)\n", i + 1, count);
                    return 1;
                }
            }
        }
        free(ctx->cq);

        for (i = 0; i < count; i++) {
            if (*(ctx->mr + i)) {
                if (ibv_dereg_mr(*(ctx->mr + i))) {
                    fprintf(stderr, "rdma_close_ctx: Couldn't deregister MR (#%d out of %d)\n", i + 1, count);
                    return 1;
                }
            }
        }
        free(ctx->mr);

        if (ibv_dealloc_pd(ctx->pd)) {
            fprintf(stderr, "rdma_close_ctx: Couldn't deallocate PD\n");
            return 1;
        }

        if (ibv_close_device(ctx->context)) {
            fprintf(stderr, "rdma_close_ctx: Couldn't release context\n");
            return 1;
        }

        for (i = 0; i < count; i++) {
            if (*(ctx->buf + i)) {
                free(*(ctx->buf + i));
            }
        }
        free(ctx->buf);

        free(ctx);

        return 0;
    }

    int
    rdma_post_send(struct rdma_context *ctx, struct rdma_endpoint **remote_endpoint, unsigned long *message_count, unsigned long *message_size, unsigned long *mem_offset, unsigned count)
    {
        struct ibv_send_wr *wr, **bad_wr;
        struct ibv_sge *list;

        int i, j, k, l, total, full_queue_count, remainder_queue_size, num_cq_events, loop_num_cq_events, done, last, left, ne;
        struct ibv_wc wc[RDMA_MAX_SEND_WR];

        for (k = 0; k < count; k++) {
            full_queue_count = *(message_count + k) / RDMA_MAX_SEND_WR;
            remainder_queue_size = *(message_count + k) % RDMA_MAX_SEND_WR;

            total = 0;
            num_cq_events = 0;
            for (j = 0; j < full_queue_count; j++) {
                wr = (struct ibv_send_wr *)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_send_wr));
                bad_wr = (struct ibv_send_wr **)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_send_wr *));
                list = (struct ibv_sge *)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_sge));

                // printf("PREPARE: client #%d:\n", k + 1);
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

                        (wr + i)->send_flags = ctx->send_flags;
                        (wr + i)->wr.rdma.remote_addr = (*(remote_endpoint + k))->addr + *(mem_offset + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k);
                        (wr + i)->wr.rdma.rkey = (*(remote_endpoint + k))->rkey;

                        (list + i)->length = *(message_size + k);
                        (list + i)->addr = (uint64_t)(*(ctx->buf + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k));
                        (list + i)->lkey = (*(ctx->mr + k))->lkey;

                        if (ibv_post_send(*(ctx->qp + k), wr + i, bad_wr + i)) {
                            fprintf(stderr, "rdma_post_send: Couldn't post send #%d\n", i);
                            break;
                        } else {
                            total++;
                        }
                    }
                    // printf("\nDONE PREPARE #%d, loop %d\n", k + 1, j);

                    debug_print("(client %d, loop %d) written %d posts\n", k + 1, j, i);
                    if (i < RDMA_MAX_SEND_WR) {
                        debug_print("(client %d, loop %d) missing %d writes\n", k + 1, j, RDMA_MAX_SEND_WR - i);
                        done = 0;
                        last = i;
                    } else {
                        debug_print("(client %d, loop %d) all writes done\n", k + 1, j);
                        done = 1;
                    }

                    left = RDMA_MAX_SEND_WR;
                    do {
                        ne = ibv_poll_cq(*(ctx->cq + k), left, wc);
                        if (ne < 0) {
                            debug_print("(client %d, loop %d) poll CQ failed %d\n", k + 1, j, ne);
                        } else {
                            left -= ne;
                            debug_print("(client %d, loop %d) ne=%d, left=%d\n", k + 1, j, ne, left);
                        }
                    } while (left > 0);
                    debug_print("(client %d, loop %d) all ACK\n", k + 1, j);
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

                    (wr + i)->send_flags = ctx->send_flags;
                    (wr + i)->wr.rdma.remote_addr = (*(remote_endpoint + k))->addr + *(mem_offset + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k);
                    (wr + i)->wr.rdma.rkey = (*(remote_endpoint + k))->rkey;

                    (list + i)->length = *(message_size + k);
                    (list + i)->addr = (uint64_t)(*(ctx->buf + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k));
                    (list + i)->lkey = (*(ctx->mr + k))->lkey;

                    if (ibv_post_send(*(ctx->qp + k), wr + i, bad_wr + i)) {
                        fprintf(stderr, "rdma_post_send: Couldn't post send #%d\n", i);
                        break;
                    } else {
                        total++;
                    }
                }

                debug_print("(client %d, final loop) written %d posts\n", k + 1, i);
                if (i < remainder_queue_size) {
                    debug_print("(client %d, final loop) missing %d writes\n", k + 1, remainder_queue_size - i);
                    done = 0;
                    last = i;
                } else {
                    debug_print("(client %d, final loop) all writes done\n", k + 1);
                    done = 1;
                }

                left = remainder_queue_size;
                do {
                    // sleep(1);
                    ne = ibv_poll_cq(*(ctx->cq + k), left, wc);
                    if (ne < 0) {
                        debug_print("(client %d, final loop) poll CQ failed %d\n", k + 1, ne);
                    } else {
                        for (l=0; l<ne; l++) {
                            if ((wc + l)->status != IBV_WC_SUCCESS) {
                                debug_print("(RDMA_SEND) ibv_poll_cq failed status %s (%d) for wr_id %d\n", ibv_wc_status_str((wc + l)->status), (wc + l)->status, (int)((wc + l)->wr_id));
                            } else {
                                debug_print("(RDMA_SEND) ibv_poll_cq success status for wr_id %d\n", (int)((wc + l)->wr_id));
                            }
                        }
                        left -= ne;
                        debug_print("(client %d, final loop) ne=%d, left=%d\n", k + 1, ne, left);
                    }
                } while (left > 0);
                debug_print("(client %d, final loop) all ACK\n", k + 1);
            }

            free(wr);
            free(bad_wr);
            free(list);
        }

        return 0;
    }

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
            timestamp_ns = (get_current_timestamp_ns() - backpressure_thread_args->start_ts) / 1E6;

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
            timestamp_ns = (get_current_timestamp_ns() - thread_args->start_ts) / 1E6;

            if (!backpressure_thread_args->backpressure) {
                timestamp_ns_thread_cpu_start = get_current_timestamp_ns_thread_cpu();

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

                timestamp_ns_thread_cpu_now = get_current_timestamp_ns_thread_cpu();

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

            (thread_arg_list + i)->start_ts = get_current_timestamp_ns();

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

            timestamp_ns = get_current_timestamp_ns() - thread_args->start_ts;

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

            timestamp_ns = get_current_timestamp_ns() - thread_args->start_ts;

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

        set_timerfd(tfd, 1, 0);
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
    }

    int
    rdma_consume(int control_socket, unsigned int backpressure_threshold_up, unsigned int backpressure_threshold_down, struct rdma_context *ctx, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned long *mem_offset)
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

        thread_args->start_ts = get_current_timestamp_ns();

        thread_args->sem_recv_data = (sem_t *)malloc(sizeof(sem_t));
        thread_args->backpressure_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));

        sem_init(thread_args->sem_recv_data, 0, 0);
        pthread_mutex_init(thread_args->backpressure_mutex, NULL);

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
}
#endif /* RDMA_INFRASTRUCTURE_H */
