#include "rdma_infrastructure.h"


int
rdma_get_port_info(struct ibv_context *context, int port, struct ibv_port_attr *attr)
{
	return ibv_query_port(context, port, attr);
}

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

char **
rdma_prepare(struct rdma_config *config, int role)
{
    int i;
    char **rdma_metadata;
    struct timespec now;

    config->dev_list = ibv_get_device_list(NULL);
    if (!config->dev_list) {
        fprintf(stderr, "rdma_prepare: Failed to get IB devices list\n");
        return NULL;
    }

    for (i = 0; config->dev_list[i]; ++i) {
        if (!strcmp(ibv_get_device_name(config->dev_list[i]), config->ib_devname)) {
            break;
        }
    }
    config->ib_dev = config->dev_list[i];
    if (!config->ib_dev) {
        fprintf(stderr, "rdma_prepare: IB device %s not found\n", config->ib_devname);
        return NULL;
    }

    config->rdma_ctx = rdma_init_ctx(config->ib_dev, config->message_count, config->message_size, config->buffer_size, config->remote_count, 1, role);
    if (!config->rdma_ctx) {
        fprintf(stderr, "rdma_prepare: Failed to create RDMA context\n");
        return NULL;
    }

    printf("rdma_prepare: buffer addr: %d\n", config->rdma_ctx->buf);

    if (rdma_get_port_info(config->rdma_ctx->context, 1, &config->rdma_ctx->portinfo)) {
        fprintf(stderr, "rdma_prepare: Couldn't get port info\n");
        return NULL;
    }
    
    rdma_metadata = (char **)calloc(config->remote_count, sizeof(char *));
    for (i=0; i < config->remote_count; i++) {
        (*(config->local_endpoint + i))->lid = config->rdma_ctx->portinfo.lid;
        if (config->rdma_ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET && !(*(config->local_endpoint + i))->lid) {
            fprintf(stderr, "rdma_prepare: Couldn't get local LID\n");
            return NULL;
        }
        
        if (config->gidx >= 0) {
            if (ibv_query_gid(config->rdma_ctx->context, 1, config->gidx, &(*(config->local_endpoint + i))->gid)) {
            fprintf(stderr, "rdma_prepare: Can't read sgid of index %d\n", config->gidx);
            return NULL;
            }
        } else {
            memset(&(*(config->local_endpoint + i))->gid, 0, sizeof((*(config->local_endpoint + i))->gid));
        }

        inet_ntop(AF_INET6, &(*(config->local_endpoint + i))->gid, (*(config->local_endpoint + i))->gid_string, sizeof((*(config->local_endpoint + i))->gid_string));
        gid_to_wire_gid(&(*(config->local_endpoint + i))->gid, (*(config->local_endpoint + i))->gid_string);

        if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
            srand(time(NULL));
        } else {
            srand((int)now.tv_nsec);
        }
        (*(config->local_endpoint + i))->qpn = (*(config->rdma_ctx->qp + i))->qp_num;
        (*(config->local_endpoint + i))->psn = rand() & 0xffffff;
        // (*(config->local_endpoint + i))->psn = (rand() & 0xffffff) + i;
        // (*(config->local_endpoint + i))->psn = (rand() & 0xffffff) + i * 10000;

        if (config->function == RDMA_WRITE && role == RDMA_RECEIVER) {
            (*(config->local_endpoint + i))->rkey = (*(config->rdma_ctx->mr + i))->rkey;
            (*(config->local_endpoint + i))->addr = (uint64_t)(*(config->rdma_ctx->mr + i))->addr;
            *(rdma_metadata + i) = (char *)malloc(78); // 4+1+6+1+6+1+8+1+16+1+32+1 (last one is the string terminator)
            memset(*(rdma_metadata + i), 0, 78);
            snprintf(*(rdma_metadata + i), 78, "%04x:%06x:%06x:%08x:%016lx:%s", (*(config->local_endpoint + i))->lid, (*(config->local_endpoint + i))->qpn, (*(config->local_endpoint + i))->psn, (*(config->local_endpoint + i))->rkey, (*(config->local_endpoint + i))->addr, (*(config->local_endpoint + i))->gid_string);
            debug_print("(RDMA_WRITE) local RDMA metadata for remote #%d: %s\n", i, *(rdma_metadata + i));
        } else {
            *(rdma_metadata + i) = (char *)malloc(52); // 4+1+6+1+6+1++32+1 (last one is the string terminator)
            memset(*(rdma_metadata + i), 0, 52);
            snprintf(*(rdma_metadata + i), 52, "%04x:%06x:%06x:%s", (*(config->local_endpoint + i))->lid, (*(config->local_endpoint + i))->qpn, (*(config->local_endpoint + i))->psn, (*(config->local_endpoint + i))->gid_string);
            debug_print("(RDMA_SEND) local RDMA metadata for remote #%d: %s\n", i, *(rdma_metadata + i));
        }
    }

    return(rdma_metadata);
}

struct rdma_context *
rdma_init_ctx(struct ibv_device *ib_dev, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned count, int port, int role)
{
    struct rdma_context *ctx;
    int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    int i, j;
    
    ctx = (struct rdma_context *)calloc(1, sizeof(*ctx));
    if (!ctx) {
        fprintf(stderr, "rdma_init_ctx: Failed to create RDMA context.\n");
        return NULL;
    }

    ctx->size = (unsigned long *)calloc(count, sizeof(unsigned long));
    if (!ctx->size) {
        fprintf(stderr, "rdma_init_ctx: Failed to create work buffer size(s).\n");
        return NULL;
    }
    for (i = 0; i < count; i++) {
        if (role == RDMA_SENDER) {
            printf("message_count: %d, message_size: %d\n", *(message_count + i), *(message_size + i));
            *(ctx->size + i) = *(message_count + i) * *(message_size + i);
        } else {
            printf("buffer_size: %d\n", *(buffer_size + i));
            *(ctx->size + i) = *(buffer_size + i);
        }
    }
    ctx->send_flags = IBV_SEND_SIGNALED;

    ctx->buf = (char **)malloc(count * sizeof(char *));
    if (!ctx->buf) {
        fprintf(stderr, "rdma_init_ctx: Couldn't allocate array of work buffers.\n");
        goto clean_ctx;
    }
    printf("rdma_init_ctx 1: buffer addr: %d\n", ctx->buf);
    for (i = 0; i < count; i++) {
        *(ctx->buf + i) = (char *)malloc(*(ctx->size + i));
        if (!*(ctx->buf + i)) {
            fprintf(stderr, "rdma_init_ctx: Couldn't allocate work buffer #%d out of %d.\n", i + 1, count);
            goto clean_ctx;
        } else {
            bzero(*(ctx->buf + i), *(ctx->size + i));
        }
    }

    for (i = 0; i < count; i++) {
        if (role == RDMA_SENDER) {
            for (j = 0; j < *(message_count + i); j++) {
                memset(*(ctx->buf + i) + j * *(message_size + i), j+43+i*(*(message_count + i)), *(message_size + i));
            }
        } else {
            memset(*(ctx->buf + i), 0x7b, *(ctx->size + i));
        }
    }
    // int k;
    // printf("ININTIALIZATION: Buffer data:\n");
    // for (k = 0; k < count; k++) {
    //     printf("ININTIALIZATION: client #%d:\n", k + 1);
    //     for (i = 0; i < *(message_count + k); i++) {
    //         for (j = 0; j < *(message_size + k); j++) {
    //             printf("%d:", *(*(ctx->buf + k) + i * *(message_size + k) + j));
    //         }
    //     }
    //     printf("\n");
    // }
    // printf("DONE\n");

    // Let the games begin!
    // open device
    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context) {
        fprintf(stderr, "rdma_init_ctx: Couldn't get context for %s\n", ibv_get_device_name(ib_dev));
        goto clean_buffer;
    }
    
    // create PD (Protection Domani) - a single one
    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd) {
        fprintf(stderr, "rdma_init_ctx: Couldn't allocate PD\n");
    }
    
    // create MR (Memory Region) - one for each client
    ctx->mr = (struct ibv_mr **)calloc(count, sizeof(struct ibv_mr *));
    if (!ctx->mr) {
        fprintf(stderr, "rdma_init_ctx: Couldn't allocate MR array\n");
        goto clean_pd;
    }
    printf("rdma_init_ctx 2: buffer addr: %d\n", ctx->buf);
    printf("rdma_init_ctx 3: buffer addr: %d\n", *ctx->buf);
    for (i = 0; i < count; i++) {
        *(ctx->mr + i) = ibv_reg_mr(ctx->pd, *(ctx->buf + i), *(ctx->size + i), access_flags);
        fprintf(stderr, "rdma_init_ctx: MR addr: %d\n", (*(ctx->mr + i))->addr);
        fprintf(stderr, "rdma_init_ctx: buffer addr: %d\n", *(ctx->buf + i));
        if (!*(ctx->mr + i)) {
            fprintf(stderr, "rdma_init_ctx: Couldn't register MR #%d out of %d.\n", i + 1, count);
            goto clean_mr;
        }
    }
    
    // create CQ (Completion Queue) - one for each client
    ctx->cq = (struct ibv_cq **)calloc(count, sizeof(struct ibv_cq *));
    if (!ctx->cq) {
        fprintf(stderr, "rdma_init_ctx: Couldn't allocate CQ array\n");
        goto clean_mr;
    }
    for (i = 0; i < count; i++) {
        *(ctx->cq + i) = ibv_create_cq(ctx->context, RDMA_MAX_SEND_WR, NULL, NULL, 0);
        if (!*(ctx->cq + i)) {
            fprintf(stderr, "rdma_init_ctx: Couldn't create CQ #%d out of %d.\n", i + 1, count);
            goto clean_cq;
        }
    }

    // create QP (Queue Pair) - one for each client
    ctx->qp = (struct ibv_qp **)calloc(count, sizeof(struct ibv_qp *));
    if (!ctx->qp) {
        fprintf(stderr, "rdma_init_ctx: Couldn't allocate QP array\n");
        goto clean_cq;
    }
    for (i = 0; i < count; i++) {
        {
            struct ibv_qp_init_attr init_attr = {
                .send_cq = *(ctx->cq + i),
                .recv_cq = *(ctx->cq + i),
                .cap     = {
                    .max_send_wr  = RDMA_MAX_SEND_WR,
                    .max_recv_wr  = RDMA_MAX_RECV_WR,
                    .max_send_sge = 1,
                    .max_recv_sge = 1
                },
                .sq_sig_all = 1,
                .qp_type = IBV_QPT_RC
            };
            
            *(ctx->qp + i) = ibv_create_qp(ctx->pd, &init_attr);
            if (!*(ctx->qp + i))  {
                fprintf(stderr, "rdma_init_ctx: Couldn't create QP #%d out of %d.\n", i + 1, count);
                goto clean_qp;
            }
        }

        {
            struct ibv_qp_attr attr = {
                .qp_state        = IBV_QPS_INIT,
                .pkey_index      = 0,
                .port_num        = port,
                .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
            };
            
            if (ibv_modify_qp(*(ctx->qp + i), &attr,
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

    return ctx;

    clean_qp:
    for (i = 0; i < count; i++) {
        if (*(ctx->qp + i)) {
            ibv_destroy_qp(*(ctx->qp + i));
        }
    }
    free(ctx->qp);

    clean_cq:
    for (i = 0; i < count; i++) {
        if (*(ctx->cq + i)) {
            ibv_destroy_cq(*(ctx->cq + i));
        }
    }
    free(ctx->cq);

    clean_mr:
    for (i = 0; i < count; i++) {
        if (*(ctx->mr + i)) {
            ibv_dereg_mr(*(ctx->mr + i));
        }
    }
    free(ctx->mr);

    clean_pd:
    ibv_dealloc_pd(ctx->pd);

    clean_buffer:
    for (i = 0; i < count; i++) {
        if (*(ctx->buf + i)) {
            free(*(ctx->buf + i));
        }
    }
    free(ctx->buf);

    clean_ctx:
    free(ctx);

    return NULL;
}

int
rdma_connect_ctx(struct rdma_context *ctx, int port, enum ibv_mtu mtu, struct rdma_endpoint **local_endpoint, struct rdma_endpoint **remote_endpoint, unsigned count, int sgid_idx, int role)
{
    int i;

    for (i = 0; i < count; i++) {
        struct ibv_qp_attr attr = {
            .qp_state           = IBV_QPS_RTR,
            .path_mtu           = mtu,
            .dest_qp_num        = (*(remote_endpoint + i))->qpn,
            .rq_psn             = (*(remote_endpoint + i))->psn,
            .max_dest_rd_atomic	= 1,
            .min_rnr_timer      = 12,
            .ah_attr			= {
                .is_global      = 0,
                .dlid           = (*(remote_endpoint + i))->lid,
                .sl             = 0,
                .src_path_bits  = 0,
                .port_num       = port
            }
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
                // printf("addr: %016lx - ", (*(remote_endpoint + k))->addr);
                (wr + i)->wr.rdma.rkey = (*(remote_endpoint + k))->rkey;
                // printf("rkey: %08x - ", (*(remote_endpoint + k))->rkey);
                // printf("psn: %0lx - ", (*(remote_endpoint + k))->psn);
                // printf("qpn: %0lx - ", (*(remote_endpoint + k))->qpn);
                // printf("lid: %0lx - ", (*(remote_endpoint + k))->lid);

                (list + i)->length = *(message_size + k);
                (list + i)->addr = (uint64_t)(*(ctx->buf + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k));
                // printf("buf: %d\n", *(*(ctx->buf + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k)));
                (list + i)->lkey = (*(ctx->mr + k))->lkey;

                if (ibv_post_send(*(ctx->qp + k), wr + i, bad_wr + i)) {
                    fprintf(stderr, "rdma_post_send: Couldn't post send #%d\n", i);
                    break;
                } else {
                    total++;
                }
                // printf("DONE POST SEND %d\n", i);
            }
            // printf("\nDONE PREPARE #%d, last loop\n", k + 1);

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
    // int sval;
    char buf[32];
    // struct linked_list_node *temp;

    struct rdma_notification_thread_param *notification_thread_args = (struct rdma_notification_thread_param *)arg;

    while(1) {
        sem_wait(notification_thread_args->sem_send_data);

        // sem_getvalue(notification_thread_args->sem_send_data, &sval);

        // debug_print("(sender_notification_thread) semaphore value: %d\n", sval);

        // if (notification_thread_args->first) {
            bzero(buf, 32);
            sprintf(buf, "%u", notification_thread_args->sent_size);
            // sprintf(buf, "%u", notification_thread_args->first->sent_size);
            // debug_print("(sender_notification_thread) sending %u bytes\n", notification_thread_args->first->sent_size);
            write(notification_thread_args->sender_thread_socket, buf, 32);
            // debug_print("(sender_notification_thread) notification sent\n");

            // pthread_mutex_lock(notification_thread_args->notification_mutex);
            // if (notification_thread_args->first->next == NULL) {
            //     notification_thread_args->first = notification_thread_args->last =  NULL;
            // } else {
            //     temp = notification_thread_args->first;
            //     notification_thread_args->first = notification_thread_args->first->next;
            // }
            // free(temp);
            // pthread_mutex_unlock(notification_thread_args->notification_mutex);
        // } else {
            // debug_print("(sender_notification_thread) no data in queue\n");
        // }
    }
}

void *
sender_control_thread(void *arg)
{
    char buf[32];
    long int timestamp_ns;
    long int timestamp_ms;
    char *buffer_token;

    struct rdma_backpressure_thread_param *backpressure_thread_args = (struct rdma_backpressure_thread_param *)arg;

    while(1) {
        bzero(buf, 32);
        read(backpressure_thread_args->sender_thread_socket, buf, 32);

        buffer_token = strtok(buf, ":");
        timestamp_ns = get_current_timestamp_ns() - backpressure_thread_args->start_ts;
        timestamp_ms = timestamp_ns / 1E6;

        // debug_print("(sender_control_thread) received: >>>%s<<<\n", buf);
        if (strcmp(buffer_token, "WAIT") == 0) {
            backpressure_thread_args->backpressure = 1;
            buffer_token = strtok(NULL, ":");
            printf("b1:%d:%ld:%ld:%d:%s\n", backpressure_thread_args->client_id, timestamp_ns, timestamp_ms, 0, buffer_token);
        } else if (strcmp(buffer_token, "GO") == 0) {
            backpressure_thread_args->backpressure = 0;
            buffer_token = strtok(NULL, ":");
            printf("b1:%d:%ld:%ld:%d:%s\n", backpressure_thread_args->client_id, timestamp_ns, timestamp_ms, 1, buffer_token);
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
    long int timestamp_ms;
    unsigned long chunk_size;
    // unsigned long chunk_count = 0;
    // unsigned long chunk_limit = 10;

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
        timestamp_ns = get_current_timestamp_ns() - thread_args->start_ts;
        timestamp_ms = timestamp_ns / 1E6;

        if (!backpressure_thread_args->backpressure) {
            timestamp_ns_thread_cpu_start = get_current_timestamp_ns_thread_cpu();

            total = 0;
            num_cq_events = 0;
            for (j = 0; j < full_queue_count; j++) {
                wr = (struct ibv_send_wr *)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_send_wr));
                bad_wr = (struct ibv_send_wr **)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_send_wr *));
                list = (struct ibv_sge *)malloc(RDMA_MAX_SEND_WR * sizeof(struct ibv_sge));

                // printf("PREPARE: client #%d:\n", thread_args->ctx_index + 1);
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
                        // debug_print("DST (client %d, loop %d, step %d) buffer addr %d\n", thread_args->ctx_index + 1, j, i, thread_args->remote_endpoint->addr);
                        // debug_print("DST (client %d, loop %d, step %d) buffer chunk offset %d\n", thread_args->ctx_index + 1, j, i, thread_args->mem_offset);
                        // debug_print("DST (client %d, loop %d, step %d) buffer buffer offset %d\n", thread_args->ctx_index + 1, j, i, (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                        // debug_print("DST (client %d, loop %d, step %d) buffer addr + chunk + buffer %d %d %d %d\n", thread_args->ctx_index + 1, j, i, thread_args->remote_endpoint->addr + thread_args->mem_offset + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size, thread_args->remote_endpoint->addr, thread_args->mem_offset, (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                        (wr + i)->wr.rdma.remote_addr = thread_args->remote_endpoint->addr + thread_args->mem_offset + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size;
                        (wr + i)->wr.rdma.rkey = thread_args->remote_endpoint->rkey;

                        (list + i)->length = thread_args->message_size;
                        // debug_print("SRC (client %d, loop %d, step %d) buffer addr %d\n", thread_args->ctx_index + 1, j, i, *(thread_args->rdma_ctx->buf + thread_args->ctx_index));
                        // debug_print("SRC (client %d, loop %d, step %d) buffer offset %d\n", thread_args->ctx_index + 1, j, i, (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                        // debug_print("SRC (client %d, loop %d, step %d) buffer addr + offset %d %d %d\n", thread_args->ctx_index + 1, j, i, *(thread_args->rdma_ctx->buf + thread_args->ctx_index) + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size, *(thread_args->rdma_ctx->buf + thread_args->ctx_index), (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                        (list + i)->addr = (uint64_t)(*(thread_args->rdma_ctx->buf + thread_args->ctx_index) + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                        (list + i)->lkey = (*(thread_args->rdma_ctx->mr + thread_args->ctx_index))->lkey;

                        // if (ibv_post_send(*(thread_args->rdma_ctx->qp + thread_args->ctx_index), wr + i, bad_wr + i)) {
                        //     fprintf(stderr, "rdma_post_send_mt: Couldn't post send #%d\n", i);
                        //     break;
                        // } else {
                        //     total++;
                        // }
                    }
                    if (ibv_post_send(*(thread_args->rdma_ctx->qp + thread_args->ctx_index), wr, bad_wr)) {
                        fprintf(stderr, "rdma_post_send_mt: Couldn't post sends, client #%d\n", thread_args->client_id);
                    //     break;
                    // } else {
                    //     total++;
                    }
                    // printf("\nDONE PREPARE #%d, loop %d\n", thread_args->ctx_index + 1, j);

                    // debug_print("(client %d, loop %d) written %d posts\n", thread_args->ctx_index + 1, j, i);
                    // if (i < RDMA_MAX_SEND_WR) {
                    //     debug_print("(client %d, loop %d) missing %d writes\n", thread_args->ctx_index + 1, j, RDMA_MAX_SEND_WR - i);
                    //     done = 0;
                    //     last = i;
                    // } else {
                    //     // debug_print("(client %d, loop %d) all writes done\n", thread_args->ctx_index + 1, j);
                        done = 1;
                    // }

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
                    // debug_print("DST (client %d, final loop, step %d) buffer addr %d\n", thread_args->ctx_index + 1, i, thread_args->remote_endpoint->addr);
                    // debug_print("DST (client %d, final loop, step %d) buffer chunk offset %d\n", thread_args->ctx_index + 1, i, thread_args->mem_offset);
                    // debug_print("DST (client %d, final loop, step %d) buffer buffer offset %d\n", thread_args->ctx_index + 1, i, (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                    // debug_print("DST (client %d, final loop, step %d) buffer addr + chunk + buffer %d %d %d %d\n", thread_args->ctx_index + 1, i, thread_args->remote_endpoint->addr + thread_args->mem_offset + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size, thread_args->remote_endpoint->addr, thread_args->mem_offset, (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                    (wr + i)->wr.rdma.remote_addr = thread_args->remote_endpoint->addr + thread_args->mem_offset + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size;
                    // printf("addr: %016lx - ", thread_args->remote_endpoint->addr);
                    (wr + i)->wr.rdma.rkey = thread_args->remote_endpoint->rkey;
                    // printf("rkey: %08x - ", thread_args->remote_endpoint->rkey);
                    // printf("psn: %0lx - ", thread_args->remote_endpoint->psn);
                    // printf("qpn: %0lx - ", thread_args->remote_endpoint->qpn);
                    // printf("lid: %0lx - ", thread_args->remote_endpoint->lid);

                    (list + i)->length = thread_args->message_size;
                    // debug_print("SRC (client %d, final loop, step %d) buffer addr %d\n", thread_args->ctx_index + 1, i, *(thread_args->rdma_ctx->buf + thread_args->ctx_index));
                    // debug_print("SRC (client %d, final loop, step %d) buffer offset %d\n", thread_args->ctx_index + 1, i, (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                    // debug_print("SRC (client %d, final loop, step %d) buffer addr + offset %d %d %d\n", thread_args->ctx_index + 1, i, *(thread_args->rdma_ctx->buf + thread_args->ctx_index) + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size, *(thread_args->rdma_ctx->buf + thread_args->ctx_index), (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                    (list + i)->addr = (uint64_t)(*(thread_args->rdma_ctx->buf + thread_args->ctx_index) + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size);
                    // printf("buf: %d\n", *(*(thread_args->rdma_ctx->buf + k) + (j * RDMA_MAX_SEND_WR + i) * thread_args->message_size));
                    (list + i)->lkey = (*(thread_args->rdma_ctx->mr + thread_args->ctx_index))->lkey;

                    // if (ibv_post_send(*(thread_args->rdma_ctx->qp + thread_args->ctx_index), wr + i, bad_wr + i)) {
                    //     fprintf(stderr, "rdma_post_send_mt: Couldn't post send #%d\n", i);
                    //     break;
                    // } else {
                    //     total++;
                    // }
                    // printf("DONE POST SEND %d\n", i);
                }
                if (ibv_post_send(*(thread_args->rdma_ctx->qp + thread_args->ctx_index), wr, bad_wr)) {
                    fprintf(stderr, "rdma_post_send_mt: Couldn't post sends, client #%d\n", thread_args->client_id);
                //     break;
                // } else {
                //     total++;
                }
                // printf("\nDONE PREPARE #%d, last loop\n", thread_args->ctx_index + 1);

                // debug_print("(client %d, final loop) written %d posts\n", thread_args->ctx_index + 1, i);
                // if (i < remainder_queue_size) {
                //     debug_print("(client %d, final loop) missing %d writes\n", thread_args->ctx_index + 1, remainder_queue_size - i);
                //     done = 0;
                //     last = i;
                // } else {
                //     // debug_print("(client %d, final loop) all writes done\n", thread_args->ctx_index + 1);
                    done = 1;
                // }

                left = remainder_queue_size;
                do {
                    // sleep(1);
                    ne = ibv_poll_cq(*(thread_args->rdma_ctx->cq + thread_args->ctx_index), left, wc);
                    if (ne < 0) {
                        debug_print("(client %d, final loop) poll CQ failed %d, client #%d\n", thread_args->ctx_index + 1, ne, thread_args->client_id);
                    } else {
                        // for (l=0; l<ne; l++) {
                        //     if ((wc + l)->status != IBV_WC_SUCCESS) {
                        //         debug_print("(RDMA_SEND_MT_STREAM) ibv_poll_cq failed status %s (%d) for wr_id %d\n", ibv_wc_status_str((wc + l)->status), (wc + l)->status, (int)((wc + l)->wr_id));
                        //     } else {
                        //         debug_print("(RDMA_SEND_MT_STREAM) ibv_poll_cq success status for wr_id %d\n", (int)((wc + l)->wr_id));
                        //     }
                        // }
                        left -= ne;
                        // debug_print("(client %d, final loop) ne=%d, left=%d\n", thread_args->ctx_index + 1, ne, left);
                    }
                } while (left > 0);
                // debug_print("(client %d, final loop) all ACK\n", thread_args->ctx_index + 1);
            }

            free(wr);
            free(bad_wr);
            free(list);

            timestamp_ns_thread_cpu_now = get_current_timestamp_ns_thread_cpu();

            printf("t1:%d:%ld:%ld:%lu\n", thread_args->client_id, timestamp_ns, timestamp_ns_thread_cpu_now - timestamp_ns_thread_cpu_start, chunk_size);
            // debug_print("t1:%d:%ld:%ld:%lu\n", thread_args->client_id, timestamp_ns, timestamp_ns_thread_cpu_now - timestamp_ns_thread_cpu_start, chunk_size);
            debug_print("t1:%d:%ld\n", thread_args->client_id, timestamp_ns);

            if (!thread_args->stream) {
                break;
            }

            thread_args->mem_offset += chunk_size;
            thread_args->mem_offset %= thread_args->buffer_size;
            // debug_print("(RDMA_SEND_MT_STREAM) buffer size: %d\n", thread_args->buffer_size);
            // debug_print("(RDMA_SEND_MT_STREAM) current mem_offset: %d\n", thread_args->mem_offset);

            // struct linked_list_node *new_node = (struct linked_list_node *)malloc(sizeof(struct linked_list_node));
            // new_node->sent_size = chunk_size;
            // new_node->next = NULL;
            // // pthread_mutex_lock(notification_thread_args->notification_mutex);
            // if (notification_thread_args->last == NULL) {
            //     // debug_print("(RDMA_SEND_MT_STREAM) 1\n");
            //     notification_thread_args->first = notification_thread_args->last =  new_node;
            // } else {
            //     if (notification_thread_args->first == notification_thread_args->last) {
            //         // debug_print("(RDMA_SEND_MT_STREAM) 2\n");
            //         notification_thread_args->first->next = new_node;
            //         notification_thread_args->last = new_node;
            //     } else {
            //         // debug_print("(RDMA_SEND_MT_STREAM) 3\n");
            //         notification_thread_args->last->next = new_node;
            //         notification_thread_args->last = new_node;
            //     }
            // }
            // pthread_mutex_unlock(notification_thread_args->notification_mutex);
            if (thread_args->stream) {
                sem_post(notification_thread_args->sem_send_data);
            }

            // try #1
            // bzero(buf, 32);
            // sprintf(buf, "%d", chunk_size);
            // write(backpressure_thread_args->sender_thread_socket, buf, 32);
            // try #2
            // chunk_count++;
            // if (chunk_count == chunk_limit) {
            //     bzero(buf, 32);
            //     sprintf(buf, "%d", chunk_size * chunk_limit);
            //     write(backpressure_thread_args->sender_thread_socket, buf, 32);
            //     chunk_count = 0;
            // }
        } else {
            // debug_print("(RDMA_SEND_MT_STREAM) backpressure\n");
            printf("s1:%d:%ld:%ld\n", thread_args->client_id, timestamp_ns,  timestamp_ms);
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
receiver_work_thread(void *arg)
{
    char buf[32];
    char *devnull;
    unsigned long chunk_size, work_size, work_start_offset, work_end_offset;
    unsigned int new_mem_offset_total;
    unsigned int new_mem_offset_circular;
    long int timestamp_ns;
    long int crt = 0;
    unsigned local_worker_id;

    // // debug received data
    // FILE *f;
    // char *filename;

    struct rdma_thread_param *thread_args = (struct rdma_thread_param *)arg;

	pthread_mutex_lock(&(thread_args->cond_lock));
    local_worker_id = thread_args->worker_id++;
	pthread_mutex_unlock(&(thread_args->cond_lock));

    devnull = (char *)malloc(thread_args->message_count * thread_args->message_size);
    bzero(devnull, thread_args->message_count * thread_args->message_size);

    while (1) {
        timestamp_ns = get_current_timestamp_ns() - thread_args->start_ts;
        // printf("d1[%ld]:%ld:%ld\n", local_worker_id, timestamp_ns, (timestamp_ns / 1000000));

		pthread_barrier_wait(&(thread_args->workers_done_barrier));

        // printf("db1[%ld]\n", local_worker_id);
        // printf("db1[%ld]:%ld:%ld\n", local_worker_id, timestamp_ns, (timestamp_ns / 1000000));

		pthread_mutex_lock(&(thread_args->cond_lock));
		while(thread_args->mem_offset_produce == thread_args->mem_offset_consume) {
			pthread_cond_wait(&(thread_args->start_work), &(thread_args->cond_lock));
        }

        // printf("dw[%ld]\n", local_worker_id);
        // printf("dw1[%ld]:%ld:%ld\n", local_worker_id, timestamp_ns, (timestamp_ns / 1000000));

		// if(thread_args->got_work == READY){
		// 	pthread_mutex_unlock(&(thread_args->cond_lock));
		// 	return NULL; //finish thread	
		// }

        chunk_size = thread_args->received_size;
        new_mem_offset_total = thread_args->mem_offset_consume + chunk_size;
        new_mem_offset_circular = new_mem_offset_total % thread_args->buffer_size;
		pthread_mutex_unlock(&(thread_args->cond_lock));

        // work being done
        // devnull = (char *)malloc(thread_args->received_size);
        // bzero(devnull, thread_args->received_size);

        work_size = chunk_size / thread_args->worker_count;
        work_start_offset = local_worker_id * work_size;
        work_end_offset = (local_worker_id + 1) * work_size;
        if (local_worker_id == thread_args->worker_count - 1) {
            if (work_end_offset < chunk_size) {
                work_end_offset = chunk_size;
                work_size = work_end_offset - work_start_offset;
            }
        }

        // memcpy(devnull, (*thread_args->rdma_ctx->buf) + thread_args->mem_offset_consume + work_start_offset, work_size);

        // printf("d5[%ld]:%ld:%ld:%lu\n", local_worker_id, timestamp_ns, (timestamp_ns / 1000000), work_size);

        // filename = malloc(15);
        // bzero(filename, 15);
        // sprintf(filename, "MSG%07d.log", crt);

        // f = fopen(filename, "w");
        
        // fprintf(f, "%s", devnull);

        // free(filename);
        // fclose(f);

        // free(devnull);
        // work done

		pthread_barrier_wait(&(thread_args->workers_done_barrier));

        // printf("db2[%ld]\n", local_worker_id);
        // printf("db2[%ld]:%ld:%ld\n", local_worker_id, timestamp_ns, (timestamp_ns / 1000000));

        if (local_worker_id == 0) {
            unsigned long used_size;

            pthread_mutex_lock(&(thread_args->cond_lock));
            // thread_args->got_work = 0;
            thread_args->mem_offset_consume = new_mem_offset_circular;
            thread_args->used_size -= chunk_size;
            if (thread_args->mem_offset_produce >= thread_args->mem_offset_consume) {
                used_size = thread_args->mem_offset_produce - thread_args->mem_offset_consume;
            } else {
                used_size = thread_args->buffer_size + thread_args->mem_offset_produce - thread_args->mem_offset_consume;
            }
            pthread_mutex_unlock(&(thread_args->cond_lock));

            // printf("receiver_work_thread: new used size: %d\n", thread_args->used_size);
            printf("d3:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), used_size);

            bzero(buf, 32);
            sprintf(buf, "GO:%ld", crt);
            // pthread_mutex_lock(thread_args->backpressure_mutex);
            if (thread_args->backpressure == 1 && used_size < (thread_args->buffer_size * thread_args->backpressure_threshold_down / 100)) {
                write(thread_args->control_socket, buf, 32);
                thread_args->backpressure = 0;
                // printf("receiver_work_thread: backpressure change: GO\n");
                printf("d4:%ld:%ld:%d:%ld\n", timestamp_ns, (timestamp_ns / 1000000), 1, crt++);
            } else if (thread_args->backpressure == 0) {
                // printf("receiver_work_thread: backpressure already on: GO\n");
                printf("d4:%ld:%ld:%d\n", timestamp_ns, (timestamp_ns / 1000000), 0);
            }
            // pthread_mutex_unlock(thread_args->backpressure_mutex);
        }
    }

    free(devnull);
}

void *
receiver_work_thread_tstream(void *arg)
{
    char buf[32];
    char *devnull;
    unsigned long chunk_size, work_size, work_start_offset, work_end_offset;
    unsigned int new_mem_offset_total;
    unsigned int new_mem_offset_circular;
    long int timestamp_ns;
    long int crt = 0;
    unsigned local_worker_id;

    // debug received data
    FILE *f;
    char *filename;

    struct rdma_thread_param *thread_args = (struct rdma_thread_param *)arg;

	pthread_mutex_lock(&(thread_args->cond_lock));
    local_worker_id = thread_args->worker_id++;
	pthread_mutex_unlock(&(thread_args->cond_lock));

    // devnull = (char *)malloc(thread_args->message_count * thread_args->message_size);
    // bzero(devnull, thread_args->message_count * thread_args->message_size);

    while (1) {
        timestamp_ns = get_current_timestamp_ns() - thread_args->start_ts;
        printf("d1[%ld]:%ld:%ld\n", local_worker_id, timestamp_ns, (timestamp_ns / 1000000));

		pthread_barrier_wait(&(thread_args->workers_done_barrier));

        printf("db1[%ld]\n", local_worker_id);

		pthread_mutex_lock(&(thread_args->cond_lock));
		while(thread_args->mem_offset_produce == thread_args->mem_offset_consume) {
			pthread_cond_wait(&(thread_args->start_work), &(thread_args->cond_lock));
        }

        printf("dw[%ld]\n", local_worker_id);

		// if(thread_args->got_work == READY){
		// 	pthread_mutex_unlock(&(thread_args->cond_lock));
		// 	return NULL; //finish thread	
		// }

        chunk_size = thread_args->received_size * thread_args->message_size;
        new_mem_offset_total = thread_args->mem_offset_consume + chunk_size;
        new_mem_offset_circular = new_mem_offset_total % thread_args->buffer_size;
		pthread_mutex_unlock(&(thread_args->cond_lock));

        // work starts being done
        work_size = chunk_size / thread_args->worker_count;
        work_start_offset = local_worker_id * work_size;
        work_end_offset = (local_worker_id + 1) * work_size;
        if (local_worker_id == thread_args->worker_count - 1) {
            if (work_end_offset < chunk_size) {
                work_end_offset = chunk_size;
                work_size = work_end_offset - work_start_offset;
            }
        }

        devnull = (char *)malloc(work_size);
        bzero(devnull, work_size);
        memcpy(devnull, (*thread_args->rdma_ctx->buf) + thread_args->mem_offset_consume + work_start_offset, work_size);

        printf("d5[%ld]:%ld:%ld:%lu\n", local_worker_id, timestamp_ns, (timestamp_ns / 1000000), work_size);

        filename = malloc(15);
        bzero(filename, 15);
        sprintf(filename, "MSG%07d.log", crt);

        f = fopen(filename, "w");
        
        fprintf(f, "%s", devnull);

        free(filename);
        fclose(f);

        free(devnull);
        // work done

		pthread_barrier_wait(&(thread_args->workers_done_barrier));

        printf("db2[%ld]\n", local_worker_id);

        if (local_worker_id == 0) {
            unsigned long used_size;

            pthread_mutex_lock(&(thread_args->cond_lock));
            // thread_args->got_work = 0;
            thread_args->mem_offset_consume = new_mem_offset_circular;
            thread_args->used_size -= chunk_size;
            if (thread_args->mem_offset_produce >= thread_args->mem_offset_consume) {
                used_size = thread_args->mem_offset_produce - thread_args->mem_offset_consume;
            } else {
                used_size = thread_args->buffer_size + thread_args->mem_offset_produce - thread_args->mem_offset_consume;
            }
            pthread_mutex_unlock(&(thread_args->cond_lock));

            // printf("receiver_work_thread: new used size: %d\n", thread_args->used_size);
            printf("d3:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), used_size);

            bzero(buf, 32);
            sprintf(buf, "GO:%ld", crt);
            // pthread_mutex_lock(thread_args->backpressure_mutex);
            if (thread_args->backpressure == 1 && used_size < (thread_args->buffer_size * thread_args->backpressure_threshold_down / 100)) {
                write(thread_args->control_socket, buf, 32);
                thread_args->backpressure = 0;
                // printf("receiver_work_thread: backpressure change: GO\n");
                printf("d4:%ld:%ld:%d:%ld\n", timestamp_ns, (timestamp_ns / 1000000), 1, crt++);
            } else if (thread_args->backpressure == 0) {
                // printf("receiver_work_thread: backpressure already on: GO\n");
                printf("d4:%ld:%ld:%d\n", timestamp_ns, (timestamp_ns / 1000000), 0);
            }
            // pthread_mutex_unlock(thread_args->backpressure_mutex);
        }
    }

    free(devnull);
}

// void *
// receiver_data_thread(void *arg)
// {
//     int i, sval;
//     char buf[32];
//     char *devnull;
//     unsigned long chunk_size;
//     unsigned int new_mem_offset_total;
//     unsigned int new_mem_offset_circular;
//     long int timestamp_ns;
//     long int crt = 0;

//     struct rdma_thread_param *thread_args = (struct rdma_thread_param *)arg;

//     while (1) {
//         // BLOCK

//         timestamp_ns = get_current_timestamp_ns() - thread_args->start_ts;

//         // printf("receiver_data_thread: semaphore value: %d\n", sval);
//         printf("d1:%ld:%ld:%d\n", timestamp_ns, (timestamp_ns / 1000000), sval);

//         // printf("receiver_data_thread: used size: %d\n", thread_args->used_size);
//         printf("d2:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), thread_args->used_size);

//         // Print data in the reserved memory when the sem_recv_data semaphore unlocks, meaning that data has been received
//         devnull = (char *)malloc(thread_args->received_size);
//         chunk_size = thread_args->received_size;
//         // chunk_size = thread_args->message_count * thread_args->message_size;
//         // if (chunk_size > thread_args->used_size) {
//         //     chunk_size = thread_args->used_size;
//         // }

//         // printf("receiver_data_thread: last transfer - received %d bytes\n", thread_args->received_size);
//         new_mem_offset_total = thread_args->mem_offset + chunk_size;
//         new_mem_offset_circular = new_mem_offset_total % thread_args->buffer_size;
//         // printf("receiver_data_thread: buffer addr: %d\n", (char *)(*thread_args->rdma_ctx->buf));
//         // printf("receiver_data_thread: buffer offset: %d\n", thread_args->mem_offset);
//         // printf("receiver_data_thread: buffer addr + offset: %d %d %d\n", (char *)(*thread_args->rdma_ctx->buf) + thread_args->mem_offset, (char *)(*thread_args->rdma_ctx->buf), thread_args->mem_offset);
//         // if (new_mem_offset_total > thread_args->buffer_size) {
//         //     // printf("taped together\n");
//         //     memcpy(devnull, (char *)(*thread_args->rdma_ctx->buf) + thread_args->mem_offset, chunk_size - new_mem_offset_circular);
//         //     // for (i = 0; i < chunk_size - new_mem_offset_circular; i++) {
//         //     //     printf("%d:", *(devnull + i));
//         //     // }
//         //     memcpy(devnull, (char *)(*thread_args->rdma_ctx->buf), new_mem_offset_circular);
//         //     // for (i = 0; i < new_mem_offset_circular; i++) {
//         //     //     printf("%d:", *(devnull + i));
//         //     // }
//         //     // printf("\n");
//         // } else {
//         //     // printf("single chunk\n");
//         //     memcpy(devnull, (char *)(*thread_args->rdma_ctx->buf) + thread_args->mem_offset, chunk_size);
//         //     // for (i = 0; i < chunk_size; i++) {
//         //     //     printf("%d:", *(devnull + i));
//         //     // }
//         //     // printf("\n");
//         // }
//         thread_args->mem_offset = new_mem_offset_circular;
//         thread_args->used_size -= chunk_size;
//         // printf("\nreceiver_data_thread: finished processing %d bytes chunk\n", chunk_size);

//         // usleep(50);

//         free(devnull);
//         // End of data check

//         // printf("receiver_data_thread: new used size: %d\n", thread_args->used_size);
//         printf("d3:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), thread_args->used_size);

//         bzero(buf, 32);
//         sprintf(buf, "GO:%ld", crt);
//         // pthread_mutex_lock(thread_args->backpressure_mutex);
//         if (thread_args->backpressure == 1 && thread_args->used_size < (thread_args->buffer_size * thread_args->backpressure_threshold_down / 100)) {
//             write(thread_args->control_socket, buf, 32);
//             thread_args->backpressure = 0;
//             // printf("receiver_data_thread: backpressure change: GO\n");
//             printf("d4:%ld:%ld:%d:%ld\n", timestamp_ns, (timestamp_ns / 1000000), 1, crt++);
//         } else if (thread_args->backpressure == 0) {
//             // printf("receiver_data_thread: backpressure already on: GO\n");
//             printf("d4:%ld:%ld:%d\n", timestamp_ns, (timestamp_ns / 1000000), 0);
//         }
//         // pthread_mutex_unlock(thread_args->backpressure_mutex);
//     }
// }

void *
receiver_control_thread(void *arg)
{
    char buf[32];
    char* end;
    int backpressure = 0, backpressure_change = 0;
    long int timestamp_ns;
    long int crt = 0;
    unsigned long used_size;

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
        printf("c0:%ld:%ld:%s\n", timestamp_ns, (timestamp_ns / 1000000), buf);
        // debug_print("receiver_control_thread: received bytes: %d\n", thread_args->received_size);
        // pthread_mutex_lock(&(thread_args->cond_lock));
        // // wait while the previous chunk is still being processed and move forward only when that is done
		// while(thread_args->got_work == 1) {
		// 	pthread_cond_wait(&(thread_args->start_work), &(thread_args->cond_lock));
        // }
		pthread_mutex_lock(&(thread_args->cond_lock));
        thread_args->mem_offset_produce += thread_args->received_size;
        if (thread_args->mem_offset_produce >= thread_args->buffer_size) {
            thread_args->mem_offset_produce %= thread_args->buffer_size;
        }
        thread_args->used_size += thread_args->received_size;
        thread_args->used_size_timed += thread_args->received_size;

        // thread_args->got_work = 1; // start processing new chunk
		// if(thread_args->mem_offset_produce != thread_args->mem_offset_consume) {
            pthread_cond_broadcast(&(thread_args->start_work));
        // }

        if (thread_args->mem_offset_produce >= thread_args->mem_offset_consume) {
            used_size = thread_args->mem_offset_produce - thread_args->mem_offset_consume;
        } else {
            used_size = thread_args->buffer_size + thread_args->mem_offset_produce - thread_args->mem_offset_consume;
        }
        // printf("c3:%lu:%lu:%lu\n", thread_args->buffer_size, thread_args->mem_offset_produce, thread_args->mem_offset_consume);
   		pthread_mutex_unlock(&(thread_args->cond_lock));

        // printf("receiver_control_thread: new used size: %d\n", thread_args->used_size);
        printf("c1:%ld:%ld:%lu\n", timestamp_ns, (timestamp_ns / 1000000), used_size);

        bzero(buf, 32);
        sprintf(buf, "WAIT:%ld", crt);
        // pthread_mutex_lock(thread_args->backpressure_mutex);
        if (thread_args->backpressure == 0 && used_size >= (thread_args->buffer_size * thread_args->backpressure_threshold_up / 100)) {
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
rdma_consume(int control_socket, unsigned int backpressure_threshold_up, unsigned int backpressure_threshold_down, struct rdma_context *ctx, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned long *mem_offset, unsigned worker_count)
{
    // pthread_t *data_thread, *control_thread, *instrumentation_thread, *worker_threads;
    pthread_t *control_thread, *instrumentation_thread, *worker_threads;
    struct rdma_thread_param *thread_args;

    // data_thread = (pthread_t *)malloc(sizeof(pthread_t));
    control_thread = (pthread_t *)malloc(sizeof(pthread_t));
    instrumentation_thread = (pthread_t *)malloc(sizeof(pthread_t));
    worker_threads = (pthread_t *)malloc(worker_count * sizeof(pthread_t));

    thread_args = (struct rdma_thread_param *)malloc(sizeof(struct rdma_thread_param));

    printf("rdma_consume before: buffer addr: %d\n", ctx->buf);
    thread_args->rdma_ctx = ctx;
    printf("rdma_consume after: buffer addr: %d\n", thread_args->rdma_ctx->buf);
    thread_args->message_count = *message_count;
    thread_args->message_size = *message_size;
    thread_args->buffer_size = *buffer_size;
    // thread_args->mem_offset = *mem_offset;
    thread_args->mem_offset_produce = *mem_offset;
    thread_args->mem_offset_consume = *mem_offset;
    thread_args->used_size = 0;
    thread_args->used_size_timed = 0;
    thread_args->received_size = 0;

    thread_args->control_socket = control_socket;
    thread_args->backpressure = 0;
    thread_args->backpressure_threshold_up = backpressure_threshold_up;
    thread_args->backpressure_threshold_down = backpressure_threshold_down;

    thread_args->start_ts = get_current_timestamp_ns();

    thread_args->worker_count = worker_count;
    thread_args->worker_id = 0;
    thread_args->got_data = 0;
    
    // thread_args->sem_recv_data = (sem_t *)malloc(sizeof(sem_t));
    thread_args->backpressure_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));

    pthread_mutex_init(thread_args->backpressure_mutex, NULL);

  	pthread_mutex_init(&(thread_args->cond_lock), NULL);
	pthread_cond_init(&(thread_args->start_work), NULL);
   	pthread_barrier_init(&(thread_args->workers_done_barrier), NULL, worker_count);

    for (int i = 0; i < worker_count; i++) {
        if (pthread_create(&(worker_threads[i]), NULL, receiver_work_thread, thread_args) != 0) {
            debug_print("(RDMA_CONSUME) pthread_create() error - worker_threads[%d]\n", i);
        }
    }

    // if (pthread_create(data_thread, NULL, receiver_data_thread, thread_args) != 0) {
    //     debug_print("(RDMA_CONSUME) pthread_create() error - data_thread\n");
    // }

    if (pthread_create(control_thread, NULL, receiver_control_thread, thread_args) != 0) {
        debug_print("(RDMA_CONSUME) pthread_create() error - control_thread\n");
    }

    if (pthread_create(instrumentation_thread, NULL, receiver_instrumentation_thread, thread_args) != 0) {
        debug_print("(RDMA_CONSUME) pthread_create() error - instrumentation_thread\n");
    }

    // if (pthread_join(*data_thread, NULL) != 0) {
    //     debug_print("(RDMA_CONSUME) pthread_join() error - data_thread\n");
    // }

    if (pthread_join(*control_thread, NULL) != 0) {
        debug_print("(RDMA_CONSUME) pthread_join() error - control_thread\n");
    }

    if (pthread_join(*instrumentation_thread, NULL) != 0) {
        debug_print("(RDMA_CONSUME) pthread_join() error - instrumentation_thread\n");
    }

    pthread_mutex_destroy(thread_args->backpressure_mutex);

    // free(thread_args->sem_recv_data);
    free(thread_args->backpressure_mutex);
    free(thread_args);

	return 0;
}

int
rdma_consume_tstream(int control_socket, unsigned int backpressure_threshold_up, unsigned int backpressure_threshold_down, struct rdma_context *ctx, unsigned long *message_count, unsigned long *message_size, unsigned long *buffer_size, unsigned long *mem_offset, unsigned worker_count)
{
    // pthread_t *data_thread, *control_thread, *instrumentation_thread, *worker_threads;
    pthread_t *control_thread, *instrumentation_thread, *worker_threads;
    struct rdma_thread_param *thread_args;

    // data_thread = (pthread_t *)malloc(sizeof(pthread_t));
    control_thread = (pthread_t *)malloc(sizeof(pthread_t));
    instrumentation_thread = (pthread_t *)malloc(sizeof(pthread_t));
    worker_threads = (pthread_t *)malloc(worker_count * sizeof(pthread_t));

    thread_args = (struct rdma_thread_param *)malloc(sizeof(struct rdma_thread_param));

    printf("rdma_consume_tstream before: buffer addr: %d\n", ctx->buf);
    thread_args->rdma_ctx = ctx;
    printf("rdma_consume_tstream after: buffer addr: %d\n", thread_args->rdma_ctx->buf);
    thread_args->message_count = *message_count; // this should be 1
    printf("rdma_consume_tstream message count: %d (INVALID RESULTS IF not 1)\n", thread_args->message_count);
    thread_args->message_size = *message_size;
    thread_args->buffer_size = *buffer_size;
    // thread_args->mem_offset = *mem_offset;
    thread_args->mem_offset_produce = *mem_offset;
    thread_args->mem_offset_consume = *mem_offset;
    thread_args->used_size = 0;
    thread_args->used_size_timed = 0;
    thread_args->received_size = 0;

    thread_args->control_socket = control_socket;
    thread_args->backpressure = 0;
    thread_args->backpressure_threshold_up = backpressure_threshold_up;
    thread_args->backpressure_threshold_down = backpressure_threshold_down;

    thread_args->start_ts = get_current_timestamp_ns();

    thread_args->worker_count = worker_count;
    thread_args->worker_id = 0;
    thread_args->got_data = 0;
    
    // thread_args->sem_recv_data = (sem_t *)malloc(sizeof(sem_t));
    thread_args->backpressure_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));

    pthread_mutex_init(thread_args->backpressure_mutex, NULL);

  	pthread_mutex_init(&(thread_args->cond_lock), NULL);
	pthread_cond_init(&(thread_args->start_work), NULL);
   	pthread_barrier_init(&(thread_args->workers_done_barrier), NULL, worker_count);

    for (int i = 0; i < worker_count; i++) {
        if (pthread_create(&(worker_threads[i]), NULL, receiver_work_thread_tstream, thread_args) != 0) {
            debug_print("(RDMA_CONSUME) pthread_create() error - worker_threads[%d]\n", i);
        }
    }

    // if (pthread_create(data_thread, NULL, receiver_data_thread, thread_args) != 0) {
    //     debug_print("(RDMA_CONSUME) pthread_create() error - data_thread\n");
    // }

    if (pthread_create(control_thread, NULL, receiver_control_thread, thread_args) != 0) {
        debug_print("(RDMA_CONSUME) pthread_create() error - control_thread\n");
    }

    if (pthread_create(instrumentation_thread, NULL, receiver_instrumentation_thread, thread_args) != 0) {
        debug_print("(RDMA_CONSUME) pthread_create() error - instrumentation_thread\n");
    }

    // if (pthread_join(*data_thread, NULL) != 0) {
    //     debug_print("(RDMA_CONSUME) pthread_join() error - data_thread\n");
    // }

    if (pthread_join(*control_thread, NULL) != 0) {
        debug_print("(RDMA_CONSUME) pthread_join() error - control_thread\n");
    }

    if (pthread_join(*instrumentation_thread, NULL) != 0) {
        debug_print("(RDMA_CONSUME) pthread_join() error - instrumentation_thread\n");
    }

    pthread_mutex_destroy(thread_args->backpressure_mutex);

    // free(thread_args->sem_recv_data);
    free(thread_args->backpressure_mutex);
    free(thread_args);

	return 0;
}
