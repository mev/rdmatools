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

char *
rdma_prepare(struct rdma_config *config, int role)
{
    int i;
    char *rdma_metadata;

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

    config->rdma_ctx = rdma_init_ctx(config->ib_dev, config->message_count, config->message_size, 1, role);
    if (!config->rdma_ctx) {
        fprintf(stderr, "rdma_prepare: Failed to create RDMA context\n");
        return NULL;
    }

    if (rdma_get_port_info(config->rdma_ctx->context, 1, &config->rdma_ctx->portinfo)) {
        fprintf(stderr, "rdma_prepare: Couldn't get port info\n");
        return NULL;
    }
    
    config->local_endpoint.lid = config->rdma_ctx->portinfo.lid;
    if (config->rdma_ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET && !config->local_endpoint.lid) {
        fprintf(stderr, "rdma_prepare: Couldn't get local LID\n");
        return NULL;
    }
    
    if (config->gidx >= 0) {
        if (ibv_query_gid(config->rdma_ctx->context, 1, config->gidx, &config->local_endpoint.gid)) {
        fprintf(stderr, "rdma_prepare: Can't read sgid of index %d\n", config->gidx);
        return NULL;
        }
    } else {
        memset(&config->local_endpoint.gid, 0, sizeof(config->local_endpoint.gid));
    }

    inet_ntop(AF_INET6, &config->local_endpoint.gid, config->local_endpoint.gid_string, sizeof(config->local_endpoint.gid_string));
    gid_to_wire_gid(&config->local_endpoint.gid, config->local_endpoint.gid_string);

    config->local_endpoint.qpn = config->rdma_ctx->qp->qp_num;
    config->local_endpoint.psn = rand() & 0xffffff;

    if (config->function == RDMA_WRITE && role == RDMA_RECEIVER) {
        config->local_endpoint.rkey = config->rdma_ctx->mr->rkey;
        config->local_endpoint.addr = (uint64_t)config->rdma_ctx->mr->addr;
        rdma_metadata = (char *)malloc(78); // 4+1+6+1+6+1+8+1+16+1+32+1 (last one is the string terminator)
        memset(rdma_metadata, 0, 78);
        snprintf(rdma_metadata, 78, "%04x:%06x:%06x:%08x:%016lx:%s", config->local_endpoint.lid, config->local_endpoint.qpn, config->local_endpoint.psn, config->local_endpoint.rkey, config->local_endpoint.addr, config->local_endpoint.gid_string);
        debug_print("(RDMA_WRITE) local RDMA metadata: %s\n", rdma_metadata);
    } else {
        rdma_metadata = (char *)malloc(52); // 4+1+6+1+6+1++32+1 (last one is the string terminator)
        memset(rdma_metadata, 0, 52);
        snprintf(rdma_metadata, 52, "%04x:%06x:%06x:%s", config->local_endpoint.lid, config->local_endpoint.qpn, config->local_endpoint.psn, config->local_endpoint.gid_string);
        debug_print("(RDMA_SEND) local RDMA metadata: %s\n", rdma_metadata);
    }

    return(rdma_metadata);
}

struct rdma_context *
rdma_init_ctx(struct ibv_device *ib_dev, unsigned long message_count, unsigned long message_size, int port, int role)
{
    struct rdma_context *ctx;
    int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    int i, j;
    
    ctx = (struct rdma_context *)calloc(1, sizeof(*ctx));
    if (!ctx) {
        fprintf(stderr, "rdma_init_ctx: Failed to create RDMA context\n");
        return NULL;
    }

    ctx->size       = message_count * message_size;
    ctx->send_flags = IBV_SEND_SIGNALED;

    ctx->buf = (char *)malloc(ctx->size);
    if (!ctx->buf) {
        fprintf(stderr, "rdma_init_ctx: Couldn't allocate work buf.\n");
        goto clean_ctx;
    }

    if (role == RDMA_SENDER) {
        for (i = 0; i < message_count; i++) {
            memset(ctx->buf + i * message_size, i+43, message_size);
        }
    } else {
        memset(ctx->buf + i * message_size, 0x7b, message_size);
    }

    // Let the games begin!
    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context) {
        fprintf(stderr, "rdma_init_ctx: Couldn't get context for %s\n", ibv_get_device_name(ib_dev));
        goto clean_buffer;
    }
    
    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd) {
        fprintf(stderr, "rdma_init_ctx: Couldn't allocate PD\n");
    }
    
    ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, ctx->size, access_flags);
    if (!ctx->mr) {
        fprintf(stderr, "rdma_init_ctx: Couldn't register MR\n");
    }
    
    ctx->cq = ibv_create_cq(ctx->context, RDMA_MAX_RECV_WR, NULL, NULL, 0);
    if (!ctx->cq) {
        fprintf(stderr, "rdma_init_ctx: Couldn't create CQ\n");
        goto clean_mr;
    }

    {
        struct ibv_qp_init_attr init_attr = {
            .send_cq = ctx->cq,
            .recv_cq = ctx->cq,
            .cap     = {
                .max_send_wr  = RDMA_MAX_SEND_WR,
                .max_recv_wr  = RDMA_MAX_RECV_WR,
                .max_send_sge = 1,
                .max_recv_sge = 1
            },
            .sq_sig_all = 0,
            .qp_type = IBV_QPT_RC
        };
        
        ctx->qp = ibv_create_qp(ctx->pd, &init_attr);
        if (!ctx->qp)  {
            fprintf(stderr, "rdma_init_ctx: Couldn't create QP\n");
            goto clean_cq;
        }
    }

    {
        struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_INIT,
            .pkey_index      = 0,
            .port_num        = port,
            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
        };
        
        if (ibv_modify_qp(ctx->qp, &attr,
        IBV_QP_STATE              |
        IBV_QP_PKEY_INDEX         |
        IBV_QP_PORT               |
        IBV_QP_ACCESS_FLAGS)) {
            fprintf(stderr, "rdma_init_ctx: Failed to modify QP to INIT\n");
            goto clean_qp;
        } else {
            fprintf(stdout, "rdma_init_ctx: QP state set to INIT\n");
        }
    }

    return ctx;

    clean_qp:
    ibv_destroy_qp(ctx->qp);

    clean_cq:
    ibv_destroy_cq(ctx->cq);

    clean_mr:
    ibv_dereg_mr(ctx->mr);
    ibv_dealloc_pd(ctx->pd);

    clean_buffer:
    free(ctx->buf);

    clean_ctx:
    free(ctx);

    return NULL;
}

int
rdma_connect_ctx(struct rdma_context *ctx, int port, int local_psn, enum ibv_mtu mtu, struct rdma_endpoint *remote_endpoint, int sgid_idx, int role)
{
	struct ibv_qp_attr attr = {
		.qp_state           = IBV_QPS_RTR,
		.path_mtu           = mtu,
		.dest_qp_num        = remote_endpoint->qpn,
		.rq_psn             = remote_endpoint->psn,
		.max_dest_rd_atomic	= 1,
		.min_rnr_timer      = 12,
		.ah_attr			= {
			.is_global      = 0,
			.dlid           = remote_endpoint->lid,
			.sl             = 0,
			.src_path_bits  = 0,
			.port_num       = port
		}
	};

	if (remote_endpoint->gid.global.interface_id) {
		attr.ah_attr.is_global      = 1;
		attr.ah_attr.grh.hop_limit  = 1;
		attr.ah_attr.grh.dgid       = remote_endpoint->gid;
		attr.ah_attr.grh.sgid_index = sgid_idx;
	}

	if (ibv_modify_qp(ctx->qp, &attr,
			IBV_QP_STATE              |
			IBV_QP_AV                 |
			IBV_QP_PATH_MTU           |
			IBV_QP_DEST_QPN           |
			IBV_QP_RQ_PSN             |
			IBV_QP_MAX_DEST_RD_ATOMIC |
			IBV_QP_MIN_RNR_TIMER)) {
		fprintf(stderr, "rdma_connect_ctx: Failed to modify QP to RTR\n");
		return 1;
    } else {
        fprintf(stdout, "rdma_connect_ctx: QP state set to RTR\n");
	}

    if (role == RDMA_SENDER) {
        attr.qp_state       = IBV_QPS_RTS;
        attr.timeout        = 16;
        attr.retry_cnt      = 7;
        attr.rnr_retry      = 6;
        attr.sq_psn         = local_psn;
        attr.max_rd_atomic  = 1;

        if (ibv_modify_qp(ctx->qp, &attr,
                IBV_QP_STATE              |
                IBV_QP_TIMEOUT            |
                IBV_QP_RETRY_CNT          |
                IBV_QP_RNR_RETRY          |
                IBV_QP_SQ_PSN             |
                IBV_QP_MAX_QP_RD_ATOMIC)) {
            fprintf(stderr, "rdma_connect_ctx: Failed to modify QP to RTS\n");
            return 1;
        } else {
            fprintf(stdout, "rdma_connect_ctx: QP state set to RTS\n");
        }
    }

	return 0;
}

int
rdma_close_ctx(struct rdma_context *ctx)
{
    if (ibv_destroy_qp(ctx->qp)) {
        fprintf(stderr, "rdma_close_ctx: Couldn't destroy QP\n");
        return 1;
    }
    
    if (ibv_destroy_cq(ctx->cq)) {
        fprintf(stderr, "rdma_close_ctx: Couldn't destroy CQ\n");
        return 1;
    }
    
    if (ibv_dereg_mr(ctx->mr)) {
        fprintf(stderr, "rdma_close_ctx: Couldn't deregister MR\n");
        return 1;
    }
    
    if (ibv_dealloc_pd(ctx->pd)) {
        fprintf(stderr, "rdma_close_ctx: Couldn't deallocate PD\n");
        return 1;
    }
    
    if (ibv_close_device(ctx->context)) {
        fprintf(stderr, "rdma_close_ctx: Couldn't release context\n");
        return 1;
    }
    
    free(ctx->buf);
    free(ctx);
    
    return 0;
}

int
rdma_post_send(struct rdma_context *ctx, struct rdma_endpoint *remote_endpoint, unsigned long message_count, unsigned long message_size)
{
    struct ibv_send_wr *wr, **bad_wr;
    struct ibv_sge *list;

	int i, j, total, full_queue_count, remainder_queue_size, num_cq_events, loop_num_cq_events;
	struct ibv_cq *ev_cq;
	void *ev_ctx;
    char *buf;
    buf = (char *)malloc(message_size);

	int ne;
	struct ibv_wc wc[RDMA_MAX_SEND_WR];

    int done, last, left;

    full_queue_count = message_count / RDMA_MAX_SEND_WR;
    remainder_queue_size = message_count % RDMA_MAX_SEND_WR;

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

                (wr + i)->send_flags = ctx->send_flags;
                (wr + i)->wr.rdma.remote_addr = remote_endpoint->addr + (j * RDMA_MAX_SEND_WR + i) * message_size;
                (wr + i)->wr.rdma.rkey = remote_endpoint->rkey;

                (list + i)->length = message_size;
                (list + i)->addr = (uint64_t)(ctx->buf + (j * RDMA_MAX_SEND_WR + i) * message_size);
                (list + i)->lkey = ctx->mr->lkey;

                if (ibv_post_send(ctx->qp, wr + i, bad_wr + i)) {
                    fprintf(stderr, "rdma_post_send: Couldn't post send #%d\n", i);
                    break;
                } else {
                    total++;
                }
            }

            debug_print("(loop %d) written %d posts\n", j, i);
            if (i < RDMA_MAX_SEND_WR) {
                debug_print("(loop %d) missing %d writes\n", j, RDMA_MAX_SEND_WR - i);
                done = 0;
                last = i;
            } else {
                debug_print("(loop %d) all writes done\n", j);
                done = 1;
            }

            left = RDMA_MAX_SEND_WR;
            do {
                ne = ibv_poll_cq(ctx->cq, left, wc);
                if (ne < 0) {
                    debug_print("(loop %d) poll CQ failed %d\n", j, ne);
                } else {
                    left -= ne;
                    debug_print("(loop %d) ne=%d, left=%d\n", j, ne, left);          
                }
            } while (left > 0);
            debug_print("(loop %d) all ACK\n", j);          
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
            (wr + i)->wr.rdma.remote_addr = remote_endpoint->addr + (j * RDMA_MAX_SEND_WR + i) * message_size;
            (wr + i)->wr.rdma.rkey = remote_endpoint->rkey;

            (list + i)->length = message_size;
            (list + i)->addr = (uint64_t)(ctx->buf + (j * RDMA_MAX_SEND_WR + i) * message_size);
            (list + i)->lkey = ctx->mr->lkey;

            if (ibv_post_send(ctx->qp, wr + i, bad_wr + i)) {
                fprintf(stderr, "rdma_post_send: Couldn't post send #%d\n", i);
                break;
            } else {
                total++;
            }
        }

        debug_print("(final loop) written %d posts\n", i);
        if (i < remainder_queue_size) {
            debug_print("(final loop) missing %d writes\n", remainder_queue_size - i);
            done = 0;
            last = i;
        } else {
            debug_print("(final loop) all writes done\n");
            done = 1;
        }

        left = remainder_queue_size;
        do {
            ne = ibv_poll_cq(ctx->cq, left, wc);
            if (ne < 0) {
                debug_print("(final loop) poll CQ failed %d\n", ne);
            } else {
                left -= ne;
                debug_print("(final loop) ne=%d, left=%d\n", ne, left);          
            }
        } while (left > 0);
        debug_print("(final loop) all ACK\n");          
    }

    free(wr);
    free(bad_wr);
    free(list);

    free(buf);

	return total;
}
