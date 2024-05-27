#pragma once

#include <infiniband/verbs.h>
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

    int rdma_get_port_info(int port)
    {
        return ibv_query_port(context, port, &portinfo);
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

    int
    rdma_connect_ctx(struct rdma_context *ctx, int port, enum ibv_mtu mtu, struct rdma_endpoint **local_endpoint, struct rdma_endpoint **remote_endpoint, unsigned count, int sgid_idx, int role)
    {
        int i;

        for (i = 0; i < count; i++) {
            struct ibv_qp_attr attr = {
                    .qp_state           = IBV_QPS_RTR,
                    .path_mtu           = mtu,
                    .rq_psn             = remote_endpoint[i]->psn,
                    .dest_qp_num        = remote_endpoint[i]->qpn,
                    .ah_attr			= {
                            .dlid           = remote_endpoint[i]->lid,
                            .sl             = 0,
                            .src_path_bits  = 0,
                            .is_global      = 0,
                            .port_num       = static_cast<uint8_t>(port)
                    },
                    .max_dest_rd_atomic	= 1,
                    .min_rnr_timer      = 12
            };

            if (remote_endpoint[i]->gid.global.interface_id) {
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
                attr.sq_psn         = local_endpoint[i]->psn;
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
            full_queue_count = message_count[k] / RDMA_MAX_SEND_WR;
            remainder_queue_size = message_count[k] % RDMA_MAX_SEND_WR;

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
                        (wr + i)->wr.rdma.remote_addr = remote_endpoint[k]->addr + *(mem_offset + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k);
                        (wr + i)->wr.rdma.rkey = remote_endpoint[k]->rkey;

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
                    (wr + i)->wr.rdma.remote_addr = remote_endpoint[k]->addr + *(mem_offset + k) + (j * RDMA_MAX_SEND_WR + i) * *(message_size + k);
                    (wr + i)->wr.rdma.rkey = remote_endpoint[k]->rkey;

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
};