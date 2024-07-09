#pragma once

#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include "rdma_rand.hpp"

struct rdma_config {
    std::string usage_scenario;

    int function;

    const char *local_hostname;
    unsigned local_port;

    const char *remote_hostname;
    unsigned remote_port;

    std::string ib_devname;
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
    char* rdma_prepare_remote(int role) {
        // 52 characters for the string + 1 character for the new line, otherwise the getchar() misbehaves
        char* remote_sender_rdma_metadata = (char *)malloc(53);
        memset(remote_sender_rdma_metadata, 0, 53);

        fgets(remote_sender_rdma_metadata, 53, stdin);
        sscanf(remote_sender_rdma_metadata, "%hx:%x:%x:%s\n", &((*(remote_endpoint))->lid), &((*(remote_endpoint))->qpn),
               &((*(remote_endpoint))->psn), (char*)&((*(remote_endpoint))->gid_string));
        return remote_sender_rdma_metadata;
    }

    char ** rdma_prepare_local(int role)
    {
        int i;
        char **rdma_metadata;

        dev_list = ibv_get_device_list(NULL);
        if (!dev_list) {
            fprintf(stderr, "rdma_prepare: Failed to get IB devices list\n");
            return NULL;
        }

        for (i = 0; dev_list[i]; ++i) {
            if (std::string(ibv_get_device_name(dev_list[i])) == ib_devname) {
                break;
            }
        }
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "rdma_prepare: IB device %s not found\n", ib_devname.c_str());
            return NULL;
        }

        rdma_ctx->init(ib_dev, message_count, message_size, buffer_size, remote_count, 1, role);
        if (!rdma_ctx) {
            fprintf(stderr, "rdma_prepare: Failed to create RDMA context\n");
            return NULL;
        }

        printf("rdma_prepare: buffer addr: %ld\n", (uint64_t)(rdma_ctx->buf));

        if (rdma_ctx->rdma_get_port_info(1)) {
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
            rdma_utils::gid_to_wire_gid(&local_endpoint[i]->gid, local_endpoint[i]->gid_string);

            rdma_rand rr;
            rr.init();

            (*(local_endpoint + i))->qpn = (*(rdma_ctx->qp + i))->qp_num;
            (*(local_endpoint + i))->psn = rr.getrand() & 0xffffff;

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

    void rdma_connect() {
        rdma_utils::wire_gid_to_gid((*(remote_endpoint))->gid_string, &((*(remote_endpoint))->gid));
        if (rdma_context::rdma_connect_ctx(rdma_ctx, 1, mtu, local_endpoint, remote_endpoint, remote_count, gidx, RDMA_RECEIVER)) {
            fprintf(stderr, "main:  Failed to connect to remote RDMA endpoint (provider).\n");
            exit(1);
        }
    }

    int rdma_disconnect() {
        return rdma_context::rdma_close_ctx(rdma_ctx, remote_count);
    }
};

