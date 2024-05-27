#pragma once

#include <infiniband/verbs.h>

struct rdma_endpoint {
    uint16_t lid;
    uint32_t qpn;
    uint32_t psn;
    uint32_t rkey;
    uint64_t addr;
    char gid_string[32];
    union ibv_gid gid;
};