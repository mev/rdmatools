#pragma once

// #define RDMA_MAX_SEND_WR (32)
// #define RDMA_MAX_RECV_WR (32)
// #define RDMA_MAX_SEND_WR (64)
// #define RDMA_MAX_RECV_WR (64)
// #define RDMA_MAX_SEND_WR (128)
// #define RDMA_MAX_RECV_WR (128)
// #define RDMA_MAX_SEND_WR (160)
// #define RDMA_MAX_RECV_WR (160)
#include <sys/timerfd.h>

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

class rdma_utils {
public:
    static void
    wire_gid_to_gid(const char *wgid, union ibv_gid *gid) {
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

    static void
    gid_to_wire_gid(const union ibv_gid *gid, char wgid[]) {
        uint32_t tmp_gid[4];
        int i;

        memcpy(tmp_gid, gid, sizeof(tmp_gid));
        for (i = 0; i < 4; ++i) {
            sprintf(&wgid[i * 8], "%08x", htobe32(tmp_gid[i]));
        }
    }

    static void
    set_timerfd(int fd, unsigned s, unsigned ns) {
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
    get_current_timestamp_ns() {
        struct timespec now;

        if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
            printf("get_current_timestamp_ns: clock_gettime failed");
            return -1;
        } else {
            return now.tv_nsec + now.tv_sec * 1E9;
        }
    }

    static long int
    get_current_timestamp_ns_thread_cpu() {
        struct timespec now;

        if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now) == -1) {
            printf("get_current_timestamp_ns: clock_gettime failed");
            return -1;
        } else {
            return now.tv_nsec + now.tv_sec * 1E9;
        }
    }
};