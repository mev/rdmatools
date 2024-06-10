#pragma once

struct rdma_rand {
    struct timespec now;

    void init() {
        if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
            srand(time(NULL));
        } else {
            srand((int)now.tv_nsec);
        }
    }

    auto getrand() { return rand();}
};
