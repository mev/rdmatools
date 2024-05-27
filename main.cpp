#include <iostream>
#include <infiniband/verbs.h>
#include <vector>
#include <semaphore.h>

#define DEBUG 0
#define debug_print(fmt, ...) do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

#include <rdma_utils.hpp>
#include <rdma_endpoint.hpp>
#include <rdma_context.hpp>
#include <rdma_config.hpp>
#include <rdma_infrastructure.hpp>

int main() {
    rdma_config config;
    config.rdma_prepare(RDMA_RECEIVER);
    //config.toString();
    //wire_git_to_gid;
    //RdmaReceiver.rdma_prepare(config, RDMA_RECEIVER);
    //RdmaReceiver.rdma_connect_ctx(config);
    //RdmaReceiver.rdma_close_ctx
    std::cout << "Hello, World!" << std::endl;
    return 0;
}
