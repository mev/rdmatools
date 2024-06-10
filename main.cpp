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


/* Options. */
static struct argp_option options[] = {
        {"ib-device", 'd', "IBDEV", 0, "IB device (e.g. mlx5_0)"},
        {"ib-gid-index", 'i', "IBGIDX", 0, "IB GID index (e.g. 5)"},
        {"message-count", 'M', "MCOUNT", 0, "RDMA message count to be received"},
        {"message-size", 'S', "MSIZE", 0, "RDMA message size to be received"},
        { 0 }
};

/* Options' parsing function. */
static error_t
parse_opt(int key, char *arg, struct argp_state *state)
{
    /* Get the input argument from argp_parse, which we
        know is a pointer to our config structure. */
    rdma_config *cfg = (rdma_config*)state->input;
    char* end;

    switch (key) {
        case 'd':
            cfg->ib_devname = std::string(arg);
            break;

        case 'i':
            cfg->gidx = strtol(arg, &end, 0);
            if (end == arg) {
                argp_error(state, "'%s' is not a number", arg);
            }
            break;

        case 'M':
            *(cfg->message_count) = strtol(arg, &end, 0);
            if (end == arg) {
                argp_error(state, "'%s' is not a number", arg);
            }
            break;

        case 'S':
            *(cfg->message_size) = strtol(arg, &end, 0);
            if (end == arg) {
                argp_error(state, "'%s' is not a number", arg);
            }
            break;

        case ARGP_KEY_ARG:
            switch(state->arg_num) {
                case 0: // <local-ip-address>
                    cfg->local_hostname = strdup(arg);
                    break;

                case 1: // <remote-ip-address>
                    cfg->remote_hostname = strdup(arg);
                    break;

                case 2: // <remote-tcp-port>
                    cfg->remote_port = strtol(arg, &end, 0);
                    if (end == arg) {
                        argp_error(state, "'%s' is not a number", arg);
                    }
                    break;

                default:
                    /* Too many arguments. */
                    argp_usage(state);
                    break;
            }
            break;

        case ARGP_KEY_END:
            if (state->arg_num != 3) {
                /* Not enough arguments. */
                argp_usage(state);
            }
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }

    return 0;
}

/* A description of the arguments we accept. */
static char args_doc[] = "<local-ip-address> <remote-ip-address> <remote-tcp-port>";

/* Program documentation. */
static char doc[] = "RDMA receiver with manual metadata exchange";

/* Our argp parser. */
static struct argp argp = { options, parse_opt, args_doc, doc };

void
cli_parse(int argc, char **argv, struct rdma_config* config)
{
    rdma_rand rr;
    rr.init();

    // set default values
    config->function = RDMA_WRITE;

    config->local_hostname = "";
    config->local_port = rr.getrand();
    config->remote_hostname = "";
    config->remote_port = 53100;

    config->ib_devname = "mlx5_0";
    config->gidx = 5;
    config->mtu = IBV_MTU_1024;

    config->remote_count = 1;

    config->local_endpoint = (struct rdma_endpoint **)calloc(config->remote_count, sizeof(struct rdma_endpoint *));
    *(config->local_endpoint) = (struct rdma_endpoint *)malloc(sizeof(struct rdma_endpoint));
    config->remote_endpoint = (struct rdma_endpoint **)calloc(config->remote_count, sizeof(struct rdma_endpoint *));
    *(config->remote_endpoint) = (struct rdma_endpoint *)malloc(sizeof(struct rdma_endpoint));

    config->message_count = (unsigned long *)calloc(config->remote_count, sizeof(unsigned long));
    *(config->message_count) = 10;
    config->message_size = (unsigned long *)calloc(config->remote_count, sizeof(unsigned long));
    *(config->message_size) = 1024;
    config->buffer_size = (unsigned long *)calloc(config->remote_count, sizeof(unsigned long));
    *(config->buffer_size) = *(config->message_count) * *(config->message_size);

    // parse arguments
    argp_parse(&argp, argc, argv, 0, 0, config);
}

int main(int argc, char** argv)
{
    rdma_config rc;
    char **local_receiver_rdma_metadata;
    char *remote_sender_rdma_metadata;
    cli_parse(argc, argv, &rc);

    ////
    local_receiver_rdma_metadata = rc.rdma_prepare_local(RDMA_RECEIVER);
    if (local_receiver_rdma_metadata == NULL) {
        fprintf(stderr, "main: Failed to initialize RDMA and get the receiver RDMA metadata.\n");
        exit(1);
    }
    fprintf(stdout, "(RDMA_RECEIVER) local RDMA metadata: %s\n", *local_receiver_rdma_metadata);
    fprintf(stdout, "(RDMA_RECEIVER) [SECOND] remote RDMA metadata: ");

    remote_sender_rdma_metadata = rc.rdma_prepare_remote(RDMA_RECEIVER);
    rc.rdma_connect();

    fprintf(stdout, "(RDMA_RECEIVER) [FOURTH] [Wait a little and then press ENTER to check the received data...]\n");
    getchar();

    // Print data in the reserved memory at the end of the write
    int i, j;

    printf("SUBSCRIBER: Buffer data:\n");
    // i = config.message_count - 1;
    for (i = 0; i < *(rc.message_count); i++) {
        for (j = 0; j < *(rc.message_size); j++) {
            printf("%d:", *(*(rc.rdma_ctx->buf) + i * *(rc.message_size) + j));
        }
    }
    printf("\nDONE\n");
    // End of data check

    if (rc.rdma_disconnect()) {
        fprintf(stderr, "main: Failed to clean up before exiting.\n");
    }
}