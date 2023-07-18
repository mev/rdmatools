#include "rdma_infrastructure.h"


struct rdma_config config;

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
    struct rdma_config *cfg = state->input;
    char* end;

    switch (key) {
    case 'd':
        cfg->ib_devname = strdup(arg);
        break;

    case 'i':
        cfg->gidx = strtol(arg, &end, 0);
        if (end == arg) {
            argp_error(state, "'%s' is not a number", arg);
        }
        break;

    case 'M':
        cfg->message_count = strtol(arg, &end, 0);
        if (end == arg) {
            argp_error(state, "'%s' is not a number", arg);
        }
        break;

    case 'S':
        cfg->message_size = strtol(arg, &end, 0);
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
    // set default values
    config->function = RDMA_WRITE;

    config->local_hostname = "";
    config->local_port = rand();
    config->remote_hostname = "";
    config->remote_port = 53100;

    config->ib_devname = "mlx5_0";
    config->gidx = 5;
    config->mtu = IBV_MTU_1024;

    config->message_count = 10;
    config->message_size = 1024;

    // parse arguments
    argp_parse(&argp, argc, argv, 0, 0, config);
}

int
main(int argc, char** argv)
{
    char *remote_sender_rdma_metadata;
    char *local_receiver_rdma_metadata;


    cli_parse(argc, argv, &config);

    local_receiver_rdma_metadata = rdma_prepare(&config, RDMA_RECEIVER);
    if (local_receiver_rdma_metadata == NULL) {
        fprintf(stderr, "main: Failed to initialize RDMA and get the receiver RDMA metadata.\n");
        exit(0);
    }
    fprintf(stdout, "(RDMA_RECEIVER) local RDMA metadata: %s\n", local_receiver_rdma_metadata);

    fprintf(stdout, "(RDMA_RECEIVER) [SECOND] remote RDMA metadata: ");

    // 52 characters for the string + 1 character for the new line, otherwise the getchar() misbehaves
    remote_sender_rdma_metadata = (char *)malloc(53);
    memset(remote_sender_rdma_metadata, 0, 53);

    fgets(remote_sender_rdma_metadata, 53, stdin);
    sscanf(remote_sender_rdma_metadata, "%0lx:%0lx:%0lx:%s\n", &config.remote_endpoint.lid, &config.remote_endpoint.qpn, &config.remote_endpoint.psn, &config.remote_endpoint.gid_string);
    wire_gid_to_gid(config.remote_endpoint.gid_string, &config.remote_endpoint.gid);

	if (rdma_connect_ctx(config.rdma_ctx, 1, 0, config.mtu, &config.remote_endpoint, config.gidx, RDMA_RECEIVER)) {
        fprintf(stderr, "main:  Failed to connect to remote RDMA endpoint (provider).\n");
        exit(0);
	}

    fprintf(stdout, "(RDMA_RECEIVER) [FOURTH] [Wait a little and then press ENTER to check the received data...]\n");
    getchar();

    // Print data in the reserved memory at the end of the write
    int i, j;

    printf("SUBSCRIBER: Buffer data:\n");
    // i = config.message_count - 1;
    for (i = 0; i < config.message_count; i++) {
        for (j = 0; j < config.message_size; j++) {
        printf("%d:", *(config.rdma_ctx->buf + i * config.message_size + j));
        }
    }
    printf("\nDONE\n");
    // End of data check

	if (rdma_close_ctx(config.rdma_ctx)) {
        fprintf(stderr, "main: Failed to clean up before exiting.\n");
	}
}
