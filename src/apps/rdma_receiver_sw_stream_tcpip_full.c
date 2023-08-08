#include "rdma_infrastructure.h"


struct rdma_config config;
unsigned int backpressure_threshold_up = 90;
unsigned int backpressure_threshold_down = 75;

/* Options. */
static struct argp_option options[] = {
    {"ib-device", 'd', "IBDEV", 0, "IB device (e.g. mlx5_0)"},
    {"ib-gid-index", 'i', "IBGIDX", 0, "IB GID index (e.g. 5)"},
    {"message-count", 'M', "MCOUNT", 0, "RDMA message count to be received"},
    {"message-size", 'S', "MSIZE", 0, "RDMA message size to be received"},
    {"buffer-size", 'B', "BSIZE", 0, "Size of the memory buffer which will store the received RDMA messages"},
    {"backpressure-threshold-up", 's', "UPTHR", 0, "Threshold(%)) of buffer being in use for enabling backpresure"},
    {"backpressure-threshold-down", 'j', "DOWNTHR", 0, "Threshold(%)) of buffer being in use for disabling backpresure"},
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

    case 'B':
        *(cfg->buffer_size) = strtol(arg, &end, 0);
        if (end == arg) {
            argp_error(state, "'%s' is not a number", arg);
        }
        break;

    case 's':
        backpressure_threshold_up = strtol(arg, &end, 0);
        if (end == arg) {
            argp_error(state, "'%s' is not a number", arg);
        }
        break;

    case 'j':
        backpressure_threshold_down = strtol(arg, &end, 0);
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
    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        srand(time(NULL));
    } else {
        srand((int)now.tv_nsec);
    }

    // set default values
    config->function = RDMA_WRITE;

    config->local_hostname = "";
    config->local_port = rand();
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
    config->mem_offset = (unsigned long *)calloc(config->remote_count, sizeof(unsigned long));
    *(config->mem_offset) = 0;

    // parse arguments
    argp_parse(&argp, argc, argv, 0, 0, config);
}

int
main(int argc, char** argv)
{
    char **local_receiver_rdma_metadata;
    char *remote_sender_rdma_metadata;


    cli_parse(argc, argv, &config);

    local_receiver_rdma_metadata = rdma_prepare(&config, RDMA_RECEIVER);
    if (local_receiver_rdma_metadata == NULL) {
        fprintf(stderr, "main: Failed to initialize RDMA and get the receiver RDMA metadata.\n");
        exit(1);
    }
    fprintf(stdout, "(RDMA_RECEIVER) local RDMA metadata: %s\n", *local_receiver_rdma_metadata);

    printf("rdma_receiver_sw_stream_tcpiop_full 1: buffer addr: %d\n", config.rdma_ctx->buf);

    // establish TCP/IP connection
    int s;
    struct sockaddr_in s_in;

    if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        fprintf(stderr, "main: Socket initialization failed.\n");
        exit(1);
    }

    bzero(&s_in, sizeof(s_in));
    s_in.sin_family = AF_INET;
    s_in.sin_addr.s_addr=inet_addr(config.remote_hostname);
    s_in.sin_port = htons(config.remote_port);

    // connect the client socket to server socket
    if (connect(s, (struct sockaddr *)&s_in, sizeof(s_in)) != 0) {
        fprintf(stderr, "main: Connection with the server failed.\n");
        exit(1);
    } else {
        fprintf(stdout, "main: Connected to the server.\n");
    }

    // exchange metadata
    if (config.function == RDMA_WRITE) {
        write(s, *local_receiver_rdma_metadata, 78);
    } else {
        write(s, *local_receiver_rdma_metadata, 52);
    }

    remote_sender_rdma_metadata = (char *)malloc(52);
    memset(remote_sender_rdma_metadata, 0, 52);

    read(s, remote_sender_rdma_metadata, 52);
    sscanf(remote_sender_rdma_metadata, "%0lx:%0lx:%0lx:%s", &((*(config.remote_endpoint))->lid), &((*(config.remote_endpoint))->qpn), &((*(config.remote_endpoint))->psn), &((*(config.remote_endpoint))->gid_string));
    wire_gid_to_gid((*(config.remote_endpoint))->gid_string, &((*(config.remote_endpoint))->gid));

    fprintf(stdout, "(RDMA_RECEIVER) [SECOND] remote RDMA metadata: %s\n", remote_sender_rdma_metadata);

    // fprintf(stdout, "(RDMA_RECEIVER) [FOURTH-bis] [Wait a little and then press ENTER to check the received data... (BEFORE changing the QP state)]\n");
    // getchar();

    printf("rdma_receiver_sw_stream_tcpiop_full 2: buffer addr: %d\n", config.rdma_ctx->buf);

	if (rdma_connect_ctx(config.rdma_ctx, 1, config.mtu, config.local_endpoint, config.remote_endpoint, config.remote_count, config.gidx, RDMA_RECEIVER)) {
        fprintf(stderr, "main:  Failed to connect to remote RDMA endpoint (provider).\n");
        exit(1);
	}

    printf("rdma_receiver_sw_stream_tcpiop_full 3: buffer addr: %d\n", config.rdma_ctx->buf);

    if (rdma_consume(s, backpressure_threshold_up, backpressure_threshold_down, config.rdma_ctx, config.message_count, config.message_size, config.buffer_size, config.mem_offset) < 0) {
        fprintf(stderr, "main: Failed to consume incoming data.\n");
        exit(1);
    }

    // fprintf(stdout, "(RDMA_RECEIVER) [FOURTH] [Wait a little and then press ENTER to check the received data... (AFTER changing the QP state)]\n");
    // getchar();

    // char buf[32];
    // do {
    //     bzero(buf, 32);
    //     read(s, buf, 32);
    // } while (strcmp(buf, "DONE") == 0);

    // // Print data in the reserved memory at the end of the write
    // int i, j;

    // printf("SUBSCRIBER: Buffer data:\n");
    // // i = config.message_count - 1;
    // for (i = 0; i < *(config.message_count); i++) {
    //     for (j = 0; j < *(config.message_size); j++) {
    //         printf("%d:", *(*(config.rdma_ctx->buf) + i * *(config.message_size) + j));
    //     }
    // }
    // printf("\nDONE\n");
    // // End of data check

	if (rdma_close_ctx(config.rdma_ctx, config.remote_count)) {
        fprintf(stderr, "main: Failed to clean up before exiting.\n");
	}
}
