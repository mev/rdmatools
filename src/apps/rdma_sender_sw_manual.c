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

        default:
            /* Too many arguments. */
            argp_usage(state);
            break;
        }
        break;

    case ARGP_KEY_END:
        if (state->arg_num != 1) {
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
static char args_doc[] = "<local-ip-address>";

/* Program documentation. */
static char doc[] = "RDMA sender with manual metadata exchange";

/* Our argp parser. */
static struct argp argp = { options, parse_opt, args_doc, doc };

void
cli_parse(int argc, char **argv, struct rdma_config* config)
{
    // set default values
    config->function = RDMA_WRITE;

    config->local_hostname = "";
    config->local_port = 53100;
    config->remote_hostname = "";
    config->remote_port = 0;

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
    char **local_sender_rdma_metadata;
    char *remote_receiver_rdma_metadata;


    cli_parse(argc, argv, &config);

    local_sender_rdma_metadata = rdma_prepare(&config, RDMA_SENDER);
    if (local_sender_rdma_metadata == NULL) {
        fprintf(stderr, "main: Failed to initialize RDMA and get the sender RDMA metadata.\n");
        exit(1);
    }
    fprintf(stdout, "(RDMA_SENDER) local RDMA metadata: %s\n", *local_sender_rdma_metadata);

    fprintf(stdout, "(RDMA_SENDER) [FIRST] remote RDMA metadata: ");

    if (config.function == RDMA_WRITE) {
        // 78 characters for the string + 1 character for the new line, otherwise the getchar() misbehaves
        remote_receiver_rdma_metadata = (char *)malloc(79);
        memset(remote_receiver_rdma_metadata, 0, 79);

        fgets(remote_receiver_rdma_metadata, 79, stdin);
        sscanf(remote_receiver_rdma_metadata, "%0lx:%0lx:%0lx:%08x:%016lx:%s\n", &((*(config.remote_endpoint))->lid), &((*(config.remote_endpoint))->qpn), &((*(config.remote_endpoint))->psn), &((*(config.remote_endpoint))->rkey), &((*(config.remote_endpoint))->addr), &((*(config.remote_endpoint))->gid_string));
        wire_gid_to_gid((*(config.remote_endpoint))->gid_string, &((*(config.remote_endpoint))->gid));
    } else {
        // 52 characters for the string + 1 character for the new line, otherwise the getchar() misbehaves
        remote_receiver_rdma_metadata = (char *)malloc(53);
        memset(remote_receiver_rdma_metadata, 0, 53);

        fgets(remote_receiver_rdma_metadata, 53, stdin);
        sscanf(remote_receiver_rdma_metadata, "%0lx:%0lx:%0lx:%s\n", &((*(config.remote_endpoint))->lid), &((*(config.remote_endpoint))->qpn), &((*(config.remote_endpoint))->psn), &((*(config.remote_endpoint))->gid_string));
        wire_gid_to_gid((*(config.remote_endpoint))->gid_string, &((*(config.remote_endpoint))->gid));
    }

    fprintf(stdout, "(RDMA_SENDER) [THIRD] [Press ENTER to connect to receiver, send data and then go check the receiver]");
    getchar();

	if (rdma_connect_ctx(config.rdma_ctx, 1, config.mtu, config.local_endpoint, config.remote_endpoint, config.remote_count, config.gidx, RDMA_SENDER)) {
        fprintf(stderr, "main: Failed to connect to remote RDMA endpoint (subscriber).\n");
        exit(1);
	}

    if (rdma_post_send(config.rdma_ctx, config.remote_endpoint, config.message_count, config.message_size, config.mem_offset, config.remote_count) < 0) {
        fprintf(stderr, "main: Failed to post writes.\n");
        exit(1);
    }

	if (rdma_close_ctx(config.rdma_ctx, config.remote_count)) {
        fprintf(stderr, "main: Failed to clean up before exiting.\n");
	}
}
