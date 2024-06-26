#include "rdma_infrastructure.h"


struct rdma_config config;

/* Options. */
static struct argp_option options[] = {
    {"ib-device", 'd', "IBDEV", 0, "IB device (e.g. mlx5_0)"},
    {"ib-gid-index", 'i', "IBGIDX", 0, "IB GID index (e.g. 5)"},
    {"message-count", 'M', "MCOUNT", 0, "RDMA message count to be received"},
    {"message-size", 'S', "MSIZE", 0, "RDMA message size to be received"},
    {"buffer-size", 'B', "BSIZE", 0, "Size of the memory buffer which will store the received RDMA messages"},
    {"client-count", 'C', "CCOUNT", 0, "Number of clients expected to connect"},
    { 0 }
};

/* Options' parsing function. */
static error_t
parse_opt(int key, char *arg, struct argp_state *state)
{
    int i;

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

        for (i = 1; i < cfg->remote_count; i++) {
            *(cfg->message_count + i) = *(cfg->message_count);
        }
        break;

    case 'S':
        *(cfg->message_size) = strtol(arg, &end, 0);
        if (end == arg) {
            argp_error(state, "'%s' is not a number", arg);
        }

        for (i = 1; i < cfg->remote_count; i++) {
            *(cfg->message_size + i) = *(cfg->message_size);
        }
        break;

    case 'B':
        *(cfg->buffer_size) = strtol(arg, &end, 0);
        if (end == arg) {
            argp_error(state, "'%s' is not a number", arg);
        }
        break;

    case 'C':
        cfg->remote_count = strtol(arg, &end, 0);
        if (end == arg) {
            argp_error(state, "'%s' is not a number", arg);
        }

        cfg->local_endpoint = (struct rdma_endpoint **)realloc(cfg->local_endpoint, cfg->remote_count * sizeof(struct rdma_endpoint *));
        cfg->remote_endpoint = (struct rdma_endpoint **)realloc(cfg->remote_endpoint, cfg->remote_count * sizeof(struct rdma_endpoint *));
        cfg->message_count = (unsigned long *)realloc(cfg->message_count, cfg->remote_count * sizeof(unsigned long));
        cfg->message_size = (unsigned long *)realloc(cfg->message_size, cfg->remote_count * sizeof(unsigned long));
        cfg->buffer_size = (unsigned long *)realloc(cfg->buffer_size, cfg->remote_count * sizeof(unsigned long));
        cfg->mem_offset = (unsigned long *)realloc(cfg->mem_offset, cfg->remote_count * sizeof(unsigned long));

        for (i = 1; i < cfg->remote_count; i++) {
            *(cfg->local_endpoint + i) = (struct rdma_endpoint *)malloc(sizeof(struct rdma_endpoint));
            *(cfg->remote_endpoint + i) = (struct rdma_endpoint *)malloc(sizeof(struct rdma_endpoint));
            *(cfg->message_count + i) = *(cfg->message_count);
            *(cfg->message_size + i) = *(cfg->message_size);
            *(cfg->buffer_size + i) = *(cfg->buffer_size);
            *(cfg->mem_offset + i) = *(cfg->mem_offset);
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
    unsigned crt_remote_count = 0;
    int i, *clinet_connfd_list;


    cli_parse(argc, argv, &config);

    local_sender_rdma_metadata = rdma_prepare(&config, RDMA_SENDER);
    if (local_sender_rdma_metadata == NULL) {
        fprintf(stderr, "main: Failed to initialize RDMA and get the sender RDMA metadata.\n");
        exit(1);
    }
    for (i = 0; i < config.remote_count; i++) {
        fprintf(stdout, "(RDMA_SENDER) local RDMA metadata (#%d out of %d): %s\n", i + 1, config.remote_count, *(local_sender_rdma_metadata + i));
    }

    // establish TCP/IP connection
    int s, c, len, flag = 1;
    struct sockaddr_in s_in, c_in;

    if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        fprintf(stderr, "main: Socket initialization failed.\n");
        exit(1);
    }

    if (-1 == setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag))) {  
        fprintf(stderr, "main: setsockopt failed.\n");
        exit(1);
    }

    if (-1 == setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag))) {  
        fprintf(stderr, "main: setsockopt TCP_NODELAY failed.\n");
        exit(1);
    }

    bzero(&s_in, sizeof(s_in));
    s_in.sin_family = AF_INET;
    s_in.sin_addr.s_addr=inet_addr(config.local_hostname);
    s_in.sin_port = htons(config.local_port);

    // bind newly created socket to given IP
    if ((bind(s, (struct sockaddr *)&s_in, sizeof(s_in))) != 0) {
        fprintf(stderr, "main: Socket bind failed.\n");
        exit(1);
    } else {
        fprintf(stdout, "main: Socket successfully bound.\n");
    }
        
    // now server is ready to listen
    if ((listen(s, 5)) != 0) {
        fprintf(stderr, "main: Listen failed.\n");
        exit(1);
    } else {
        fprintf(stdout, "main: Server listening.\n");
    }
            
    len = sizeof(c_in);
    
    clinet_connfd_list = (int *)calloc(config.remote_count, sizeof(int));
    memset(clinet_connfd_list, 0, config.remote_count);

    // wait for all clients to connect
    while (crt_remote_count < config.remote_count) {
        // accept the data from client
        c = accept(s, (struct sockaddr *)&c_in, &len);
        if (c < 0) {
            fprintf(stderr, "main: Server accept failed (#%d out of %d).\n", crt_remote_count + 1, config.remote_count);
            exit(1);
        } else {
            fprintf(stdout, "main: Server accepted client (#%d out of %d).\n", crt_remote_count + 1, config.remote_count);
        }

        // exchange data
        if (config.function == RDMA_WRITE) {
            remote_receiver_rdma_metadata = (char *)malloc(78);
            memset(remote_receiver_rdma_metadata, 0, 78);

            read(c, remote_receiver_rdma_metadata, 78);
            sscanf(remote_receiver_rdma_metadata, "%0lx:%0lx:%0lx:%08x:%016lx:%s", &((*(config.remote_endpoint + crt_remote_count))->lid), &((*(config.remote_endpoint + crt_remote_count))->qpn), &((*(config.remote_endpoint + crt_remote_count))->psn), &((*(config.remote_endpoint + crt_remote_count))->rkey), &((*(config.remote_endpoint + crt_remote_count))->addr), &((*(config.remote_endpoint + crt_remote_count))->gid_string));
            wire_gid_to_gid((*(config.remote_endpoint + crt_remote_count))->gid_string, &((*(config.remote_endpoint + crt_remote_count))->gid));
        } else {
            remote_receiver_rdma_metadata = (char *)malloc(52);
            memset(remote_receiver_rdma_metadata, 0, 52);

            read(c, remote_receiver_rdma_metadata, 52);
            sscanf(remote_receiver_rdma_metadata, "%0lx:%0lx:%0lx:%s", &((*(config.remote_endpoint + crt_remote_count))->lid), &((*(config.remote_endpoint + crt_remote_count))->qpn), &((*(config.remote_endpoint + crt_remote_count))->psn), &((*(config.remote_endpoint + crt_remote_count))->gid_string));
            wire_gid_to_gid((*(config.remote_endpoint + crt_remote_count))->gid_string, &((*(config.remote_endpoint + crt_remote_count))->gid));
        }

        fprintf(stdout, "(RDMA_SENDER) [FIRST] remote RDMA metadata: %s (#%d out of %d).\n", remote_receiver_rdma_metadata, crt_remote_count + 1, config.remote_count);

        write(c, *(local_sender_rdma_metadata + crt_remote_count), 52);

        // fprintf(stdout, "(RDMA_SENDER) [THIRD] [Press ENTER to connect to receiver, send data and then go check the receiver]");
        // getchar();

        *(clinet_connfd_list + crt_remote_count++) = c;
    }

    if (rdma_connect_ctx(config.rdma_ctx, 1, config.mtu, config.local_endpoint, config.remote_endpoint, config.remote_count, config.gidx, RDMA_SENDER)) {
        fprintf(stderr, "main: Failed to connect to remote RDMA endpoint (subscriber).\n");
        exit(1);
    }

    char buf[32];
    for (i = 0; i < config.remote_count; i++) {
        // char buf[32];
        do {
            bzero(buf, 32);
            read(*(clinet_connfd_list + i), buf, 32);
        } while (strcmp(buf, "GO") != 0);
    }

    // if (rdma_post_send(config.rdma_ctx, config.remote_endpoint, config.message_count, config.message_size, config.mem_offset, config.remote_count) < 0) {
    if (rdma_post_send_mt(config.rdma_ctx, config.remote_endpoint, config.message_count, config.message_size, config.mem_offset, config.remote_count) < 0) {
        fprintf(stderr, "main: Failed to post writes.\n");
        exit(1);
    }

    for (i = 0; i < config.remote_count; i++) {
        // char buf[32];
        write(*(clinet_connfd_list + i), "DONE", 32);
    }

	if (rdma_close_ctx(config.rdma_ctx, config.remote_count)) {
        fprintf(stderr, "main: Failed to clean up before exiting.\n");
	}
}
