#pragma once

#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

class tcpip_client{
    struct sockaddr_in s_in;
    int s;

public:
    int getHandle() { return s;}

    bool open(const char* hostname, unsigned int port) {
        // establish TCP/IP connection
        int flag = 1;

        if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            fprintf(stderr, "main: Socket initialization failed.\n");
            exit(1);
        }

        if (-1 == setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag))) {
            fprintf(stderr, "main: setsockopt TCP_NODELAY failed.\n");
            exit(1);
        }

        bzero(&s_in, sizeof(s_in));
        s_in.sin_family = AF_INET;
        s_in.sin_addr.s_addr=inet_addr(hostname);
        s_in.sin_port = htons(port);

        // connect the client socket to server socket
        if (connect(s, (struct sockaddr *)&s_in, sizeof(s_in)) != 0) {
            fprintf(stderr, "main: Connection with the server failed.\n");
            return false;
        } else {
            fprintf(stdout, "main: Connected to the server.\n");
            return true;
        }
    }
};