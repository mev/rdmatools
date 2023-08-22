CC=gcc
O=2

INCLUDES=-Isrc/common -I/usr/include/infiniband
FLAGS=-O$(O) -g -DDEBUG=1
LINK_FLAGS=
LINK_LIBRARIES=-L/usr/lib64 -libverbs -lpthread -lrt

CFLAGS=$(INCLUDES) $(FLAGS) -std=gnu11

# Sources

# common
C_FILES=$(shell find src/common -name "*.c")
OBJ_FILES=$(C_FILES:.c=.o)

# application-specific
C_FILES_SENDER_MANUAL=src/apps/rdma_sender_sw_manual.c
OBJ_FILES_SENDER_MANUAL=$(C_FILES_SENDER_MANUAL:.c=.o)
C_FILES_RECEIVER_MANUAL=src/apps/rdma_receiver_sw_manual.c
OBJ_FILES_RECEIVER_MANUAL=$(C_FILES_RECEIVER_MANUAL:.c=.o)

C_FILES_SENDER_TCPIP=src/apps/rdma_sender_sw_tcpip.c
OBJ_FILES_SENDER_TCPIP=$(C_FILES_SENDER_TCPIP:.c=.o)
C_FILES_RECEIVER_TCPIP=src/apps/rdma_receiver_sw_tcpip.c
OBJ_FILES_RECEIVER_TCPIP=$(C_FILES_RECEIVER_TCPIP:.c=.o)

C_FILES_SENDER_TCPIP_FULL=src/apps/rdma_sender_sw_tcpip_full.c
OBJ_FILES_SENDER_TCPIP_FULL=$(C_FILES_SENDER_TCPIP_FULL:.c=.o)
C_FILES_RECEIVER_TCPIP_FULL=src/apps/rdma_receiver_sw_tcpip_full.c
OBJ_FILES_RECEIVER_TCPIP_FULL=$(C_FILES_RECEIVER_TCPIP_FULL:.c=.o)

C_FILES_SENDER_ONETOMANY_TCPIP_FULL=src/apps/rdma_sender_onetomany_sw_tcpip_full.c
OBJ_FILES_SENDER_ONETOMANY_TCPIP_FULL=$(C_FILES_SENDER_ONETOMANY_TCPIP_FULL:.c=.o)

C_FILES_SENDER_STREAM_TCPIP_FULL=src/apps/rdma_sender_sw_stream_tcpip_full.c
OBJ_FILES_SENDER_STREAM_TCPIP_FULL=$(C_FILES_SENDER_STREAM_TCPIP_FULL:.c=.o)
C_FILES_RECEIVER_STREAM_TCPIP_FULL=src/apps/rdma_receiver_sw_stream_tcpip_full.c
OBJ_FILES_RECEIVER_STREAM_TCPIP_FULL=$(C_FILES_RECEIVER_STREAM_TCPIP_FULL:.c=.o)

C_FILES_SENDER_ONETOMANY_STREAM_TCPIP_FULL=src/apps/rdma_sender_onetomany_sw_stream_tcpip_full.c
OBJ_FILES_SENDER_ONETOMANY_STREAM_TCPIP_FULL=$(C_FILES_SENDER_ONETOMANY_STREAM_TCPIP_FULL:.c=.o)

# Targets

.PHONY: all
all: rdma_sender_sw_manual rdma_receiver_sw_manual rdma_sender_sw_tcpip rdma_receiver_sw_tcpip rdma_sender_sw_tcpip_full rdma_receiver_sw_tcpip_full rdma_sender_onetomany_sw_tcpip_full rdma_sender_sw_stream_tcpip_full rdma_receiver_sw_stream_tcpip_full rdma_sender_onetomany_sw_stream_tcpip_full

rdma_sender_sw_manual: $(OBJ_FILES) $(OBJ_FILES_SENDER_MANUAL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_receiver_sw_manual: $(OBJ_FILES) $(OBJ_FILES_RECEIVER_MANUAL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_sender_sw_tcpip: $(OBJ_FILES) $(OBJ_FILES_SENDER_TCPIP)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_receiver_sw_tcpip: $(OBJ_FILES) $(OBJ_FILES_RECEIVER_TCPIP)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_sender_sw_tcpip_full: $(OBJ_FILES) $(OBJ_FILES_SENDER_TCPIP_FULL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_receiver_sw_tcpip_full: $(OBJ_FILES) $(OBJ_FILES_RECEIVER_TCPIP_FULL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_sender_onetomany_sw_tcpip_full: $(OBJ_FILES) $(OBJ_FILES_SENDER_ONETOMANY_TCPIP_FULL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_sender_sw_stream_tcpip_full: $(OBJ_FILES) $(OBJ_FILES_SENDER_STREAM_TCPIP_FULL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_receiver_sw_stream_tcpip_full: $(OBJ_FILES) $(OBJ_FILES_RECEIVER_STREAM_TCPIP_FULL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_sender_onetomany_sw_stream_tcpip_full: $(OBJ_FILES) $(OBJ_FILES_SENDER_ONETOMANY_STREAM_TCPIP_FULL)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

clean:
	rm -rf $(OBJ_FILES) $(OBJ_FILES_SENDER_MANUAL) $(OBJ_FILES_RECEIVER_MANUAL) $(OBJ_FILES_SENDER_TCPIP) $(OBJ_FILES_RECEIVER_TCPIP) $(OBJ_FILES_SENDER_TCPIP_FULL) $(OBJ_FILES_RECEIVER_TCPIP_FULL) $(OBJ_FILES_SENDER_ONETOMANY_TCPIP_FULL) $(OBJ_FILES_SENDER_STREAM_TCPIP_FULL) $(OBJ_FILES_RECEIVER_STREAM_TCPIP_FULL) $(OBJ_FILES_SENDER_ONETOMANY_STREAM_TCPIP_FULL)
	rm -rf rdma_sender_sw_manual rdma_receiver_sw_manual rdma_sender_sw_tcpip rdma_receiver_sw_tcpip rdma_sender_sw_tcpip_full rdma_receiver_sw_tcpip_full rdma_sender_onetomany_sw_tcpip_full rdma_sender_sw_stream_tcpip_full rdma_receiver_sw_stream_tcpip_full rdma_sender_onetomany_sw_stream_tcpip_full
