CC=gcc
O=2

INCLUDES=-Isrc/common -I/usr/include/infiniband
FLAGS=-O$(O) -g -DDEBUG
LINK_FLAGS=
LINK_LIBRARIES=-L/usr/lib64 -libverbs

CFLAGS=$(INCLUDES) $(FLAGS) -std=gnu11

# Sources
C_FILES=$(shell find src/common -name "*.c")
OBJ_FILES=$(C_FILES:.c=.o)
C_FILES_SENDER=src/apps/rdma_sender_sw_manual.c
OBJ_FILES_SENDER=$(C_FILES_SENDER:.c=.o)
C_FILES_RECEIVER=src/apps/rdma_receiver_sw_manual.c
OBJ_FILES_RECEIVER=$(C_FILES_RECEIVER:.c=.o)

# Targets
.PHONY: all
all: rdma_sender_sw_manual rdma_receiver_sw_manual

rdma_sender_sw_manual: $(OBJ_FILES) $(OBJ_FILES_SENDER)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

rdma_receiver_sw_manual: $(OBJ_FILES) $(OBJ_FILES_RECEIVER)
	$(CC) $(FLAGS) $(LINK_FLAGS) $^ -o $@ $(LINK_LIBRARIES)

clean:
	rm -rf $(OBJ_FILES) $(OBJ_FILES_SENDER) $(OBJ_FILES_RECEIVER)
	rm -rf rdma_sender_sw_manual rdma_receiver_sw_manual
