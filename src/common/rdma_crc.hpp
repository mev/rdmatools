#include <zlib.h>
#include <cstdint>

class rdma_crc {
    uint32_t mCRC;
public:
    rdma_crc() {
        mCRC = crc32(0L, Z_NULL, 0);
    }

    uint32_t getCRC32value(const unsigned char* address, uint32_t len) {
        return crc32(mCRC, address, len);
    }
};