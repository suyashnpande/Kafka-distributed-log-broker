#include "common/murmur2.h"

namespace mk {

// Direct port of org.apache.kafka.common.utils.Utils.murmur2 — mixing is done
// in uint32_t throughout so right-shifts match Java's unsigned >>>.
int32_t murmur2(const uint8_t* data, size_t len) {
    constexpr uint32_t kSeed = 0x9747b28cu;
    constexpr uint32_t kM = 0x5bd1e995u;
    constexpr int kR = 24;

    uint32_t h = kSeed ^ static_cast<uint32_t>(len);
    size_t length4 = len / 4;

    for (size_t i = 0; i < length4; ++i) {
        size_t i4 = i * 4;
        uint32_t k = static_cast<uint32_t>(data[i4]) |
                     (static_cast<uint32_t>(data[i4 + 1]) << 8) |
                     (static_cast<uint32_t>(data[i4 + 2]) << 16) |
                     (static_cast<uint32_t>(data[i4 + 3]) << 24);
        k *= kM;
        k ^= k >> kR;
        k *= kM;
        h *= kM;
        h ^= k;
    }

    size_t tailStart = length4 * 4;
    switch (len % 4) {
        case 3:
            h ^= static_cast<uint32_t>(data[tailStart + 2]) << 16;
            [[fallthrough]];
        case 2:
            h ^= static_cast<uint32_t>(data[tailStart + 1]) << 8;
            [[fallthrough]];
        case 1:
            h ^= static_cast<uint32_t>(data[tailStart]);
            h *= kM;
            break;
        default:
            break;
    }

    h ^= h >> 13;
    h *= kM;
    h ^= h >> 15;

    return static_cast<int32_t>(h);
}

}  // namespace mk
