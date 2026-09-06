#include "common/crc32.h"

#include <array>

using namespace std;

namespace mk {

namespace {

constexpr uint32_t kPoly = 0xEDB88320u;

array<uint32_t, 256> make_table() {
    array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (kPoly ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}

const array<uint32_t, 256> kTable = make_table();

}  // namespace

uint32_t crc32(const uint8_t* data, size_t len) {
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        c = kTable[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

}  // namespace mk
