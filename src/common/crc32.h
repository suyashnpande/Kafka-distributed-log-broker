#pragma once

#include <cstddef>
#include <cstdint>

namespace mk {

// CRC-32/ISO-HDLC (same table/polynomial as zlib's crc32).
uint32_t crc32(const uint8_t* data, size_t len);

}  // namespace mk
