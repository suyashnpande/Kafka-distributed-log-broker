#pragma once

#include <cstdint>
#include <vector>

#include "common/codec.h"

// On-disk / on-wire record format (see AGENT_BRIEF.md section 5):
//   totalLen  : uint32   -- bytes after this field (crc32 + body)
//   crc32     : uint32   -- crc over body (offset..value)
//   offset    : uint64
//   timestamp : uint64
//   keyLen    : uint32
//   key       : bytes
//   valueLen  : uint32
//   value     : bytes

namespace mk {

struct Record {
    uint64_t offset = 0;
    uint64_t timestamp = 0;
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

// Appends the fully framed record (totalLen + crc32 + body) to buf.
void encode_record(const Record& rec, std::vector<uint8_t>& buf);

// Decodes one framed record starting at r's current position. Throws
// DecodeError if the frame is incomplete (not enough bytes remaining — the
// torn-write case during recovery) or if the CRC doesn't match (corruption).
Record decode_record(Reader& r);

}  // namespace mk
