#pragma once

#include <cstddef>
#include <cstdint>

namespace mk {

// 32-bit MurmurHash2 with Kafka's own seed (0x9747b28c), matching
// org.apache.kafka.common.utils.Utils.murmur2 — used for key-hash partition
// routing. Returns a signed int32 that can be negative; callers must mask
// with & 0x7fffffff before taking a modulo (see AGENT_BRIEF.md M3 notes).
int32_t murmur2(const uint8_t* data, size_t len);

}  // namespace mk
