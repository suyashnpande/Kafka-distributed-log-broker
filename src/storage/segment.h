#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "storage/record.h"

// One segment = one immutable-once-rolled pair of files: <base>.log (the
// framed records) and <base>.index (a sparse (relativeOffset, bytePosition)
// map, mirrored on disk). `Partition` owns a list of these and decides which
// one is active; `Segment` only knows how to read/write itself.

namespace mk {

class Segment {
public:
    Segment(std::string logPath, std::string indexPath, uint64_t baseOffset,
            uint32_t indexIntervalBytes);

    uint64_t base_offset() const { return baseOffset_; }
    uint64_t size_bytes() const { return sizeBytes_; }

    // Creates empty .log/.index files for a brand new segment.
    void create_empty();

    // Active-segment startup path: scans .log from byte 0, verifying each
    // frame's CRC. Stops at the first incomplete or corrupt frame (a torn
    // write), truncates the file there, and rebuilds the in-memory index (and
    // rewrites .index) from just the clean data. Returns the last valid
    // record's offset, or -1 if the segment ends up empty.
    int64_t scan_and_recover();

    // Closed-segment startup path: trusts the segment completely and just
    // loads its .index file into memory — no byte scan.
    void load_index();

    // Encodes and appends already offset/timestamp-assigned records, adding a
    // sparse index entry every indexIntervalBytes of data written.
    void append(const std::vector<Record>& records);

    // Decodes every record from `fromPos` to end of file. Callers apply
    // offset/maxBytes/high-watermark filtering.
    std::vector<Record> read_from(uint64_t fromPos) const;

    // Floor lookup into the sparse index: largest entry whose relative offset
    // is <= relOffset, or 0 if relOffset precedes the first entry.
    uint64_t find_start_position(uint32_t relOffset) const;

private:
    void append_index_entry(uint32_t relOffset, uint64_t pos, bool persist);

    std::string logPath_;
    std::string indexPath_;
    uint64_t baseOffset_;
    uint32_t indexIntervalBytes_;
    uint64_t sizeBytes_ = 0;
    uint64_t bytesSinceLastIndexEntry_ = 0;
    std::vector<std::pair<uint32_t, uint64_t>> index_;  // (relOffset, position), ascending
};

}  // namespace mk
