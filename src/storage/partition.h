#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/record.h"
#include "storage/segment.h"

namespace mk {

// Thread-safe: as of M5, a single Partition can be touched concurrently by
// several connection threads (Produce/Fetch/FETCH_REPLICA from different
// clients/followers) and IsrManager's background sweep thread all at once —
// this wasn't exercised by anything before M5's always-on sweep, but the
// underlying possibility (two clients hitting the same partition) existed
// since M2's thread-per-connection model. All public methods lock mu_.
class Partition {
public:
    explicit Partition(const std::string& dir, uint32_t maxSegBytes = 128u << 20);

    // Loads existing segments (if any), trusts all but the last, recovers the
    // last from any torn write, and sets nextOffset_. Must be called once
    // before append()/read().
    void recover();

    // Assigns offsets and timestamps (callers leave those fields as 0),
    // rolling to a new segment first if the active one has reached
    // maxSegBytes. Returns the offset assigned to the first record.
    int64_t append(std::vector<Record> records);

    // M5: writes records that already carry real offsets/timestamps assigned
    // by the partition's leader (a follower replicating them verbatim) —
    // unlike append(), does not assign offsets and does not touch the high
    // watermark. A follower's own HWM isn't consumer-visible (only leaders
    // serve Produce/Fetch), so there's nothing meaningful to set it to here.
    void append_replicated(std::vector<Record> records);

    // Returns records in [startOffset, ...) capped at maxBytes of encoded
    // size and clamped to the high watermark. Never returns a partial
    // trailing record, except when a single record alone exceeds maxBytes (it
    // is still returned, alone, so a fetcher can't stall forever on it).
    std::vector<Record> read(int64_t startOffset, uint32_t maxBytes) const;

    // M5: same as read(), but clamped to the log end offset instead of the
    // high watermark — used to serve FETCH_REPLICA, since a follower catching
    // up needs the true end of the log, not just the consumer-visible part.
    std::vector<Record> read_up_to_leo(int64_t startOffset, uint32_t maxBytes) const;

    int64_t log_end_offset() const {
        std::lock_guard<std::mutex> lock(mu_);
        return static_cast<int64_t>(nextOffset_);
    }
    int64_t high_watermark() const {
        std::lock_guard<std::mutex> lock(mu_);
        return hwm_;
    }
    void set_high_watermark(int64_t hwm) {
        std::lock_guard<std::mutex> lock(mu_);
        hwm_ = hwm;
    }

private:
    static constexpr uint32_t kIndexIntervalBytes = 4096;

    std::string segment_base_name(uint64_t baseOffset) const;
    Segment* segment_for_offset(int64_t offset) const;  // caller holds mu_
    void roll_segment(uint64_t baseOffset);              // caller holds mu_
    std::vector<Record> read_clamped(int64_t startOffset, uint32_t maxBytes, int64_t clamp) const;  // caller holds mu_

    mutable std::mutex mu_;
    std::string dir_;
    uint32_t maxSegBytes_;
    std::vector<std::unique_ptr<Segment>> segments_;  // ascending by baseOffset
    uint64_t nextOffset_ = 0;
    int64_t hwm_ = 0;
};

}  // namespace mk
