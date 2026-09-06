#include "storage/partition.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <sstream>

using namespace std;
namespace fs = std::filesystem;

namespace mk {

namespace {

uint64_t now_ms() {
    return chrono::duration_cast<chrono::milliseconds>(
               chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

Partition::Partition(const string& dir, uint32_t maxSegBytes)
    : dir_(dir), maxSegBytes_(maxSegBytes) {}

string Partition::segment_base_name(uint64_t baseOffset) const {
    ostringstream oss;
    oss << setw(20) << setfill('0') << baseOffset;
    return oss.str();
}

void Partition::recover() {
    lock_guard<mutex> lock(mu_);
    fs::create_directories(dir_);

    vector<uint64_t> baseOffsets;
    for (const auto& entry : fs::directory_iterator(dir_)) {
        if (entry.path().extension() != ".log") continue;
        baseOffsets.push_back(stoull(entry.path().stem().string()));
    }
    sort(baseOffsets.begin(), baseOffsets.end());

    segments_.clear();
    for (uint64_t base : baseOffsets) {
        string name = segment_base_name(base);
        auto seg = make_unique<Segment>(dir_ + "/" + name + ".log",
                                         dir_ + "/" + name + ".index", base,
                                         kIndexIntervalBytes);
        segments_.push_back(move(seg));
    }

    if (segments_.empty()) {
        roll_segment(0);
        nextOffset_ = 0;
        hwm_ = 0;
        return;
    }

    for (size_t i = 0; i + 1 < segments_.size(); ++i) {
        segments_[i]->load_index();
    }
    int64_t lastOffset = segments_.back()->scan_and_recover();
    nextOffset_ = (lastOffset >= 0) ? static_cast<uint64_t>(lastOffset) + 1
                                     : segments_.back()->base_offset();
    hwm_ = static_cast<int64_t>(nextOffset_);
}

void Partition::roll_segment(uint64_t baseOffset) {
    string name = segment_base_name(baseOffset);
    auto seg = make_unique<Segment>(dir_ + "/" + name + ".log",
                                     dir_ + "/" + name + ".index", baseOffset,
                                     kIndexIntervalBytes);
    seg->create_empty();
    segments_.push_back(move(seg));
}

int64_t Partition::append(vector<Record> records) {
    lock_guard<mutex> lock(mu_);
    if (records.empty()) return static_cast<int64_t>(nextOffset_);

    if (segments_.back()->size_bytes() >= maxSegBytes_) {
        roll_segment(nextOffset_);
    }

    int64_t base = static_cast<int64_t>(nextOffset_);
    uint64_t ts = now_ms();
    for (Record& rec : records) {
        rec.offset = nextOffset_++;
        rec.timestamp = ts;
    }

    segments_.back()->append(records);
    hwm_ = static_cast<int64_t>(nextOffset_);
    return base;
}

void Partition::append_replicated(vector<Record> records) {
    lock_guard<mutex> lock(mu_);
    if (records.empty()) return;

    if (segments_.back()->size_bytes() >= maxSegBytes_) {
        roll_segment(nextOffset_);
    }

    segments_.back()->append(records);
    nextOffset_ = records.back().offset + 1;
}

Segment* Partition::segment_for_offset(int64_t offset) const {
    if (segments_.empty()) return nullptr;
    if (offset <= static_cast<int64_t>(segments_.front()->base_offset())) {
        return segments_.front().get();
    }
    Segment* found = segments_.front().get();
    for (const auto& seg : segments_) {
        if (static_cast<int64_t>(seg->base_offset()) > offset) break;
        found = seg.get();
    }
    return found;
}

vector<Record> Partition::read(int64_t startOffset, uint32_t maxBytes) const {
    lock_guard<mutex> lock(mu_);
    return read_clamped(startOffset, maxBytes, hwm_);
}

vector<Record> Partition::read_up_to_leo(int64_t startOffset, uint32_t maxBytes) const {
    lock_guard<mutex> lock(mu_);
    return read_clamped(startOffset, maxBytes, static_cast<int64_t>(nextOffset_));
}

vector<Record> Partition::read_clamped(int64_t startOffset, uint32_t maxBytes, int64_t clamp) const {
    vector<Record> result;
    if (startOffset >= clamp) return result;

    Segment* start = segment_for_offset(startOffset);
    if (!start) return result;

    size_t startIdx = 0;
    for (size_t i = 0; i < segments_.size(); ++i) {
        if (segments_[i].get() == start) {
            startIdx = i;
            break;
        }
    }

    uint32_t bytesUsed = 0;
    for (size_t i = startIdx; i < segments_.size(); ++i) {
        Segment* seg = segments_[i].get();
        uint64_t fromPos = 0;
        if (i == startIdx && startOffset > static_cast<int64_t>(seg->base_offset())) {
            uint32_t relOffset = static_cast<uint32_t>(startOffset - seg->base_offset());
            fromPos = seg->find_start_position(relOffset);
        }

        vector<Record> segRecords = seg->read_from(fromPos);
        for (Record& rec : segRecords) {
            if (static_cast<int64_t>(rec.offset) < startOffset) continue;
            if (static_cast<int64_t>(rec.offset) >= clamp) return result;

            vector<uint8_t> encoded;
            encode_record(rec, encoded);
            uint32_t recSize = static_cast<uint32_t>(encoded.size());

            if (bytesUsed + recSize > maxBytes) {
                if (result.empty()) result.push_back(move(rec));  // lone oversized record
                return result;
            }
            bytesUsed += recSize;
            result.push_back(move(rec));
        }
    }
    return result;
}

}  // namespace mk
