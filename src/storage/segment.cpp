#include "storage/segment.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "common/codec.h"

using namespace std;
namespace fs = std::filesystem;

namespace mk {

namespace {

// .index file entries are fixed-size: relOffset(4) + position(8) = 12 bytes.
constexpr size_t kIndexEntrySize = 12;

vector<uint8_t> read_whole_file(const string& path) {
    ifstream in(path, ios::binary);
    if (!in) return {};
    in.seekg(0, ios::end);
    streamoff size = in.tellg();
    if (size <= 0) return {};
    in.seekg(0, ios::beg);
    vector<uint8_t> buf(static_cast<size_t>(size));
    in.read(reinterpret_cast<char*>(buf.data()), size);
    return buf;
}

}  // namespace

Segment::Segment(string logPath, string indexPath, uint64_t baseOffset,
                  uint32_t indexIntervalBytes)
    : logPath_(move(logPath)),
      indexPath_(move(indexPath)),
      baseOffset_(baseOffset),
      indexIntervalBytes_(indexIntervalBytes) {}

void Segment::create_empty() {
    ofstream(logPath_, ios::binary | ios::app).close();
    ofstream(indexPath_, ios::binary | ios::app).close();
    sizeBytes_ = 0;
    bytesSinceLastIndexEntry_ = 0;
    index_.clear();
}

void Segment::append_index_entry(uint32_t relOffset, uint64_t pos, bool persist) {
    index_.emplace_back(relOffset, pos);
    if (persist) {
        vector<uint8_t> entry;
        put_u32(entry, relOffset);
        put_u64(entry, pos);
        ofstream out(indexPath_, ios::binary | ios::app);
        out.write(reinterpret_cast<const char*>(entry.data()), entry.size());
    }
}

int64_t Segment::scan_and_recover() {
    vector<uint8_t> data = read_whole_file(logPath_);
    Reader r(data.data(), data.size());

    index_.clear();
    bytesSinceLastIndexEntry_ = 0;
    int64_t lastOffset = -1;
    uint64_t lastGoodPos = 0;

    // Rebuild the index from scratch as we replay, mirroring append()'s cadence.
    ofstream freshIndex(indexPath_, ios::binary | ios::trunc);
    freshIndex.close();

    while (r.remaining() > 0) {
        uint64_t framePos = r.pos();
        try {
            Record rec = decode_record(r);
            uint32_t relOffset = static_cast<uint32_t>(rec.offset - baseOffset_);
            if (index_.empty() || bytesSinceLastIndexEntry_ >= indexIntervalBytes_) {
                append_index_entry(relOffset, framePos, /*persist=*/true);
                bytesSinceLastIndexEntry_ = 0;
            }
            uint64_t frameLen = r.pos() - framePos;
            bytesSinceLastIndexEntry_ += frameLen;
            lastOffset = static_cast<int64_t>(rec.offset);
            lastGoodPos = r.pos();
        } catch (const DecodeError&) {
            break;  // torn write or corruption — stop here, discard the rest
        }
    }

    fs::resize_file(logPath_, lastGoodPos);
    sizeBytes_ = lastGoodPos;
    return lastOffset;
}

void Segment::load_index() {
    vector<uint8_t> data = read_whole_file(indexPath_);
    index_.clear();
    Reader r(data.data(), data.size());
    while (r.remaining() >= kIndexEntrySize) {
        uint32_t relOffset = r.get_u32();
        uint64_t pos = r.get_u64();
        index_.emplace_back(relOffset, pos);
    }
    error_code ec;
    sizeBytes_ = fs::file_size(logPath_, ec);
    if (ec) sizeBytes_ = 0;
    bytesSinceLastIndexEntry_ = 0;
}

void Segment::append(const vector<Record>& records) {
    ofstream out(logPath_, ios::binary | ios::app);
    for (const Record& rec : records) {
        uint32_t relOffset = static_cast<uint32_t>(rec.offset - baseOffset_);
        if (index_.empty() || bytesSinceLastIndexEntry_ >= indexIntervalBytes_) {
            append_index_entry(relOffset, sizeBytes_, /*persist=*/true);
            bytesSinceLastIndexEntry_ = 0;
        }
        vector<uint8_t> encoded;
        encode_record(rec, encoded);
        out.write(reinterpret_cast<const char*>(encoded.data()),
                   static_cast<streamsize>(encoded.size()));
        sizeBytes_ += encoded.size();
        bytesSinceLastIndexEntry_ += encoded.size();
    }
}

vector<Record> Segment::read_from(uint64_t fromPos) const {
    vector<uint8_t> data = read_whole_file(logPath_);
    if (fromPos >= data.size()) return {};

    Reader r(data.data() + fromPos, data.size() - fromPos);
    vector<Record> out;
    while (r.remaining() > 0) {
        try {
            out.push_back(decode_record(r));
        } catch (const DecodeError&) {
            break;  // shouldn't happen for clean data; stop defensively
        }
    }
    return out;
}

uint64_t Segment::find_start_position(uint32_t relOffset) const {
    if (index_.empty() || relOffset < index_.front().first) return 0;
    // Last entry with first <= relOffset (index_ is ascending by construction).
    auto it = upper_bound(index_.begin(), index_.end(), relOffset,
                          [](uint32_t val, const pair<uint32_t, uint64_t>& e) {
                              return val < e.first;
                          });
    return prev(it)->second;
}

}  // namespace mk
