#include "common/codec.h"
using namespace std;

namespace mk {

void put_u8(vector<uint8_t>& buf, uint8_t v) { buf.push_back(v); }

void put_u16(vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v >> 8));
    buf.push_back(static_cast<uint8_t>(v));
}

void put_u32(vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v >> 24));
    buf.push_back(static_cast<uint8_t>(v >> 16));
    buf.push_back(static_cast<uint8_t>(v >> 8));
    buf.push_back(static_cast<uint8_t>(v));
}

void put_u64(vector<uint8_t>& buf, uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        buf.push_back(static_cast<uint8_t>(v >> shift));
    }
}

void put_i16(vector<uint8_t>& buf, int16_t v) {
    put_u16(buf, static_cast<uint16_t>(v));
}

void put_i64(vector<uint8_t>& buf, int64_t v) {
    put_u64(buf, static_cast<uint64_t>(v));
}

void put_bytes(vector<uint8_t>& buf, const uint8_t* data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

void put_string(vector<uint8_t>& buf, const string& s) {
    put_u16(buf, static_cast<uint16_t>(s.size()));
    put_bytes(buf, reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

void put_blob(vector<uint8_t>& buf, const vector<uint8_t>& b) {
    put_u32(buf, static_cast<uint32_t>(b.size()));
    put_bytes(buf, b.data(), b.size());
}

void Reader::need(size_t n) const {
    if (remaining() < n) {
        throw DecodeError("buffer underflow: need " + to_string(n) +
                           " bytes, have " + to_string(remaining()));
    }
}

uint8_t Reader::get_u8() {
    need(1);
    return data_[pos_++];
}

uint16_t Reader::get_u16() {
    need(2);
    uint16_t v = (static_cast<uint16_t>(data_[pos_]) << 8) | data_[pos_ + 1];
    pos_ += 2;
    return v;
}

uint32_t Reader::get_u32() {
    need(4);
    uint32_t v = (static_cast<uint32_t>(data_[pos_]) << 24) |
                 (static_cast<uint32_t>(data_[pos_ + 1]) << 16) |
                 (static_cast<uint32_t>(data_[pos_ + 2]) << 8) |
                 static_cast<uint32_t>(data_[pos_ + 3]);
    pos_ += 4;
    return v;
}

uint64_t Reader::get_u64() {
    need(8);
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | data_[pos_ + i];
    }
    pos_ += 8;
    return v;
}

int16_t Reader::get_i16() { return static_cast<int16_t>(get_u16()); }
int64_t Reader::get_i64() { return static_cast<int64_t>(get_u64()); }

string Reader::get_string() {
    uint16_t n = get_u16();
    need(n);
    string s(reinterpret_cast<const char*>(data_ + pos_), n);
    pos_ += n;
    return s;
}

vector<uint8_t> Reader::get_blob() {
    uint32_t n = get_u32();
    need(n);
    vector<uint8_t> b(data_ + pos_, data_ + pos_ + n);
    pos_ += n;
    return b;
}

const uint8_t* Reader::get_raw(size_t n) {
    need(n);
    const uint8_t* p = data_ + pos_;
    pos_ += n;
    return p;
}

}  // namespace mk
