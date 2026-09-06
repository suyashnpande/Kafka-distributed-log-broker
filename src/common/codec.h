#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

// Big-endian wire/disk codec shared by storage (record.cpp) and protocol
// (requests.cpp). All multi-byte integers are big-endian. Strings use a
// 2-byte length prefix; raw byte blobs use a 4-byte length prefix.

namespace mk {

struct DecodeError : std::runtime_error {
    explicit DecodeError(const std::string& what) : std::runtime_error(what) {}
};

void put_u8(std::vector<uint8_t>& buf, uint8_t v);
void put_u16(std::vector<uint8_t>& buf, uint16_t v);
void put_u32(std::vector<uint8_t>& buf, uint32_t v);
void put_u64(std::vector<uint8_t>& buf, uint64_t v);
void put_i16(std::vector<uint8_t>& buf, int16_t v);
void put_i64(std::vector<uint8_t>& buf, int64_t v);
void put_bytes(std::vector<uint8_t>& buf, const uint8_t* data, size_t len);
void put_string(std::vector<uint8_t>& buf, const std::string& s);       // 2-byte len prefix
void put_blob(std::vector<uint8_t>& buf, const std::vector<uint8_t>& b);  // 4-byte len prefix

// Sequential reader over a fixed buffer. Every getter throws DecodeError if
// the buffer doesn't have enough remaining bytes — callers (protocol/broker)
// catch this at the request boundary rather than every call site.
class Reader {
public:
    Reader(const uint8_t* data, size_t len) : data_(data), len_(len), pos_(0) {}

    uint8_t get_u8();
    uint16_t get_u16();
    uint32_t get_u32();
    uint64_t get_u64();
    int16_t get_i16();
    int64_t get_i64();
    std::string get_string();
    std::vector<uint8_t> get_blob();
    const uint8_t* get_raw(size_t n);  // returns pointer to n raw bytes, advances

    size_t remaining() const { return len_ - pos_; }
    size_t pos() const { return pos_; }

private:
    void need(size_t n) const;

    const uint8_t* data_;
    size_t len_;
    size_t pos_;
};

}  // namespace mk
