#include "storage/record.h"

#include "common/crc32.h"

using namespace std;

namespace mk {

void encode_record(const Record& rec, vector<uint8_t>& buf) {
    vector<uint8_t> body;  // temperorary buffer to hold the record body 

    put_u64(body, rec.offset);
    put_u64(body, rec.timestamp);
    put_blob(body, rec.key);
    put_blob(body, rec.value);

    uint32_t crc = crc32(body.data(), body.size());
    uint32_t totalLen = static_cast<uint32_t>(4 + body.size());  // crc32 field + body

    put_u32(buf, totalLen);
    put_u32(buf, crc);
    
    buf.insert(buf.end(), body.begin(), body.end());
}

Record decode_record(Reader& r) {
    uint32_t totalLen = r.get_u32();
    const uint8_t* frame = r.get_raw(totalLen);  // throws DecodeError if torn/incomplete

    Reader body(frame, totalLen);
    uint32_t storedCrc = body.get_u32();
    const uint8_t* bodyStart = frame + 4;
    size_t bodyLen = totalLen - 4;
    uint32_t actualCrc = crc32(bodyStart, bodyLen);
    if (actualCrc != storedCrc) {
        throw DecodeError("record CRC mismatch (torn write or corruption)");
    }

    Record rec;
    rec.offset = body.get_u64();
    rec.timestamp = body.get_u64();
    rec.key = body.get_blob();
    rec.value = body.get_blob();
    return rec;
}

}  // namespace mk
