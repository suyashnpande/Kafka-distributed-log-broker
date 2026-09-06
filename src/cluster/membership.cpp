#include "cluster/membership.h"

using namespace std;

namespace mk {

ClusterMembership::ClusterMembership(uint32_t selfId, map<uint32_t, Address> peers)
    : selfId_(selfId), peers_(move(peers)) {
    for (const auto& [id, addr] : peers_) {
        (void)addr;
        if (id != selfId_) conns_.emplace(id, make_unique<PeerConn>());
    }
}

vector<uint32_t> ClusterMembership::all_ids() const {
    vector<uint32_t> ids;
    ids.reserve(peers_.size());
    for (const auto& [id, addr] : peers_) {
        (void)addr;
        ids.push_back(id);
    }
    return ids;  // map iterates in ascending key order
}

optional<Address> ClusterMembership::address_of(uint32_t id) const {
    auto it = peers_.find(id);
    if (it == peers_.end()) return nullopt;
    return it->second;
}

optional<ResponseFrame> ClusterMembership::call(uint32_t peerId, const RequestFrame& req) {
    auto connIt = conns_.find(peerId);
    auto addrIt = peers_.find(peerId);
    if (connIt == conns_.end() || addrIt == peers_.end()) return nullopt;

    PeerConn& pc = *connIt->second;
    lock_guard<mutex> lock(pc.mu);

    for (int attempt = 0; attempt < 2; ++attempt) {
        if (!pc.sock.valid()) {
            try {
                pc.sock = Socket::connect(addrIt->second.host, addrIt->second.port);
            } catch (const exception&) {
                return nullopt;
            }
        }
        try {
            write_request_frame(pc.sock, req);
            return read_response_frame(pc.sock);
        } catch (const exception&) {
            pc.sock = Socket();  // drop the dead connection; retry once with a fresh one
        }
    }
    return nullopt;
}

}  // namespace mk
