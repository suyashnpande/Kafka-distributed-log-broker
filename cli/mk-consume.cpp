#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"
#include "protocol/requests.h"
#include "protocol/types.h"

using namespace std;
using namespace mk;

namespace {

void usage() {
    cerr << "usage: mk-consume --broker HOST:PORT --topic T [--partition N] "
            "[--from-beginning] [--offset N]\n"
            "   or: mk-consume --broker HOST:PORT --topic T --group G "
            "[--from-beginning] [--offset N]\n";
}

pair<string, uint16_t> split_broker(const string& hostPort) {
    size_t colon = hostPort.rfind(':');
    if (colon == string::npos) {
        cerr << "--broker must be HOST:PORT\n";
        exit(1);
    }
    return {hostPort.substr(0, colon), static_cast<uint16_t>(stoul(hostPort.substr(colon + 1)))};
}

constexpr uint32_t kMaxFetchBytes = 1 << 20;  // 1 MiB per poll
constexpr uint32_t kSessionTimeoutMs = 10000;

atomic<bool> g_shuttingDown{false};
void handle_sigint(int) { g_shuttingDown.store(true); }

void print_record(const Record& rec) {
    string key(rec.key.begin(), rec.key.end());
    string value(rec.value.begin(), rec.value.end());
    cout << "offset=" << rec.offset << " key=" << key << " value=" << value << "\n";
}

// ---- Direct mode: fetch one fixed partition. ----
int run_direct_mode(const string& host, uint16_t port, const string& topic,
                     uint32_t partitionId, uint64_t offset) {
    // Resolve who actually leads this partition up front (in standalone mode
    // Metadata always points back at `host:port` itself, so this is a no-op
    // there). Falls back to the given --broker if the lookup itself fails —
    // the Fetch call below will then surface a clear error either way.
    string fetchHost = host;
    uint16_t fetchPort = port;
    {
        Socket metaSock = Socket::connect(host, port);
        MetadataRequest metaReq{topic};
        vector<uint8_t> metaPayload;
        encode_metadata_request(metaReq, metaPayload);
        write_request_frame(metaSock, RequestFrame{static_cast<uint16_t>(RequestType::Metadata),
                                                     1, metaPayload});
        ResponseFrame metaRespFrame = read_response_frame(metaSock);
        Reader metaR(metaRespFrame.payload.data(), metaRespFrame.payload.size());
        MetadataResponse metaResp = decode_metadata_response(metaR);
        for (const PartitionMetadata& pm : metaResp.partitions) {
            if (pm.partitionId == partitionId && !pm.leaderHost.empty()) {
                fetchHost = pm.leaderHost;
                fetchPort = static_cast<uint16_t>(pm.leaderPort);
                break;
            }
        }
    }

    Socket sock = Socket::connect(fetchHost, fetchPort);
    uint32_t correlationId = 1;

    while (true) {
        FetchRequest req{topic, partitionId, offset, kMaxFetchBytes};
        vector<uint8_t> payload;
        encode_fetch_request(req, payload);
        write_request_frame(sock, RequestFrame{static_cast<uint16_t>(RequestType::Fetch),
                                                 correlationId++, payload});
        ResponseFrame respFrame = read_response_frame(sock);
        Reader r(respFrame.payload.data(), respFrame.payload.size());
        FetchResponse resp = decode_fetch_response(r);

        if (resp.errorCode == ErrorCode::NotLeaderForPartition && !resp.leaderHost.empty()) {
            cerr << "redirecting to partition leader at " << resp.leaderHost << ":"
                 << resp.leaderPort << "\n";
            sock = Socket::connect(resp.leaderHost, static_cast<uint16_t>(resp.leaderPort));
            continue;
        }
        if (resp.errorCode != ErrorCode::None) {
            cerr << "fetch failed, errorCode=" << static_cast<int>(resp.errorCode) << "\n";
            return 1;
        }
        if (resp.records.empty()) {
            this_thread::sleep_for(chrono::milliseconds(200));
            continue;
        }
        for (const Record& rec : resp.records) {
            print_record(rec);
            offset = rec.offset + 1;
        }
    }
}

// ---- Group mode ----

// Splits [0, partitionCount) as evenly as possible across members sorted by
// memberId — computed client-side by the group's leader, per AGENT_BRIEF.md.
map<string, vector<uint32_t>> compute_range_assignment(vector<string> members,
                                                         uint32_t partitionCount) {
    sort(members.begin(), members.end());
    map<string, vector<uint32_t>> assignment;
    if (members.empty()) return assignment;

    size_t n = members.size();
    uint32_t base = partitionCount / static_cast<uint32_t>(n);
    uint32_t extra = partitionCount % static_cast<uint32_t>(n);
    uint32_t p = 0;
    for (size_t i = 0; i < n; ++i) {
        uint32_t count = base + (i < extra ? 1 : 0);
        vector<uint32_t> parts;
        for (uint32_t k = 0; k < count; ++k) parts.push_back(p++);
        assignment[members[i]] = move(parts);
    }
    return assignment;
}

// Runs on its own socket (never shared with the main thread's connection —
// the wire protocol here is strictly request/response per connection, so two
// threads writing/reading the same socket concurrently would tear each
// other's frames apart).
void heartbeat_loop(Socket sock, string groupId, string memberId, uint32_t generationId,
                     uint32_t intervalMs, atomic<bool>* needRejoin) {
    uint32_t correlationId = 1;
    while (!needRejoin->load() && !g_shuttingDown.load()) {
        this_thread::sleep_for(chrono::milliseconds(intervalMs));
        if (needRejoin->load() || g_shuttingDown.load()) break;
        try {
            HeartbeatRequest req{groupId, memberId, generationId};
            vector<uint8_t> payload;
            encode_heartbeat_request(req, payload);
            write_request_frame(sock, RequestFrame{static_cast<uint16_t>(RequestType::Heartbeat),
                                                     correlationId++, payload});
            ResponseFrame respFrame = read_response_frame(sock);
            Reader r(respFrame.payload.data(), respFrame.payload.size());
            HeartbeatResponse resp = decode_heartbeat_response(r);
            if (resp.errorCode != ErrorCode::None) {
                needRejoin->store(true);
                break;
            }
        } catch (const exception&) {
            break;  // connection trouble — main loop's own I/O will surface it
        }
    }
}

int run_group_mode(const string& host, uint16_t port, const string& topic, const string& groupId,
                    bool haveOffsetOverride, uint64_t offsetOverride) {
    signal(SIGINT, handle_sigint);

    // Resolve the actual coordinator for this group and talk to it for every
    // group-protocol call (JoinGroup/SyncGroup/Heartbeat/CommitOffset/
    // FetchOffset) — in cluster mode that's not necessarily the broker named
    // by --broker. Without this, two consumers connecting to different
    // brokers would each join a separate, broker-local group instead of the
    // same one (harmless in M2/M3's single-broker world, a real bug once
    // there's more than one broker to pick).
    string coordHost = host;
    uint16_t coordPort = port;
    {
        Socket findSock = Socket::connect(host, port);
        FindCoordinatorRequest fcReq{groupId};
        vector<uint8_t> fcPayload;
        encode_find_coordinator_request(fcReq, fcPayload);
        write_request_frame(findSock, RequestFrame{static_cast<uint16_t>(RequestType::FindCoordinator),
                                                     1, fcPayload});
        ResponseFrame fcRespFrame = read_response_frame(findSock);
        Reader fcR(fcRespFrame.payload.data(), fcRespFrame.payload.size());
        FindCoordinatorResponse fcResp = decode_find_coordinator_response(fcR);
        if (fcResp.errorCode == ErrorCode::None && !fcResp.host.empty()) {
            coordHost = fcResp.host;
            coordPort = static_cast<uint16_t>(fcResp.port);
        }
    }

    Socket mainSock = Socket::connect(coordHost, coordPort);
    uint32_t correlationId = 1;

    while (!g_shuttingDown.load()) {
        // --- JoinGroup ---
        JoinGroupRequest joinReq{groupId, topic, kSessionTimeoutMs};
        vector<uint8_t> joinPayload;
        encode_join_group_request(joinReq, joinPayload);
        write_request_frame(mainSock, RequestFrame{static_cast<uint16_t>(RequestType::JoinGroup),
                                                     correlationId++, joinPayload});
        ResponseFrame joinRespFrame = read_response_frame(mainSock);
        Reader joinR(joinRespFrame.payload.data(), joinRespFrame.payload.size());
        JoinGroupResponse joinResp = decode_join_group_response(joinR);
        if (joinResp.errorCode != ErrorCode::None) {
            cerr << "join-group failed, errorCode=" << static_cast<int>(joinResp.errorCode) << "\n";
            return 1;
        }
        string memberId = joinResp.memberId;
        uint32_t generationId = joinResp.generationId;
        cout << "joined group \"" << groupId << "\" as " << memberId << " (generation "
             << generationId << ", leader=" << joinResp.leaderId << ")\n";

        // --- SyncGroup ---
        SyncGroupRequest syncReq;
        syncReq.groupId = groupId;
        syncReq.memberId = memberId;
        syncReq.generationId = generationId;
        if (memberId == joinResp.leaderId) {
            MetadataRequest metaReq{topic};
            vector<uint8_t> metaPayload;
            encode_metadata_request(metaReq, metaPayload);
            write_request_frame(mainSock, RequestFrame{static_cast<uint16_t>(RequestType::Metadata),
                                                         correlationId++, metaPayload});
            ResponseFrame metaRespFrame = read_response_frame(mainSock);
            Reader metaR(metaRespFrame.payload.data(), metaRespFrame.payload.size());
            MetadataResponse metaResp = decode_metadata_response(metaR);
            if (metaResp.errorCode != ErrorCode::None || metaResp.partitions.empty()) {
                cerr << "metadata lookup failed, errorCode="
                     << static_cast<int>(metaResp.errorCode) << "\n";
                return 1;
            }
            uint32_t partitionCount = static_cast<uint32_t>(metaResp.partitions.size());
            map<string, vector<uint32_t>> assignment =
                compute_range_assignment(joinResp.members, partitionCount);
            for (const auto& [id, parts] : assignment) {
                SyncGroupAssignment a;
                a.memberId = id;
                a.partitions = parts;
                syncReq.assignments.push_back(move(a));
            }
        }

        vector<uint8_t> syncPayload;
        encode_sync_group_request(syncReq, syncPayload);
        write_request_frame(mainSock, RequestFrame{static_cast<uint16_t>(RequestType::SyncGroup),
                                                     correlationId++, syncPayload});
        ResponseFrame syncRespFrame = read_response_frame(mainSock);
        Reader syncR(syncRespFrame.payload.data(), syncRespFrame.payload.size());
        SyncGroupResponse syncResp = decode_sync_group_response(syncR);
        if (syncResp.errorCode != ErrorCode::None) {
            cerr << "sync-group failed, errorCode=" << static_cast<int>(syncResp.errorCode)
                 << " — retrying join\n";
            this_thread::sleep_for(chrono::milliseconds(500));
            continue;
        }

        vector<uint32_t> myPartitions = syncResp.partitions;
        cout << "assigned partitions:";
        for (uint32_t p : myPartitions) cout << " " << p;
        if (myPartitions.empty()) cout << "(none — waiting for a future rebalance)";
        cout << "\n";

        // --- Resume offsets (unless overridden) ---
        map<uint32_t, uint64_t> partitionOffsets;
        for (uint32_t p : myPartitions) {
            if (haveOffsetOverride) {
                partitionOffsets[p] = offsetOverride;
                continue;
            }
            FetchOffsetRequest foReq{groupId, topic, p};
            vector<uint8_t> foPayload;
            encode_fetch_offset_request(foReq, foPayload);
            write_request_frame(mainSock, RequestFrame{static_cast<uint16_t>(RequestType::FetchOffset),
                                                         correlationId++, foPayload});
            ResponseFrame foRespFrame = read_response_frame(mainSock);
            Reader foR(foRespFrame.payload.data(), foRespFrame.payload.size());
            FetchOffsetResponse foResp = decode_fetch_offset_response(foR);
            partitionOffsets[p] = (foResp.offset >= 0) ? static_cast<uint64_t>(foResp.offset) : 0;
        }

        // --- Resolve per-partition leader addresses (everyone does this, not
        // just the leader — a partition's leader is not necessarily this
        // group's coordinator broker). Falls back to `host:port` for any
        // partition Metadata didn't resolve. ---
        map<uint32_t, pair<string, uint16_t>> partitionAddr;
        {
            MetadataRequest metaReq2{topic};
            vector<uint8_t> metaPayload2;
            encode_metadata_request(metaReq2, metaPayload2);
            write_request_frame(mainSock, RequestFrame{static_cast<uint16_t>(RequestType::Metadata),
                                                         correlationId++, metaPayload2});
            ResponseFrame metaRespFrame2 = read_response_frame(mainSock);
            Reader metaR2(metaRespFrame2.payload.data(), metaRespFrame2.payload.size());
            MetadataResponse metaResp2 = decode_metadata_response(metaR2);
            for (const PartitionMetadata& pm : metaResp2.partitions) {
                if (!pm.leaderHost.empty()) {
                    partitionAddr[pm.partitionId] = {pm.leaderHost, static_cast<uint16_t>(pm.leaderPort)};
                }
            }
        }
        map<uint32_t, Socket> partitionSockets;
        for (uint32_t p : myPartitions) {
            auto it = partitionAddr.find(p);
            string h = (it != partitionAddr.end()) ? it->second.first : host;
            uint16_t po = (it != partitionAddr.end()) ? it->second.second : port;
            partitionSockets[p] = Socket::connect(h, po);
        }

        // --- Heartbeat thread (own socket — see heartbeat_loop's comment) ---
        atomic<bool> needRejoin{false};
        Socket hbSock = Socket::connect(coordHost, coordPort);
        thread hbThread(heartbeat_loop, move(hbSock), groupId, memberId, generationId,
                         kSessionTimeoutMs / 3, &needRejoin);

        // --- Poll loop: round-robin Fetch across assigned partitions ---
        while (!needRejoin.load() && !g_shuttingDown.load()) {
            bool gotAny = false;
            for (uint32_t p : myPartitions) {
                FetchRequest req{topic, p, partitionOffsets[p], kMaxFetchBytes};
                vector<uint8_t> payload;
                encode_fetch_request(req, payload);
                write_request_frame(partitionSockets[p], RequestFrame{static_cast<uint16_t>(RequestType::Fetch),
                                                             correlationId++, payload});
                ResponseFrame respFrame = read_response_frame(partitionSockets[p]);
                Reader r(respFrame.payload.data(), respFrame.payload.size());
                FetchResponse resp = decode_fetch_response(r);

                if (resp.errorCode == ErrorCode::NotLeaderForPartition && !resp.leaderHost.empty()) {
                    cerr << "redirecting partition " << p << " to " << resp.leaderHost << ":"
                         << resp.leaderPort << "\n";
                    partitionSockets[p] = Socket::connect(resp.leaderHost,
                                                            static_cast<uint16_t>(resp.leaderPort));
                    continue;  // picked up on the next round
                }
                if (resp.errorCode != ErrorCode::None || resp.records.empty()) continue;

                gotAny = true;
                for (const Record& rec : resp.records) {
                    print_record(rec);
                    partitionOffsets[p] = rec.offset + 1;
                }

                CommitOffsetRequest commitReq{groupId, memberId, generationId, topic, p,
                                               partitionOffsets[p]};
                vector<uint8_t> commitPayload;
                encode_commit_offset_request(commitReq, commitPayload);
                write_request_frame(mainSock, RequestFrame{static_cast<uint16_t>(RequestType::CommitOffset),
                                                             correlationId++, commitPayload});
                read_response_frame(mainSock);  // commit failures aren't fatal here
            }
            if (!gotAny) this_thread::sleep_for(chrono::milliseconds(200));
        }

        needRejoin.store(true);  // in case we broke out via shutdown, not a rebalance
        hbThread.join();

        if (g_shuttingDown.load()) {
            LeaveGroupRequest leaveReq{groupId, memberId};
            vector<uint8_t> leavePayload;
            encode_leave_group_request(leaveReq, leavePayload);
            try {
                write_request_frame(mainSock, RequestFrame{static_cast<uint16_t>(RequestType::LeaveGroup),
                                                             correlationId++, leavePayload});
                read_response_frame(mainSock);
            } catch (const exception&) {
            }
            cout << "left group \"" << groupId << "\"\n";
            return 0;
        }

        cout << "rebalance triggered; rejoining group \"" << groupId << "\"\n";
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    string brokerAddr, topic, groupId;
    uint32_t partitionId = 0;
    uint64_t offset = 0;
    bool haveOffsetOverride = false;

    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        auto next = [&](const char* flag) -> string {
            if (i + 1 >= argc) {
                cerr << "missing value for " << flag << "\n";
                exit(1);
            }
            return argv[++i];
        };
        if (arg == "--broker") brokerAddr = next("--broker");
        else if (arg == "--topic") topic = next("--topic");
        else if (arg == "--partition") partitionId = static_cast<uint32_t>(stoul(next("--partition")));
        else if (arg == "--group") groupId = next("--group");
        else if (arg == "--from-beginning") { offset = 0; haveOffsetOverride = true; }
        else if (arg == "--offset") { offset = stoull(next("--offset")); haveOffsetOverride = true; }
        else {
            cerr << "unknown argument: " << arg << "\n";
            usage();
            return 1;
        }
    }

    if (brokerAddr.empty() || topic.empty()) {
        usage();
        return 1;
    }

    auto [host, port] = split_broker(brokerAddr);

    if (!groupId.empty()) {
        return run_group_mode(host, port, topic, groupId, haveOffsetOverride, offset);
    }
    return run_direct_mode(host, port, topic, partitionId, offset);
}
