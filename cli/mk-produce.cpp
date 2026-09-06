#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/murmur2.h"
#include "net/socket.h"
#include "protocol/requests.h"
#include "protocol/types.h"

using namespace std;
using namespace mk;

namespace {

void usage() {
    cerr << "usage: mk-produce --broker HOST:PORT --topic T [--key K] "
            "[--value V] [--file PATH] [--acks N] [--partition N]\n";
}

// Kafka's default partitioner: (murmur2(key) & 0x7fffffff) % partitionCount.
uint32_t partition_for_key(const string& key, uint32_t partitionCount) {
    int32_t h = murmur2(reinterpret_cast<const uint8_t*>(key.data()), key.size());
    return static_cast<uint32_t>(h & 0x7fffffff) % partitionCount;
}

// mk-produce is one-shot (one record per process, per the M2 CLI design), so
// there's no persistent producer session to hold a round-robin cursor across
// calls — uniform-random-per-call is the pragmatic stand-in for "round robin
// when no key" here.
uint32_t random_partition(uint32_t partitionCount) {
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<uint32_t> dist(0, partitionCount - 1);
    return dist(gen);
}

pair<string, uint16_t> split_broker(const string& hostPort) {
    size_t colon = hostPort.rfind(':');
    if (colon == string::npos) {
        cerr << "--broker must be HOST:PORT\n";
        exit(1);
    }
    string host = hostPort.substr(0, colon);
    uint16_t port = static_cast<uint16_t>(stoul(hostPort.substr(colon + 1)));
    return {host, port};
}

vector<uint8_t> read_file(const string& path) {
    ifstream in(path, ios::binary);
    if (!in) {
        cerr << "cannot open file: " << path << "\n";
        exit(1);
    }
    return vector<uint8_t>(istreambuf_iterator<char>(in), istreambuf_iterator<char>());
}

constexpr int kMaxProduceAttempts = 10;
constexpr uint32_t kInitialBackoffMs = 50;
constexpr uint32_t kMaxBackoffMs = 2000;

// Re-resolves the current leader for partitionId via a fresh Metadata call
// against the original --broker entry point. Returns nullopt if even that
// lookup fails (e.g. the entry point itself is down) — callers keep using
// whatever address they already had rather than giving up outright.
optional<pair<string, uint16_t>> resolve_leader(const string& host, uint16_t port,
                                                  const string& topic, uint32_t partitionId) {
    try {
        Socket sock = Socket::connect(host, port);
        MetadataRequest req{topic};
        vector<uint8_t> payload;
        encode_metadata_request(req, payload);
        write_request_frame(sock, RequestFrame{static_cast<uint16_t>(RequestType::Metadata), 1, payload});
        ResponseFrame respFrame = read_response_frame(sock);
        Reader r(respFrame.payload.data(), respFrame.payload.size());
        MetadataResponse resp = decode_metadata_response(r);
        if (resp.errorCode != ErrorCode::None) return nullopt;
        for (const PartitionMetadata& pm : resp.partitions) {
            if (pm.partitionId == partitionId && !pm.leaderHost.empty()) {
                return make_pair(pm.leaderHost, static_cast<uint16_t>(pm.leaderPort));
            }
        }
    } catch (const exception&) {
    }
    return nullopt;
}

}  // namespace

int main(int argc, char** argv) {
    string brokerAddr, topic, key, value, filePath;
    uint8_t acks = 1;
    bool haveKey = false;
    bool havePartitionOverride = false;
    uint32_t partitionOverride = 0;

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
        else if (arg == "--key") { key = next("--key"); haveKey = true; }
        else if (arg == "--value") value = next("--value");
        else if (arg == "--file") filePath = next("--file");
        else if (arg == "--acks") acks = static_cast<uint8_t>(stoul(next("--acks")));
        else if (arg == "--partition") {
            partitionOverride = static_cast<uint32_t>(stoul(next("--partition")));
            havePartitionOverride = true;
        } else {
            cerr << "unknown argument: " << arg << "\n";
            usage();
            return 1;
        }
    }

    if (brokerAddr.empty() || topic.empty()) {
        usage();
        return 1;
    }

    Record rec;
    rec.key.assign(key.begin(), key.end());
    if (!filePath.empty()) {
        rec.value = read_file(filePath);
    } else {
        rec.value.assign(value.begin(), value.end());
    }

    auto [host, port] = split_broker(brokerAddr);
    uint32_t correlationId = 1;

    uint32_t partitionId = partitionOverride;
    // Best guess at who actually leads this partition. In cluster mode,
    // Metadata's PartitionMetadata already carries the real leader's address
    // (in standalone mode it's always this same broker), so we can usually
    // connect directly instead of guessing wrong and needing a redirect.
    string produceHost = host;
    uint16_t producePort = port;
    if (!havePartitionOverride) {
        try {
            Socket metaSock = Socket::connect(host, port);
            MetadataRequest metaReq{topic};
            vector<uint8_t> metaPayload;
            encode_metadata_request(metaReq, metaPayload);
            write_request_frame(metaSock, RequestFrame{static_cast<uint16_t>(RequestType::Metadata),
                                                         correlationId++, metaPayload});
            ResponseFrame metaRespFrame = read_response_frame(metaSock);
            Reader metaR(metaRespFrame.payload.data(), metaRespFrame.payload.size());
            MetadataResponse metaResp = decode_metadata_response(metaR);

            if (metaResp.errorCode != ErrorCode::None || metaResp.partitions.empty()) {
                cerr << "metadata lookup failed, errorCode="
                     << static_cast<int>(metaResp.errorCode) << "\n";
                return 1;
            }
            uint32_t partitionCount = static_cast<uint32_t>(metaResp.partitions.size());
            partitionId = haveKey ? partition_for_key(key, partitionCount) : random_partition(partitionCount);

            for (const PartitionMetadata& pm : metaResp.partitions) {
                if (pm.partitionId == partitionId && !pm.leaderHost.empty()) {
                    produceHost = pm.leaderHost;
                    producePort = static_cast<uint16_t>(pm.leaderPort);
                    break;
                }
            }
        } catch (const exception& e) {
            cerr << "cannot reach " << host << ":" << port << " (" << e.what() << ")\n";
            return 1;
        }
    }

    ProduceRequest req;
    req.topic = topic;
    req.partitionId = partitionId;
    req.acks = acks;
    req.records.push_back(move(rec));

    vector<uint8_t> payload;
    encode_produce_request(req, payload);

    // M6: resilient retry across a leader failover. A dead redirect target
    // (Socket::connect/write/read throwing) is just as expected here as a
    // clean NotLeaderForPartition — both mean "try again elsewhere" — and
    // exponential backoff (capped) gives the cluster time to actually elect
    // and broadcast a new leader (kAliveThresholdMs=600ms detection +
    // ClusterController::check_partition_failover, see cluster/controller.cpp)
    // instead of exhausting attempts before that window even closes.
    uint32_t backoffMs = kInitialBackoffMs;
    for (int attempt = 0; attempt < kMaxProduceAttempts; ++attempt) {
        try {
            Socket sock = Socket::connect(produceHost, producePort);
            write_request_frame(sock, RequestFrame{static_cast<uint16_t>(RequestType::Produce),
                                                     correlationId++, payload});
            ResponseFrame respFrame = read_response_frame(sock);

            Reader r(respFrame.payload.data(), respFrame.payload.size());
            ProduceResponse resp = decode_produce_response(r);

            if (resp.errorCode == ErrorCode::None) {
                cout << "produced to partition " << partitionId << " at offset " << resp.baseOffset
                     << "\n";
                return 0;
            }
            if (resp.errorCode == ErrorCode::NotLeaderForPartition && !resp.leaderHost.empty()) {
                cerr << "redirecting to partition leader at " << resp.leaderHost << ":"
                     << resp.leaderPort << "\n";
                produceHost = resp.leaderHost;
                producePort = static_cast<uint16_t>(resp.leaderPort);
                continue;  // valid redirect — worth trying right away, no backoff needed
            }
            cerr << "produce attempt " << (attempt + 1) << " failed, errorCode="
                 << static_cast<int>(resp.errorCode) << " — retrying\n";
        } catch (const exception& e) {
            cerr << "produce attempt " << (attempt + 1) << " could not reach " << produceHost << ":"
                 << producePort << " (" << e.what() << ") — retrying\n";
        }

        this_thread::sleep_for(chrono::milliseconds(backoffMs));
        backoffMs = min(backoffMs * 2, kMaxBackoffMs);
        if (auto resolved = resolve_leader(host, port, topic, partitionId)) {
            produceHost = resolved->first;
            producePort = resolved->second;
        }
    }
    cerr << "gave up after " << kMaxProduceAttempts << " attempts\n";
    return 1;
}
