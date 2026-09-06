#include <iostream>
#include <string>

#include "net/socket.h"
#include "protocol/requests.h"
#include "protocol/types.h"

using namespace std;
using namespace mk;

namespace {

void usage() {
    cerr << "usage: mk-admin create-topic --broker HOST:PORT --topic T --partitions N "
            "[--replication-factor N]\n";
}

pair<string, uint16_t> split_broker(const string& hostPort) {
    size_t colon = hostPort.rfind(':');
    if (colon == string::npos) {
        cerr << "--broker must be HOST:PORT\n";
        exit(1);
    }
    return {hostPort.substr(0, colon), static_cast<uint16_t>(stoul(hostPort.substr(colon + 1)))};
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        usage();
        return 1;
    }
    string command = argv[1];
    if (command != "create-topic") {
        cerr << "unknown command: " << command << "\n";
        usage();
        return 1;
    }

    string brokerAddr, topic;
    uint32_t partitions = 1;
    uint32_t replicationFactor = 1;

    for (int i = 2; i < argc; ++i) {
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
        else if (arg == "--partitions") partitions = static_cast<uint32_t>(stoul(next("--partitions")));
        else if (arg == "--replication-factor") {
            replicationFactor = static_cast<uint32_t>(stoul(next("--replication-factor")));
        } else {
            cerr << "unknown argument: " << arg << "\n";
            usage();
            return 1;
        }
    }

    if (brokerAddr.empty() || topic.empty() || partitions == 0) {
        usage();
        return 1;
    }

    CreateTopicRequest req{topic, partitions, replicationFactor};
    vector<uint8_t> payload;
    encode_create_topic_request(req, payload);

    auto [host, port] = split_broker(brokerAddr);

    // In cluster mode only the controller can create topics; a non-controller
    // broker redirects us there via NotLeaderForPartition + controller info.
    // Bounded to 3 attempts so a stale/flapping redirect can't hang forever.
    for (int attempt = 0; attempt < 3; ++attempt) {
        Socket sock = Socket::connect(host, port);
        write_request_frame(sock, RequestFrame{static_cast<uint16_t>(RequestType::CreateTopic), 1, payload});
        ResponseFrame respFrame = read_response_frame(sock);

        Reader r(respFrame.payload.data(), respFrame.payload.size());
        CreateTopicResponse resp = decode_create_topic_response(r);

        if (resp.errorCode == ErrorCode::NotLeaderForPartition && !resp.controllerHost.empty()) {
            cerr << "redirecting to controller at " << resp.controllerHost << ":"
                 << resp.controllerPort << "\n";
            host = resp.controllerHost;
            port = static_cast<uint16_t>(resp.controllerPort);
            continue;
        }
        if (resp.errorCode != ErrorCode::None) {
            cerr << "create-topic failed, errorCode=" << static_cast<int>(resp.errorCode) << "\n";
            return 1;
        }
        cout << "created topic \"" << topic << "\" with " << partitions
             << " partitions (replication factor " << replicationFactor << ")\n";
        return 0;
    }
    cerr << "gave up after 3 redirect attempts\n";
    return 1;
}
