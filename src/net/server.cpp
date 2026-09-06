#include "net/server.h"

#include <iostream>
#include <thread>

using namespace std;

namespace mk {

TcpServer::TcpServer(string host, uint16_t port, Handler handler)
    : host_(move(host)), port_(port), handler_(move(handler)) {}

void TcpServer::run() {
    listener_ = Socket::listen(host_, port_);
    running_ = true;
    while (running_) {
        Socket client = listener_.accept();
        thread(&TcpServer::session, this, move(client)).detach();
    }
}

void TcpServer::stop() { running_ = false; }

void TcpServer::session(Socket client) {
    try {
        while (true) {
            RequestFrame req = read_request_frame(client);
            ResponseFrame resp = handler_(req);
            write_response_frame(client, resp);
        }
    } catch (const exception& e) {
        // Connection closed or malformed frame — end this session quietly.
        cerr << "session ended: " << e.what() << "\n";
    }
}

}  // namespace mk
