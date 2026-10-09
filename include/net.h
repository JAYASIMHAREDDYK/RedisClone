#pragma once

#include "resp.h"
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <functional>
#include <cstdint>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using socket_t = SOCKET;
constexpr socket_t INVALID_SOCK = INVALID_SOCKET;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
using socket_t = int;
constexpr socket_t INVALID_SOCK = -1;
#endif

namespace redis {

struct ClientConnection {
    socket_t fd{INVALID_SOCK};
    std::string ip;
    uint16_t port{0};
    RespParser parser;
    std::string write_buffer;
    uint64_t last_active_time{0};
    bool close_after_write{false};

    ClientConnection(socket_t sock, std::string client_ip, uint16_t client_port);
    ~ClientConnection();

    void appendWrite(std::string_view data);
};

class EventReactor {
public:
    using CommandHandler = std::function<void(ClientConnection&, const std::vector<std::string>&)>;
    using PeriodicHandler = std::function<void()>;

    EventReactor(std::string bind_ip = "0.0.0.0", int port = 6379);
    ~EventReactor();

    EventReactor(const EventReactor&) = delete;
    EventReactor& operator=(const EventReactor&) = delete;

    bool init();
    void run();
    void stop();

    void setCommandHandler(CommandHandler handler) { command_handler_ = std::move(handler); }
    void setPeriodicHandler(PeriodicHandler handler) { periodic_handler_ = std::move(handler); }
    void setIdleTimeout(uint32_t seconds) { idle_timeout_sec_ = seconds; }

    size_t activeClientsCount() const { return clients_.size(); }
    void closeClient(socket_t fd);

private:
    std::string bind_ip_;
    int port_;
    socket_t server_fd_{INVALID_SOCK};
    bool running_{false};
    uint32_t idle_timeout_sec_{0};

    CommandHandler command_handler_;
    PeriodicHandler periodic_handler_;

    std::unordered_map<socket_t, std::unique_ptr<ClientConnection>> clients_;

#ifndef _WIN32
    int epoll_fd_{-1};
    void updateEpoll(socket_t fd, uint32_t events, int op);
    void handleEpollEvents();
#else
    void handlePollEvents();
#endif

    bool setNonBlocking(socket_t fd);
    bool setTcpNoDelay(socket_t fd);
    void acceptConnections();
    void readFromClient(ClientConnection& client);
    void writeToClient(ClientConnection& client);
    void checkIdleClients();
};

}
