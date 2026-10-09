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

struct Client {
    socket_t fd{INVALID_SOCK};
    std::string ip;
    uint16_t port{0};
    RespParser parser;
    std::string write_buf;
    uint64_t last_active_time{0};
    bool close_after_write{false};

    Client(socket_t sock, std::string client_ip, uint16_t client_port);
    ~Client();

    void write(std::string_view data);
};

class EventLoop {
public:
    using CommandHandler = std::function<void(Client&, const std::vector<std::string>&)>;
    using TickHandler = std::function<void()>;

    EventLoop(std::string bind_ip = "0.0.0.0", int port = 6379);
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    bool init();
    void run();
    void stop();

    void on_command(CommandHandler handler) { command_handler_ = std::move(handler); }
    void on_tick(TickHandler handler) { tick_handler_ = std::move(handler); }
    void set_idle_timeout(uint32_t seconds) { idle_timeout_sec_ = seconds; }

    size_t client_count() const { return clients_.size(); }
    void close(socket_t fd);

private:
    std::string bind_ip_;
    int port_;
    socket_t server_fd_{INVALID_SOCK};
    bool running_{false};
    uint32_t idle_timeout_sec_{0};

    CommandHandler command_handler_;
    TickHandler tick_handler_;

    std::unordered_map<socket_t, std::unique_ptr<Client>> clients_;

#ifndef _WIN32
    int epoll_fd_{-1};
    void update_epoll(socket_t fd, uint32_t events, int op);
    void poll_events();
#else
    void poll_events();
#endif

    bool set_nonblocking(socket_t fd);
    bool set_nodelay(socket_t fd);
    void accept_all();
    void read_client(Client& client);
    void write_client(Client& client);
    void prune_idle();
};

}
