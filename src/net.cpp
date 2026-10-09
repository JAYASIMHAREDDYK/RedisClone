#include "net.h"
#include "common.h"
#include <cstring>
#include <vector>

#ifdef _MSC_VER
#pragma comment(lib, "ws2_32.lib")
#endif

using std::string;
using std::string_view;
using std::vector;
using std::make_unique;

namespace redis {

Client::Client(socket_t sock, string client_ip, uint16_t client_port)
    : fd(sock), ip(std::move(client_ip)), port(client_port), last_active_time(unix_time_sec()) {}

Client::~Client() {
    if (fd != INVALID_SOCK) {
#ifdef _WIN32
        closesocket(fd);
#else
        ::close(fd);
#endif
        fd = INVALID_SOCK;
    }
}

void Client::write(string_view data) {
    write_buf.append(data);
}

EventLoop::EventLoop(string bind_ip, int port)
    : bind_ip_(std::move(bind_ip)), port_(port) {
#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
}

EventLoop::~EventLoop() {
    stop();
#ifdef _WIN32
    WSACleanup();
#endif
}

bool EventLoop::set_nonblocking(socket_t fd) {
#ifdef _WIN32
    u_long mode = 1;
    return ioctlsocket(fd, FIONBIO, &mode) == 0;
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

bool EventLoop::set_nodelay(socket_t fd) {
    int val = 1;
#ifdef _WIN32
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&val), sizeof(val)) == 0;
#else
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == 0;
#endif
}

bool EventLoop::init() {
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == INVALID_SOCK) return false;

    int reuse = 1;
#ifdef _WIN32
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
#else
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
#endif

    set_nonblocking(server_fd_);
    set_nodelay(server_fd_);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    inet_pton(AF_INET, bind_ip_.c_str(), &addr.sin_addr);

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
#ifdef _WIN32
        closesocket(server_fd_);
#else
        ::close(server_fd_);
#endif
        server_fd_ = INVALID_SOCK;
        return false;
    }

    if (::listen(server_fd_, 1024) != 0) {
#ifdef _WIN32
        closesocket(server_fd_);
#else
        ::close(server_fd_);
#endif
        server_fd_ = INVALID_SOCK;
        return false;
    }

#ifndef _WIN32
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        ::close(server_fd_);
        server_fd_ = INVALID_SOCK;
        return false;
    }
    update_epoll(server_fd_, EPOLLIN | EPOLLET, EPOLL_CTL_ADD);
#endif

    return true;
}

#ifndef _WIN32
void EventLoop::update_epoll(socket_t fd, uint32_t events, int op) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    epoll_ctl(epoll_fd_, op, fd, &ev);
}
#endif

void EventLoop::accept_all() {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);

        socket_t client_fd = ::accept(server_fd_, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
        if (client_fd == INVALID_SOCK) break;

        set_nonblocking(client_fd);
        set_nodelay(client_fd);

        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
        uint16_t client_port = ntohs(client_addr.sin_port);

        clients_[client_fd] = make_unique<Client>(client_fd, ip_str, client_port);

#ifndef _WIN32
        update_epoll(client_fd, EPOLLIN | EPOLLET, EPOLL_CTL_ADD);
#endif
    }
}

void EventLoop::read_client(Client& client) {
    char buf[16384];
    bool disconnect = false;

    // Edge-triggered drain: loop recv() until EAGAIN/EWOULDBLOCK
    while (true) {
#ifdef _WIN32
        int n = recv(client.fd, buf, sizeof(buf), 0);
        if (n == SOCKET_ERROR) {
            if (WSAGetLastError() == WSAEWOULDBLOCK) break;
            disconnect = true;
            break;
        }
#else
        int n = ::recv(client.fd, buf, sizeof(buf), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            disconnect = true;
            break;
        }
#endif
        if (n == 0) {
            disconnect = true;
            break;
        }

        client.last_active_time = unix_time_sec();
        client.parser.feed(buf, n);

        vector<string> args;
        while (client.parser.next_command(args)) {
            if (command_handler_) {
                command_handler_(client, args);
            }
        }
    }

    if (disconnect) {
        close(client.fd);
    } else if (!client.write_buf.empty()) {
        write_client(client);
    }
}

void EventLoop::write_client(Client& client) {
    while (!client.write_buf.empty()) {
#ifdef _WIN32
        int n = send(client.fd, client.write_buf.data(), static_cast<int>(client.write_buf.size()), 0);
        if (n == SOCKET_ERROR) {
            if (WSAGetLastError() == WSAEWOULDBLOCK) break;
            close(client.fd);
            return;
        }
#else
        int n = ::send(client.fd, client.write_buf.data(), client.write_buf.size(), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close(client.fd);
            return;
        }
#endif
        if (n > 0) {
            client.write_buf.erase(0, n);
        }
    }

#ifndef _WIN32
    if (!client.write_buf.empty()) {
        update_epoll(client.fd, EPOLLIN | EPOLLOUT | EPOLLET, EPOLL_CTL_MOD);
    } else {
        update_epoll(client.fd, EPOLLIN | EPOLLET, EPOLL_CTL_MOD);
    }
#endif

    if (client.write_buf.empty() && client.close_after_write) {
        close(client.fd);
    }
}

void EventLoop::close(socket_t fd) {
#ifndef _WIN32
    if (epoll_fd_ >= 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    }
#endif
    clients_.erase(fd);
}

void EventLoop::prune_idle() {
    if (idle_timeout_sec_ == 0) return;

    uint64_t now = unix_time_sec();
    vector<socket_t> to_close;

    for (const auto& [fd, client] : clients_) {
        if (now - client->last_active_time >= idle_timeout_sec_) {
            to_close.push_back(fd);
        }
    }

    for (socket_t fd : to_close) {
        close(fd);
    }
}

#ifndef _WIN32
void EventLoop::poll_events() {
    constexpr int MAX_EVENTS = 1024;
    epoll_event events[MAX_EVENTS];

    int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 50);
    for (int i = 0; i < num_events; ++i) {
        int fd = events[i].data.fd;
        uint32_t ev = events[i].events;

        if (fd == server_fd_) {
            accept_all();
            continue;
        }

        auto it = clients_.find(fd);
        if (it == clients_.end()) continue;

        if (ev & (EPOLLERR | EPOLLHUP)) {
            close(fd);
            continue;
        }

        if (ev & EPOLLIN) {
            read_client(*(it->second));
        }

        if (clients_.count(fd) && (ev & EPOLLOUT)) {
            write_client(*(it->second));
        }
    }
}
#else
void EventLoop::poll_events() {
    vector<WSAPOLLFD> fds;
    fds.reserve(clients_.size() + 1);

    WSAPOLLFD server_poll{};
    server_poll.fd = server_fd_;
    server_poll.events = POLLIN;
    fds.push_back(server_poll);

    for (const auto& [fd, client] : clients_) {
        WSAPOLLFD pfd{};
        pfd.fd = fd;
        pfd.events = POLLIN | (client->write_buf.empty() ? 0 : POLLOUT);
        fds.push_back(pfd);
    }

    int ret = WSAPoll(fds.data(), static_cast<ULONG>(fds.size()), 50);
    if (ret <= 0) return;

    if (fds[0].revents & POLLIN) {
        accept_all();
    }

    for (size_t i = 1; i < fds.size(); ++i) {
        socket_t client_fd = fds[i].fd;
        short revents = fds[i].revents;

        auto it = clients_.find(client_fd);
        if (it == clients_.end()) continue;

        if (revents & (POLLERR | POLLHUP | POLLNVAL)) {
            close(client_fd);
            continue;
        }

        if (revents & POLLIN) {
            read_client(*(it->second));
        }

        if (clients_.count(client_fd) && (revents & POLLOUT)) {
            write_client(*(it->second));
        }
    }
}
#endif

void EventLoop::run() {
    running_ = true;
    uint64_t last_idle_check = monotonic_time_ms();

    while (running_) {
        poll_events();

        if (tick_handler_) {
            tick_handler_();
        }

        uint64_t now = monotonic_time_ms();
        if (now - last_idle_check >= 5000) {
            prune_idle();
            last_idle_check = now;
        }
    }
}

void EventLoop::stop() {
    running_ = false;
    clients_.clear();

    if (server_fd_ != INVALID_SOCK) {
#ifdef _WIN32
        closesocket(server_fd_);
#else
        ::close(server_fd_);
#endif
        server_fd_ = INVALID_SOCK;
    }

#ifndef _WIN32
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
#endif
}

}
