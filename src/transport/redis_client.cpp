/*
  redis_client.cpp
*/

#include "transport/redis_client.hpp"
#include "transport/resp_parser.hpp"
#include "transport/socket_os.hpp"
#include <stdexcept>
#include <cstring>
#include <charconv>

RedisClient::RedisClient() {
    sock_fd = INVALID_SOCKET;
    is_connected = false;
    buffer_capacity = BUFFER_SIZE;
    current_offset = 0;

    if (!init_sockets()) {
        throw std::runtime_error("Failed to initialize socket library.");
    }

    try {
        buffer = new char[BUFFER_SIZE];
    } catch (...) {
        cleanup_sockets();
        throw std::runtime_error("Not able to allocate Memory");
    }
}

RedisClient::~RedisClient() {
    if (sock_fd != INVALID_SOCKET) {
        CLOSE_SOCKET(sock_fd);
    }
    delete[] buffer;
    cleanup_sockets();
}

void RedisClient::EnsureBufferSize(size_t needed_size) {
    if (needed_size + current_offset < buffer_capacity) {
        return;
    }

    size_t new_capacity =
        std::max(buffer_capacity * 2, current_offset + needed_size);

    char* new_buffer = new char[new_capacity];
    std::memcpy(new_buffer, buffer, current_offset);
    delete[] buffer;

    buffer = new_buffer;
    buffer_capacity = new_capacity;
}

void RedisClient::ClearBuffer(){
  current_offset = 0;
}

bool RedisClient::Connect(const char* host, int port) {
    if (sock_fd != INVALID_SOCKET) {
        CLOSE_SOCKET(sock_fd);
        sock_fd = INVALID_SOCKET;
    }
    is_connected = false;

    // Create socket
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd == INVALID_SOCKET) {
        std::cerr << "ERROR: socket() failed error: " << GET_SOCKET_ERROR() << "\n";
        return false;
    }

    // Address structure
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &server_addr.sin_addr) <= 0) {
        return false;
    }

    // connect
    set_socket_timeout(sock_fd, connection_timeout);
    int conn_result =
        connect(sock_fd,
                (struct sockaddr*)&server_addr,
                sizeof(server_addr));

    if (conn_result < 0) {
        std::cerr << "ERROR: Connection failed error: "
                  << GET_SOCKET_ERROR() << "\n";
        CLOSE_SOCKET(sock_fd);
        sock_fd = INVALID_SOCKET;
        return false;
    }

    is_connected = true;

    std::string msg =
        "*1\r\n"
        "$4\r\n"
        "PING\r\n";

    CheckedSend(msg);
    RespParser resp_parser;
    std::vector<RespObject> objects = CheckedReadResponse(resp_parser);

    if (objects.empty()) {
        std::cerr << "ERROR: Parsed 0 objects. Connection not succesfull\n";
        return false;
    }

    if (objects[0].AsString() != "PONG") {
        std::cerr << "ERROR: incorrect response to PING from Redis server\n";
        return false;
    }
    ClearBuffer();
    resp_parser.ClearObjects();
    return true;
}

std::vector<RespObject> RedisClient::CheckedReadResponse(RespParser& resp_parser) {
    int avalible_buffer_space = buffer_capacity - current_offset;
    if (avalible_buffer_space <= 5) {
        throw std::runtime_error("ERROR: No avalible room, memory missmanaged");
    }

    int read =
        recv(sock_fd,
             &buffer[current_offset],
             avalible_buffer_space - 1,
             0);
    if (read > 0) {
        buffer[current_offset + read] = '\0';
    } else if (read == 0) {
      throw std::runtime_error("ERROR: Connection closed by Redis server.\n");

    } else if (read == -1) {
        throw std::runtime_error("ERROR: error while reading response");
    }

    std::string raw_data(buffer + current_offset, read);
    resp_parser.ParseBuffer(&buffer[current_offset], read);
    current_offset += read;
    std::vector<RespObject> objects = resp_parser.GetObjects();

    if (objects.empty()) {
        throw std::runtime_error("ERROR: Parsed 0 objects. Buffer might be incomplete.");
    }

    return objects;
}

bool RedisClient::CheckedSend(const std::string& package) {
    if (send(sock_fd, package.c_str(), package.size(), 0) == -1) {
        std::cerr << "ERROR: Socket send failed error: " ;
        return false;
    }
    return true;
}

std::vector<std::string_view> RedisClient::RedisScan(std::string& query, RespParser& parser) {
    parser.SqlToResp(query);

    if (!is_connected) { //should be done by higher level duckdb extension function calls, leaving for now
        bool conn_result = Connect(host.c_str(), port);
        if (!conn_result) {
            std::cerr << "ERROR: connection failed, unhandeled case.\n";
            return {};
        }
    }

    int cursor = 0;
    std::vector<std::string_view> intermediate_buffer;
    while (true) {
        if (!CheckedSend(parser.BuildScan(std::to_string(cursor), query))) {
            std::cerr << "ERROR: send failed at cursor " << cursor;
            return {};
        }

        CheckedReadResponse(parser);
        auto objects = parser.GetObjects();
        if(objects.size() < 1){
          std::cerr << "ERROR: no objects passed back when expecting at least one";
          return {};
        }
        if(objects[0].AsString() == "0"){
          return {};
        }

        std::vector<RespObject>& results = objects[0].children[1].children;
        for(auto& it: results){
          intermediate_buffer.push_back(it.AsString());
        }

        std::string_view new_cursor = objects[0].children[0].AsString();
        if(new_cursor == "0"){
          break;
        }
        else{
          auto [ptr, ec] = std::from_chars(new_cursor.data(), new_cursor.data() + new_cursor.size(), cursor);
          if (ec != std::errc{}) {
            std::cerr << "ERROR: cursor update failed mid parse";
            return {};
          }
        }
    }
    return intermediate_buffer;
}

std::string_view RedisClient::RedisGet(const std::string& key, RespParser& parser){
    parser.ClearObjects();
    ClearBuffer();
    if (!is_connected) { //should be done by higher level duckdb extension function calls, leaving for now
        bool conn_result = Connect(host.c_str(), port);
        if (!conn_result) {
            std::cerr << "ERROR: connection failed, unhandeled case.\n";
            return "";
        }
    }

    std::string msg = parser.BuildGet(key);
    if(!CheckedSend(msg)){
      std::cerr << "ERROR: Could not send a Get Command for key" <<key;
      return "";
    }
    CheckedReadResponse(parser);
    auto objects = parser.GetObjects();
    if(objects.size() < 1){
      std::cerr << "ERROR: no objects passed back when expecting at least one";
      return "";
    }
    if(objects[0].type == RespType::NULL_VAL){
      std::cerr << "ERROR: Value not found, NULL type returned";
      return "";
    }
    return objects[0].AsString();
}
