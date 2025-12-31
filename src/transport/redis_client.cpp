/*
  redis_client.cpp

*/
#include "transport/redis_client.hpp"
#include "transport/resp_parser.hpp"
#include "transport/socket_os.hpp"
#include <stdexcept>
#include <cstring>

RedisClient::RedisClient(){
  sock_fd = INVALID_SOCKET;
  is_connected = false;
  buffer_capacity = BUFFER_SIZE;
  current_offset = 0;

  if (!init_sockets()) {
    throw std::runtime_error("Failed to initialize socket library.");
  }

  try{
    buffer = new char[BUFFER_SIZE];
  }catch(...){
    cleanup_sockets();
    throw std::runtime_error("Not able to allocate Memory");
  }
}
RedisClient::~RedisClient(){
  if(sock_fd != INVALID_SOCKET){
    CLOSE_SOCKET(sock_fd);
  }
  delete[] buffer;
  cleanup_sockets();
}

void RedisClient::EnsureBufferSize(size_t needed_size){
  if(needed_size + current_offset < buffer_capacity){
    return;
  }
  size_t new_capacity = std::max(buffer_capacity * 2, current_offset + needed_size);

  char *new_buffer = new char[new_capacity];
  std::memcpy(new_buffer, buffer, current_offset);
  delete[] buffer;

  buffer = new_buffer;
  buffer_capacity = new_capacity;
}

bool RedisClient::Connect(){
  // Create socket
  sock_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (sock_fd == INVALID_SOCKET) {
    std::cerr << "ERROR: Socket creation failed error: " << GET_SOCKET_ERROR() << "\n";
    return false;
  }
  // Address structure
  sockaddr_in server_addr{}; // Initialize to zero
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
      return false;
  }

  //connect
  set_socket_timeout(sock_fd, connection_timeout);
  int conn_result = connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
  if (conn_result < 0) {
    std::cerr << "ERROR: Connection failed error: " << GET_SOCKET_ERROR() << "\n";
    CLOSE_SOCKET(sock_fd);
    sock_fd = INVALID_SOCKET;
    return false;
  }

  is_connected = true;

   std::string msg =
    "*1\r\n"
    "$4\r\n"
    "PING\r\n";
  send(sock_fd, msg.c_str(), msg.size(), 0);

  CheckedReadResponse();
  return true;
}

bool RedisClient::CheckedReadResponse(){

  int avalible_buffer_space = buffer_capacity - current_offset;
  if(avalible_buffer_space <= 5){
    throw std::runtime_error("No avalible room, memory missmanaged");
  }
  int read = recv(sock_fd, &buffer[current_offset], avalible_buffer_space-1, 0);

  if (read > 0) {
    // mark the end of the string
    buffer[current_offset + read] = '\0';
  }
  else if (read == 0) {
      std::cerr << "ERROR: Connection closed by Redis server.\n";
      return false;
  }
  else if(read == -1){
    throw std::runtime_error("error while reading response");
  }
  RespParser resp_parser;
  std::string raw_data(buffer + current_offset, read);
  resp_parser.ParseBuffer(&buffer[current_offset], read);
  std::vector<RespObject> objects = resp_parser.GetObjects();
  if (objects.empty()) {
    std::cerr << "ERROR: Parsed 0 objects. Buffer might be incomplete.\n";
    return false;
  }
  if(objects[0].AsString() != "PONG"){
    std::cerr << "ERROR: incorrect response to PING from Redis server\n";
    return false;
  }
  return true;
}