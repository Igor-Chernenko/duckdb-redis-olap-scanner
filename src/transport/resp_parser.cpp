#include "transport/resp_parser.hpp"
#include <cstring>
#include <string>
#include <charconv>
#include <iostream>
#include <stdexcept>

// reads until \r\n and returns the number found
template <typename T>
T RespParser::ParseNumeric(const char*& cursor, const char* end) {
    const char* line_end = std::strstr(cursor, "\r\n");
    if (!line_end || line_end > end) {
        throw std::runtime_error("Incomplete buffer");
    }
    T value = 0;

    auto result = std::from_chars(cursor, line_end, value);

    if (result.ec != std::errc()) {
        // Handle parsing error (e.g., invalid characters)
        // It's often good to throw here or return a default/error code
    }

    // move cursor past the number and the CRLF
    cursor = line_end + 2;
    return value;
}

void RespParser::ParseBuffer(const char* buffer, size_t length) {
  const char* cursor = buffer;
  const char* end = buffer + length;

  // While there is data left in the buffer
  while (cursor < end) {
    try {
      // Parse one top-level object (which might contain nested objects)
      RespObjects.push_back(ParseNext(cursor, end));
    } catch (...) {
      // Handle incomplete data (wait for more network bytes)
      break;
    }
  }
}

RespObject RespParser::ParseNext(const char*& cursor, const char* end) {
  if (cursor >= end) return {};

  // 1. Read the Type Byte
  char typeByte = *cursor;
  cursor++; // Move past the type byte (e.g., past '*')

  RespObject obj;

  switch (typeByte) {
    case ':':{
      obj.type = RespType::INT;
      obj.int_val = ParseNumeric<int>(cursor, end);
      break;
    }
    case ',':{
      obj.type = RespType::DOUBLE;
      obj.int_val = ParseNumeric<double>(cursor, end);
      break;
    }
    case '+': {
      obj.type = RespType::SIMPLE_STRING;
      // Simple strings are binary safe, strstr works
      const char* string_end = std::strstr(cursor, "\r\n");
      if (!string_end || string_end > end) {
          throw std::runtime_error("Incomplete buffer");
      }
      obj.str_view.len = string_end - cursor;
      obj.str_view.ptr = cursor;
      break;
    }
    case '(':{
      obj.type = RespType::BIG_NUMBER;
      // Treat BigNumber the same as simple string
      const char* string_end = std::strstr(cursor, "\r\n");
      if (!string_end || string_end > end) {
          throw std::runtime_error("Incomplete buffer");
      }
      size_t len = string_end - cursor;
      obj.str_view.len = len;
      obj.str_view.ptr = cursor;
      cursor += len + 2;
    break;
    }
    case '$': {
      obj.type = RespType::BULK_STRING;
      {
        int64_t len = ParseNumeric<int64_t>(cursor, end); // Parse length first
        if (len == -1) {
          obj.type = RespType::NULL_VAL; // Handle null bulk string
        } else {
          // Set the pointer to the start of the actual string data
          obj.str_view.ptr = cursor;
          obj.str_view.len = len;

          // Advance cursor past the st+ring data AND the trailing \r\n
          cursor += len + 2;
        }
      }
    break;
    }
    case '*': {
      obj.type = RespType::ARRAY;
      {
        int64_t count = ParseNumeric<int64_t>(cursor, end);
        // Recursively call ParseNext 'count' times
        for (int i = 0; i < count; ++i) {
          obj.children.push_back(ParseNext(cursor, end));
      }
    }
    break;
    }
    case '#':{
    obj.type = RespType::BOOL;
    // Check the character directly
    if (*cursor == 't') {
        obj.int_val = 1; // True
    } else if (*cursor == 'f') {
        obj.int_val = 0; // False
    } else {
        throw std::runtime_error("Invalid boolean format");
    }
    // Skip the 't' and the '\r\n' (3 chars total)
    cursor += 3;
    break;
    }
  }
  return obj;
}

std::vector<RespObject> RespParser::GetObjects(){
  return RespObjects;
}

//testing functions ------------------------------------------------------------------

// Helper to print indentation
void RespParser::PrintIndent(int indent) {
    for (int i = 0; i < indent; ++i) std::cout << "  ";
}

void RespParser::PrintResp(const RespObject& obj, int indent ) {
    PrintIndent(indent);

    switch (obj.type) {
        // --- Primitive Types (Leaves) ---
        case RespType::INT:
            std::cout << "[INT] " << obj.int_val << "\n";
            break;

        case RespType::BOOL:
            // Convert 1/0 to true/false for readability
            std::cout << "[BOOL] " << (obj.int_val ? "true" : "false") << "\n";
            break;

        case RespType::DOUBLE:
            std::cout << "[DOUBLE] " << obj.double_val << "\n";
            break;

        // --- String Types (Leaves) ---
        case RespType::SIMPLE_STRING:
            std::cout << "[SIMPLE] " << obj.AsString() << "\n";
            break;

        case RespType::BULK_STRING:
            std::cout << "[BULK] " << obj.AsString() << "\n";
            break;

        case RespType::ERROR:
            // ANSI Color Red (\033[31m) makes errors pop out, Reset (\033[0m)
            std::cout << "[ERROR] \033[31m" << obj.AsString() << "\033[0m\n";
            break;

        case RespType::NULL_VAL:
             std::cout << "[NULL]\n";
             break;

        // --- Recursive Types (Nodes) ---
        case RespType::ARRAY:
            std::cout << "[ARRAY] Size: " << obj.children.size() << " {\n";
            for (const auto& child : obj.children) {
                // RECURSION: Increase indent for children
                PrintResp(child, indent + 2);
            }
            PrintIndent(indent);
            std::cout << "}\n";
            break;

        // ... Add MAP/SET cases similar to ARRAY
        default:
            std::cout << "[UNKNOWN TYPE]\n";
            break;
    }
}
