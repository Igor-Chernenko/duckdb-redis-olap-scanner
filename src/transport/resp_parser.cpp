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

    cursor = line_end + 2;
    return value;
}

void RespParser::ParseBuffer(const char* buffer, size_t length) {
    const char* cursor = buffer;
    const char* end = buffer + length;

    while (cursor < end) {
        try {
            RespObjects.push_back(ParseNext(cursor, end));
        } catch (...) {
            break;
        }
    }
}

RespObject RespParser::ParseNext(const char*& cursor, const char* end) {
    if (cursor >= end) return {};

    char typeByte = *cursor;
    cursor++;

    RespObject obj;

    switch (typeByte) {
        case ':': {
            obj.type = RespType::INT;
            obj.int_val = ParseNumeric<int>(cursor, end);
            break;
        }
        case ',': {
            obj.type = RespType::DOUBLE;
            obj.double_val = ParseNumeric<double>(cursor, end);
            break;
        }
        case '+': {
            obj.type = RespType::SIMPLE_STRING;
            const char* string_end = std::strstr(cursor, "\r\n");
            if (!string_end || string_end > end) {
                throw std::runtime_error("Incomplete buffer");
            }
            obj.str_view.len = string_end - cursor;
            obj.str_view.ptr = cursor;

            cursor = string_end + 2;
            break;
    }
        case '-': {
            obj.type = RespType::ERROR;
            const char* line_end = std::strstr(cursor, "\r\n");
            if (!line_end || line_end > end)
                throw std::runtime_error("Incomplete buffer");

            obj.str_view.ptr = cursor;
            obj.str_view.len = line_end - cursor;
            cursor = line_end + 2;
            break;
        }
        case '(': {
            obj.type = RespType::BIG_NUMBER;
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
            int64_t len = ParseNumeric<int64_t>(cursor, end);
            if (len == -1) {
                obj.type = RespType::NULL_VAL;
            } else {
                obj.str_view.ptr = cursor;
                obj.str_view.len = len;
                cursor += len + 2;
            }
            break;
        }
        case '*': {
            obj.type = RespType::ARRAY;
            int64_t count = ParseNumeric<int64_t>(cursor, end);
            for (int i = 0; i < count; ++i) {
                obj.children.push_back(ParseNext(cursor, end));
            }
            break;
        }
        case '#': {
            obj.type = RespType::BOOL;
            if (*cursor == 't') {
                obj.int_val = 1;
            } else if (*cursor == 'f') {
                obj.int_val = 0;
            } else {
                throw std::runtime_error("Invalid boolean format");
            }
            cursor += 3;
            break;
        }
    }

    return obj;
}

std::vector<RespObject> RespParser::GetObjects() {
    return RespObjects;
}

void RespParser::SqlToResp(std::string& query) {
    for (char& c : query) {
        if (c == '_') {
            c = '?';
        } else if (c == '?') {
            c = '*';
        }
    }
}

std::string RespParser::BuildScan(const std::string& cursor,
                                  const std::string& pattern) {
    std::string cmd;

    cmd += "*6\r\n";
    cmd += "$4\r\nSCAN\r\n";
    cmd += "$" + std::to_string(cursor.length()) + "\r\n" +
           cursor + "\r\n";

    cmd += "$5\r\nMATCH\r\n";
    cmd += "$" + std::to_string(pattern.length()) + "\r\n" +
           pattern + "\r\n";

    cmd += "$5\r\nCOUNT\r\n";
    cmd += "$4\r\n2048\r\n";

    return cmd;
}

// testing functions ------------------------------------------------------------------

void RespParser::PrintIndent(int indent) {
    for (int i = 0; i < indent; ++i)
        std::cout << "  ";
}

void RespParser::PrintResp(const RespObject& obj, int indent) {
    PrintIndent(indent);

    switch (obj.type) {
        case RespType::INT:
            std::cout << "[INT] " << obj.int_val << "\n";
            break;

        case RespType::BOOL:
            std::cout << "[BOOL] "
                      << (obj.int_val ? "true" : "false")
                      << "\n";
            break;

        case RespType::DOUBLE:
            std::cout << "[DOUBLE] " << obj.double_val << "\n";
            break;

        case RespType::SIMPLE_STRING:
            std::cout << "[SIMPLE] " << obj.AsString() << "\n";
            break;

        case RespType::BULK_STRING:
            std::cout << "[BULK] " << obj.AsString() << "\n";
            break;

        case RespType::ERROR:
            std::cout << "[ERROR] \033[31m"
                      << obj.AsString()
                      << "\033[0m\n";
            break;

        case RespType::NULL_VAL:
            std::cout << "[NULL]\n";
            break;

        case RespType::ARRAY:
            std::cout << "[ARRAY] Size: "
                      << obj.children.size()
                      << " {\n";
            for (const auto& child : obj.children) {
                PrintResp(child, indent + 2);
            }
            PrintIndent(indent);
            std::cout << "}\n";
            break;

        default:
            std::cout << "[UNKNOWN TYPE]\n";
            break;
    }
}
