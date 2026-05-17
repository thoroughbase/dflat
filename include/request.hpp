#pragma once

#include <string_view>

namespace dflat
{

constexpr std::string_view DFLAT_QUERY = "dflat-query";
constexpr std::string_view DFLAT_RESPONSE = "dflat-response";

constexpr std::string_view CMD_GET = "get";
constexpr std::string_view CMD_PUT = "put";
constexpr std::string_view CMD_CREATE = "create";
constexpr std::string_view CMD_DB_LIST = "db-list";
constexpr std::string_view CMD_DB_DELETE = "db-delete";

enum class DatabaseError
{
    SUCCESS, NETWORK_ERROR, TIMEOUT, DATABASE_NOT_FOUND, INVALID_RESPONSE,
    KEY_NOT_FOUND, INVALID_OBJECT, DATABASE_ALREADY_EXISTS, INVALID_COMMAND
};

}
