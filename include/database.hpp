#pragma once

#include <filesystem>
#include <fstream>
#include <string_view>

#include <buxtehude/buxtehude.hpp>

#include <nlohmann/json.hpp>

#include <tb/tb.h>

namespace dflat
{

namespace bux = buxtehude;
namespace file = std::filesystem;

using nlohmann::json;

struct FileError
{
    enum Type
    {
        CREATE_DIRECTORY, CREATE_FILE, PARSE_LIST_FILE
    };

    Type type;
    std::error_code code;
};

class Database
{
    constexpr static std::string_view DATABASE_LIST_FILE = "databases.dflat";
public:
    static auto FromClient(bux::Client& cl, file::path directory = "dflat")
    -> tb::result<Database, FileError>;

    Database(const Database&) = delete;
    auto operator=(const Database&) -> Database& = delete;

    Database(Database&& other);
    auto operator=(Database&&) -> Database& = delete;

    ~Database();

private:
    Database(bux::Client& cl, json&& database_data, file::path directory);

    void HandleMessage(const bux::Message& msg);

    file::path storage_directory;
    json databases = json::object();
    bux::Client& client;
    bool moved_from = false;
};

}
