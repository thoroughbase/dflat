#pragma once

#include <filesystem>
#include <fstream>

#include <buxtehude/buxtehude.hpp>

#include <nlohmann/json.hpp>

#include "request.hpp"
#include "validate.hpp"

namespace dflat
{

using nlohmann::json;

namespace bux = buxtehude;

struct FileError
{
    enum Type
    {
        CREATE_DIRECTORY, CREATE_FILE, PARSE_LIST_FILE
    };

    Type type;
    std::error_code code;
};

namespace file = std::filesystem;

class Database
{
    constexpr static std::string_view DATABASE_LIST_FILE = "databases.dflat";
public:
    static auto FromClient(bux::Client& cl, file::path directory = "dflat")
    -> tb::result<Database, FileError>
    {
        std::error_code err;
        file::create_directory(directory, err);
        if (err)
            return FileError { FileError::CREATE_DIRECTORY, err };

        std::fstream list_file {
            directory/DATABASE_LIST_FILE,
            std::ios::binary | std::ios::in
        };

        json file_contents;
        if (list_file.is_open()) {
            file_contents = json::parse(list_file, nullptr, false);
            if (file_contents.is_discarded())
                return FileError { FileError::PARSE_LIST_FILE };
        }

        return Database { cl, std::move(file_contents), directory };
    }

    Database(const Database&) = delete;
    auto operator=(const Database&) -> Database& = delete;

    Database(Database&& other)
    : storage_directory(other.storage_directory), databases(std::move(other.databases)),
      client(other.client)
    {
        other.moved_from = true;
        client.EraseHandler(std::string { DFLAT_QUERY });
        client.AddHandler(DFLAT_QUERY, [this] (auto&, const bux::Message& msg) {
            HandleMessage(msg);
        });
    }

    auto operator=(Database&&) -> Database& = delete;

    ~Database()
    {
        if (moved_from)
            return;

        std::fstream list_file {
            storage_directory/DATABASE_LIST_FILE,
            std::ios::binary | std::ios::out
        };

        // TODO: Error handling for failed disk writes
        list_file << databases;
    }

private:
    Database(bux::Client& cl, json&& database_data, file::path directory)
    : storage_directory(directory), databases(std::move(database_data)), client(cl)
    {
        client.AddHandler(DFLAT_QUERY, [this] (auto&, const bux::Message& msg) {
            HandleMessage(msg);
        });
    }

    void HandleMessage(const bux::Message& msg);

    file::path storage_directory;
    json databases = json::object();
    bux::Client& client;
    bool moved_from = false;
};

void Database::HandleMessage(const bux::Message& msg)
{
    if (!bux::ValidateJSON(msg.content, validate::COMMAND))
        return;

    auto db_name = msg.content["database"].get<std::string_view>();

    const auto make_msg_with = [&] (DatabaseError e, auto... args) {
        return bux::Message {
            .dest = msg.src,
            .type { DFLAT_RESPONSE },
            .content = {
                { "error-code", e },
                { "request-id", msg.content["request-id"] },
                args...
            }
        };
    };

    if (bux::ValidateJSON(msg.content, validate::GET_QUERY)) {
        if (!databases.contains(db_name)) {
            client.Write(
                make_msg_with(DatabaseError::DATABASE_NOT_FOUND)
            ).ignore_error();
            return;
        }

        json dict = json::object();
        json& eviction_queue = databases[db_name]["cache-eviction-queue"];
        json& entries = databases[db_name]["entries"];

        for (const json& key_json : msg.content["keys"]) {
            auto key = key_json.get<std::string_view>();
            // TODO: Currently only looks in cache
            if (entries.contains(key)) {
                dict.emplace(key, entries[key]);
                auto iter = std::find(eviction_queue.begin(),
                    eviction_queue.end(), key);
                if (iter != eviction_queue.end())
                    eviction_queue.erase(iter);
                eviction_queue.emplace_back(key);
            }
        }

        client.Write(
            make_msg_with(DatabaseError::SUCCESS, json { "entries", dict })
        ).ignore_error();
    } else if (bux::ValidateJSON(msg.content, validate::PUT_QUERY)) {
        if (!databases.contains(db_name)) {
            client.Write(
                make_msg_with(DatabaseError::DATABASE_NOT_FOUND)
            ).ignore_error();
            return;
        }

        bool replace = msg.content["replace"];
        json& eviction_queue = databases[db_name]["cache-eviction-queue"];
        json& entries = databases[db_name]["entries"];

        for (auto& [key, value] : msg.content["entries"].items()) {
            // TODO: Currently only looks in cache
            if (entries.contains(key) && replace)
                entries.erase(key);

            entries.emplace(key, value);
            auto iter = std::find(eviction_queue.begin(),
                eviction_queue.end(), key);
            if (iter != eviction_queue.end())
                eviction_queue.erase(iter);
            eviction_queue.emplace_back(key);
        }

        client.Write(make_msg_with(DatabaseError::SUCCESS)).ignore_error();

        size_t max_cache_items = databases[db_name]["max-cache-items"];

        if (eviction_queue.size() > max_cache_items) {
            auto start = eviction_queue.begin();
            auto end = start + eviction_queue.size() - max_cache_items;
            std::for_each(start, end, [&] (const json& j) {
                entries.erase(j.get<std::string_view>());
            });
            eviction_queue.erase(start, end - 1);
        }
    } else if (bux::ValidateJSON(msg.content, validate::CREATE_QUERY)) {
        if (databases.contains(db_name)) {
            client.Write(
                make_msg_with(DatabaseError::DATABASE_ALREADY_EXISTS)
            ).ignore_error();
            return;
        }

        databases.emplace(db_name, json {
            { "entries", json::object() },
            { "persist", msg.content["persist"] },
            { "cache-eviction-queue", json::array() },
            { "max-cache-items", msg.content["max-cache-items"] }
        });

        client.Write(make_msg_with(DatabaseError::SUCCESS)).ignore_error();
    } else if (bux::ValidateJSON(msg.content, validate::DB_LIST_QUERY)) {
        std::vector<std::string_view> names;
        names.reserve(databases.size());
        for (auto& [k, v] : databases.items())
            names.emplace_back(k);

        client.Write(
            make_msg_with(DatabaseError::SUCCESS, json { "databases", names })
        ).ignore_error();
    } else if (bux::ValidateJSON(msg.content, validate::DB_DELETE_QUERY)) {
        if (!databases.contains(db_name)) {
            client.Write(
                make_msg_with(DatabaseError::DATABASE_NOT_FOUND)
            ).ignore_error();
            return;
        }

        databases.erase(db_name);
        client.Write(make_msg_with(DatabaseError::SUCCESS)).ignore_error();
    }
}

}
