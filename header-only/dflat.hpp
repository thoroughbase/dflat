#pragma once

#include <buxtehude/buxtehude.hpp>

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstddef>
#include <future>
#include <mutex>
#include <string_view>
#include <type_traits>
#include <vector>

#include <tb/tb.h>

namespace dflat
{

namespace bux = buxtehude;
using nlohmann::json, nlohmann::json_pointer;

template<typename T>
concept Serialisable = requires (json& j) {
    { j.get<T>() };
    { j.get<std::vector<T>>() };
};

static_assert(Serialisable<std::string_view>);

template<tb::pair_range Range>
    requires tb::string_view_like<tb::pair_range_key_t<Range>>
    && dflat::Serialisable<tb::pair_range_value_t<Range>>
auto as_json_dict(Range&& range) -> json
{
    json j = dflat::json::object();
    for (const auto& [key, value] : range)
        j.emplace(key, value);
    return j;
}

}

template<std::ranges::range Range>
    requires dflat::Serialisable<std::ranges::range_value_t<Range>>
void to_json(dflat::json& j, Range&& list)
{
    j = dflat::json::array();
    for (const auto& elem : list)
        j.emplace_back(elem);
}

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

namespace validate {

inline const auto IsValidCommand = bux::predicates::Matches({
    CMD_GET, CMD_PUT, CMD_CREATE, CMD_DB_LIST, CMD_DB_DELETE
});

constexpr auto IsStringArray = [] (const json& j) {
    if (!j.is_array()) return false;
    for (auto& element : j)
        if (!element.is_string()) return false;
    return true;
};

constexpr auto IsDict = [] (const json& j) {
    return j.is_object();
};

inline const bux::ValidationSeries COMMAND = {
    { "/cmd"_json_pointer, IsValidCommand },
    { "/database"_json_pointer, bux::predicates::NotEmpty },
    { "/request-id"_json_pointer, bux::predicates::IsNumber }
};

inline const bux::ValidationSeries RESPONSE = {
    { "/request-id"_json_pointer, bux::predicates::IsNumber },
    { "/error-code"_json_pointer, bux::predicates::IsNumber }
};

inline const bux::ValidationSeries GET_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_GET) },
    { "/keys"_json_pointer, IsStringArray }
};

inline const bux::ValidationSeries GET_RESPONSE = {
    { "/entries"_json_pointer, IsDict }
};

inline const bux::ValidationSeries PUT_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_PUT) },
    { "/entries"_json_pointer, IsDict },
    { "/replace"_json_pointer, bux::predicates::IsBool }
};

inline const bux::ValidationSeries PUT_RESPONSE = {};

inline const bux::ValidationSeries CREATE_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_CREATE) },
    { "/persist"_json_pointer, bux::predicates::IsBool },
    { "/max-cache-items"_json_pointer, bux::predicates::IsNumber }
};

inline const bux::ValidationSeries CREATE_RESPONSE = {};

inline const bux::ValidationSeries DB_LIST_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_DB_LIST) }
};

inline const bux::ValidationSeries DB_LIST_RESPONSE = {
    { "/databases"_json_pointer, IsStringArray }
};

inline const bux::ValidationSeries DB_DELETE_QUERY = {
    { "/cmd"_json_pointer, bux::predicates::Compare(CMD_DB_DELETE) },
};

inline const bux::ValidationSeries DB_DELETE_RESPONSE = {};

}

struct PendingResponse
{
    std::promise<bux::Message> promise;
};

using namespace std::chrono_literals;

class Handle
{
public:
    Handle(bux::Client& client, std::string_view server_name = "dflat",
           std::chrono::seconds timeout = 5s)
    : server_name(server_name), timeout(timeout), client(client)
    {
        client.AddHandler(DFLAT_RESPONSE, [this] (auto&, const bux::Message& msg) {
            if (!bux::ValidateJSON(msg.content, validate::RESPONSE))
                return;

            unsigned id = msg.content["request-id"];
            std::scoped_lock queries_lock { queries_mutex };
            if (pending_queries.contains(id))
                pending_queries[id].promise.set_value(msg);
        });
    }

    template<Serialisable T>
    auto Get(std::string_view database_name, std::string_view key)
    -> tb::result<T, DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_GET,
            json::object({
                { "keys", json::array({ key }) }
            }), validate::GET_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();
        json& content = content_result.get_mut_unchecked();

        if (!content["entries"].contains(key) || content["entries"][key].is_null())
            return DatabaseError::KEY_NOT_FOUND;

        try {
            return content["entries"][key].get<T>();
        } catch (const json::type_error& e) {
            return DatabaseError::INVALID_OBJECT;
        }
    }

    template<Serialisable T, std::ranges::range KeyRange>
        requires tb::string_view_like<std::ranges::range_value_t<KeyRange>>
    auto GetMany(std::string_view database_name, KeyRange&& keys)
    -> tb::result<std::unordered_map<std::string, T>, DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_GET,
            json::object({ { "keys", keys } }),
            validate::GET_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();
        json& content = content_result.get_mut_unchecked();

        try {
            return content["entries"].get<std::unordered_map<std::string, T>>();
        } catch (const json::type_error& e) {
            return DatabaseError::INVALID_OBJECT;
        }
    }

    auto Put(std::string_view database_name, std::string_view key,
             Serialisable auto const& value,
             bool replace = false)
    -> tb::error<DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_PUT, {
            { "entries", json::object({ { key, value } }) },
            { "replace", replace }
        }, validate::PUT_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();

        return tb::ok;
    }

    template<Serialisable T, tb::pair_range EntriesRange>
        requires std::same_as<T, tb::pair_range_value_t<EntriesRange>>
    auto PutMany(std::string_view database_name, EntriesRange&& entries,
                 bool replace = true)
    -> tb::error<DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_PUT, {
            { "entries", as_json_dict(std::forward<EntriesRange>(entries)) },
            { "replace", replace }
        }, validate::PUT_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();

        return tb::ok;
    }

    auto Create(std::string_view database_name, bool persist, size_t max_cache)
    -> tb::error<DatabaseError>
    {
        auto content_result = CommandImpl(database_name, "create",
            json {
                { "persist", persist },
                { "max-cache-items", max_cache }
            }, validate::CREATE_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();

        return tb::ok;
    }

    auto ListDatabases() -> tb::result<std::vector<std::string>, DatabaseError>
    {
        auto content_result = CommandImpl("dummy", CMD_DB_LIST, {},
            validate::DB_LIST_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();

        return content_result.get_unchecked()["databases"].get<std::vector<std::string>>();
    }

    auto DeleteDatabase(std::string_view database_name)
    -> tb::error<DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_DB_DELETE, {},
            validate::DB_DELETE_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();

        return tb::ok;
    }

    std::string server_name;
    std::chrono::seconds timeout;
private:
    auto CommandImpl(std::string_view database_name, std::string_view command,
                     const json& args, const bux::ValidationSeries& validation)
    -> tb::result<json, DatabaseError>
    {
        unsigned id;
        std::future<bux::Message> response;

        {
            std::scoped_lock queries_lock { queries_mutex };
            id = request_id;
            pending_queries.emplace(request_id++, PendingResponse {});
            response = pending_queries[id].promise.get_future();
        }

        json msg_content = {
            { "cmd", command },
            { "database", database_name },
            { "request-id", id }
        };

        if (args.is_object())
            msg_content.update(args);

        auto write_message = [&] () -> tb::error<bux::WriteError> {
            std::scoped_lock write_lock { write_mutex };
            return client.Write({
                .dest = server_name,
                .type { DFLAT_QUERY },
                .content = std::move(msg_content),
                .only_first = true
            });
        };

        tb::scoped_guard erase_query = [&] {
            std::scoped_lock queries_lock { queries_mutex };
            pending_queries.erase(id);
        };

        if (write_message().is_error())
            return DatabaseError::NETWORK_ERROR;

        if (response.wait_for(timeout) == std::future_status::timeout)
            return DatabaseError::TIMEOUT;

        json content = std::move(response.get().content);
        if (!bux::ValidateJSON(content, validate::RESPONSE))
            return DatabaseError::INVALID_RESPONSE;

        if (content["error-code"] != DatabaseError::SUCCESS)
            return content["error-code"].get<DatabaseError>();

        if (!bux::ValidateJSON(content, validation))
            return DatabaseError::INVALID_RESPONSE;

        return content;
    }

    bux::Client& client;
    std::mutex queries_mutex;
    std::mutex write_mutex;
    std::unordered_map<unsigned, PendingResponse> pending_queries;
    unsigned request_id = 0;
};

class Database
{
public:
    Database(bux::Client& cl, std::string_view directory)
    : storage_directory(directory), client(cl)
    {
        client.AddHandler(DFLAT_QUERY, [&] (auto&, const bux::Message& msg) {
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
        });
    }

private:
    std::string storage_directory;
    json databases;
    bux::Client& client;
};

}
