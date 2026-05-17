#pragma once

#include <chrono>
#include <future>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <buxtehude/buxtehude.hpp>

#include <nlohmann/json.hpp>

#include <tb/tb.h>

#include "json.hpp"
#include "validate.hpp"

namespace dflat
{

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
        requires std::same_as<
            T,
            std::remove_cvref_t<tb::pair_range_value_t<EntriesRange>>
        >
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

}
