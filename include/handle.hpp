#pragma once

#include <chrono>
#include <future>
#include <mutex>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <buxtehude/buxtehude.hpp>

#include <nlohmann/json.hpp>

#include <tb/tb.h>

#include "json.hpp"
#include "request.hpp"
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
           std::chrono::seconds timeout = 5s);

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
    -> tb::error<DatabaseError>;

    auto ListDatabases() -> tb::result<std::vector<std::string>, DatabaseError>;
    auto DeleteDatabase(std::string_view database_name) -> tb::error<DatabaseError>;

    std::string server_name;
    std::chrono::seconds timeout;
private:
    auto CommandImpl(std::string_view database_name, std::string_view command,
                     const json& args, const bux::ValidationSeries& validation)
    -> tb::result<json, DatabaseError>;

    bux::Client& client;
    std::mutex queries_mutex;
    std::mutex write_mutex;
    std::unordered_map<unsigned, PendingResponse> pending_queries;
    unsigned request_id = 0;
};

}
