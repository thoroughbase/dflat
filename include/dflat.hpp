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

namespace dflat
{

namespace bux = buxtehude;
namespace tb = bux::tb;
using nlohmann::json, nlohmann::json_pointer;

template<typename T>
concept Serialisable = requires (json& j) {
    { j.get<T>() };
    { j.get<std::vector<T>>() };
};

static_assert(Serialisable<std::string_view>);

namespace detail
{

template<typename T, size_t I>
struct element_type
{
    using type = std::remove_cvref_t<decltype(std::get<I>(std::declval<T>()))>;
};

template<typename T, size_t I>
using element_type_t = element_type<T, I>::type;

}

template<typename T, typename K, typename V>
concept PairIteratorRange = std::ranges::input_range<T>
&& std::same_as<detail::element_type_t<std::ranges::range_reference_t<T>, 0>, K>
&& std::same_as<detail::element_type_t<std::ranges::range_reference_t<T>, 1>, V>;

namespace detail
{

template<typename T> requires Serialisable<T>
json from_pairs(PairIteratorRange<std::string, T> auto const& pairs)
{
    json j = json::object();
    for (const auto& [key, value] : pairs)
        j.emplace(key, value);

    return j;
}

template<typename T> requires Serialisable<T>
json from_span(std::span<const T> items)
{
    json j = json::array();
    for (const auto& elem : items)
        j.emplace_back(elem);

    return j;
}

}

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
           std::chrono::seconds timeout = 5s);

    template<typename T> requires Serialisable<T>
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

    template<typename T> requires Serialisable<T>
    auto GetMany(std::string_view database_name, std::span<std::string_view> keys)
    -> tb::result<std::unordered_map<std::string, T>, DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_GET,
            json::object({ { "keys", detail::from_span<std::string_view>(keys) } }),
            validate::GET_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();
        json& content = content_result.get_mut_unchecked();

        try {
            return content["entries"].get<std::unordered_map<std::string, T>>();
        } catch (const json::type_error& e) {
            return DatabaseError::INVALID_OBJECT;
        }
    }

    template<typename T> requires Serialisable<T>
    auto Put(std::string_view database_name, std::string_view key, const T& value,
             bool replace = false)
    -> tb::error<DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_PUT, {
            { "entries", json::object({ key, value }) },
            { "replace", replace }
        }, validate::PUT_RESPONSE);

        if (content_result.is_error()) return content_result.get_error();

        return tb::ok;
    }

    template<typename T> requires Serialisable<T>
    auto PutMany(std::string_view database_name,
                 PairIteratorRange<std::string, T> auto const& entries,
                 bool replace = true)
    -> tb::error<DatabaseError>
    {
        auto content_result = CommandImpl(database_name, CMD_PUT, {
            { "entries", detail::from_pairs<T>(entries) },
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
    std::unordered_map<unsigned, PendingResponse> pending_queries;
    unsigned request_id = 0;
};

class Database
{
public:
    Database(bux::Client& client, std::string_view directory);

private:
    std::string storage_directory;
    json databases;
    bux::Client& client;
};

}
