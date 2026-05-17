#pragma once

#include <nlohmann/json.hpp>

#include <tb/tb.h>

namespace dflat
{

using nlohmann::json, nlohmann::json_pointer;

template<typename T>
concept Serialisable = requires (json& j) {
    { j.get<T>() };
};

static_assert(Serialisable<std::string_view>);

template<tb::pair_range Range>
    requires tb::string_view_like<tb::pair_range_key_t<Range>>
    && Serialisable<tb::pair_range_value_t<Range>>
auto as_json_dict(Range&& range) -> json
{
    json j = json::object();
    for (const auto& [key, value] : range)
        j.emplace(key, value);
    return j;
}

} // namespace dflat

template<std::ranges::range Range>
    requires dflat::Serialisable<std::ranges::range_value_t<Range>>
void to_json(dflat::json& j, Range&& list)
{
    j = dflat::json::array();
    for (const auto& elem : list)
        j.emplace_back(elem);
}
