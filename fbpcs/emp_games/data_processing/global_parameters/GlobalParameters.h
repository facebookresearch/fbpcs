/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <stdint.h>
#include <fstream>

#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/variant.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>

namespace global_parameters {

/**
 * This header provides a comprehensive way to pass in-binary global parameters
 * around across multiple stages/containers. The list of global parameters are
 * maintained as a map from their name to a boost::variant containing their
 * values. It's user's responsibility to ensure the right type is used when
 * retrieving values stored in boost::variant.
 * This header also provides serialization APIs to convert the map from global
 * parameter names to their values into string.
 * To add a new type of value, inserting that type in to the boost::variant
 * statement below should be sufficient.
 */

inline const std::string KAdvRowCount = "Advertiser_Row_Count";
inline const std::string KPubRowCount = "Publisher_Row_Count";

inline const std::string KAdvDataWidth = "Advertiser_Data_Width";
inline const std::string KPubDataWidth = "Publisher_Data_Width";

inline const std::string KMatchedUserCount = "Matched_User_Count";

/**
 * This variant decides what are the supported types of each global parameter.
 */
using GlobalParameterType = boost::variant<
    int32_t /* basic type, can be used to represent total number of cohorts*/,
    std::unordered_map<int32_t, int32_t>
    /* representing the mapping between e.g. orignial ads ads and the corresponding aggregation ids*/ >;

/**
 * Representing the map from parameters name to their value
 */
using GlobalParameters = std::unordered_map<std::string, GlobalParameterType>;

std::string serialize(const GlobalParameters& src);

GlobalParameters deserialize(const std::string& src);

void writeToFile(const std::string& file, const GlobalParameters& gp);

GlobalParameters readFromFile(const std::string& file);

} // namespace global_parameters
