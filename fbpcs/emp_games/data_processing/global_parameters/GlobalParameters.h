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

using GlobalParameterType =
    boost::variant<int32_t, std::unordered_map<int32_t, int32_t>>;

using GlobalParameters = std::unordered_map<std::string, GlobalParameterType>;

std::string serialize(const GlobalParameters& src);

GlobalParameters deserialize(const std::string& src);

void writeToFile(const std::string& file, const GlobalParameters& gp);

GlobalParameters readFromFile(const std::string& file);

} // namespace global_parameters
