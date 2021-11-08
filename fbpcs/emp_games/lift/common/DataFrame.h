/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "fbpcs/emp_games/lift/common/Column.h"

/*
 * This DataFrame implementation is loosely based on an answer from
 * https://stackoverflow.com/a/32651111/15625637
 */
namespace df {
class ParseException : public std::exception {
public:
  explicit ParseException(const std::string &s, const std::string &typeName) {
    msg_ = "Failed to parse '" + s + "' as type '" + typeName + "'";
  }

  const char *what() const noexcept override { return msg_.c_str(); }

private:
  std::string msg_;
};

class BaseMap {
public:
  virtual ~BaseMap() {}
};

// actual map of Columns
template <typename T>
class MapT : public BaseMap,
             public std::unordered_map<std::string, Column<T>> {};

class BadTypeException : public std::exception {
public:
  explicit BadTypeException(std::string expected, std::string actual) {
    msg_ = "Expected type '" + expected + "', but got type '" + actual + "'";
  }

  const char *what() const noexcept override { return msg_.c_str(); }

private:
  std::string msg_;
};

struct TypeMap {
  std::vector<std::string> boolColumns;
  std::vector<std::string> intColumns;
  std::vector<std::string> intVecColumns;
};

class DataFrame {
public:
  using TypeInfo = std::pair<std::type_index, std::string>;

  static DataFrame readCsv(const TypeMap &typeMap, const std::string &filePath);

  static DataFrame
  loadFromRows(const TypeMap &typeMap, const std::vector<std::string> &header,
               const std::vector<std::vector<std::string>> &rows);

  static void checkType(const TypeInfo &expected, const TypeInfo &actual) {
    if (expected.first != actual.first) {
      throw BadTypeException{expected.second, actual.second};
    }
  }

  std::unordered_set<std::string> keys() const {
    std::unordered_set<std::string> res;
    for (const auto &[typ, _] : types_) {
      res.insert(typ);
    }
    return res;
  }

  template <typename T> std::unordered_set<std::string> keysOf() const {
    std::unordered_set<std::string> res;
    auto target = std::type_index(typeid(T));
    for (const auto &[typ, info] : types_) {
      if (info.first == target) {
        res.insert(typ);
      }
    }
    return res;
  }

  template <typename T> const Column<T> &get(const std::string &key) const {
    auto idx = std::type_index(typeid(T));
    // If this column is defined, ensure the type is correct
    if (types_.find(key) != types_.end()) {
      auto typeName = typeid(T).name();
      checkType(types_.at(key), std::make_pair(idx, typeName));
    }

    auto &ptr = maps_.at(idx);
    return dynamic_cast<MapT<T> &>(*ptr)[key];
  }

  template <typename T> Column<T> &get(const std::string &key) {
    auto idx = std::type_index(typeid(T));
    auto typeName = typeid(T).name();

    // First check if we've added any columns of this type
    if (maps_.find(idx) == maps_.end()) {
      maps_.emplace(idx, std::make_unique<MapT<T>>());
    }

    // Then check if we've seen this key before
    if (types_.find(key) == types_.end()) {
      types_.emplace(key, std::make_pair(idx, typeName));
    }

    return const_cast<Column<T> &>(
        const_cast<const DataFrame &>(*this).get<T>(key));
  }

  template <typename T> const Column<T> &at(const std::string &key) const {
    auto idx = std::type_index(typeid(T));
    auto typeName = typeid(T).name();
    // Ensure the type is correct
    // NOTE: This will throw std::out_of_range if `key` is not present
    checkType(types_.at(key), std::make_pair(idx, typeName));
    return (*this).get<T>(key);
  }

  template <typename T> Column<T> &at(const std::string &key) {
    return const_cast<Column<T> &>(
        const_cast<const DataFrame &>(*this).at<T>(key));
  }

  template <typename T> void drop(const std::string &key) {
    auto idx = std::type_index(typeid(T));
    auto &ptr = maps_.at(idx);

    // First erase from column map
    dynamic_cast<MapT<T> &>(*ptr).erase(key);

    // Then erase from types_
    auto typeIt = types_.find(key);
    types_.erase(typeIt);
  }

private:
  std::unordered_map<std::string, TypeInfo> types_;
  std::unordered_map<std::type_index, std::unique_ptr<BaseMap>> maps_;
};

namespace detail {
template <typename T> T parse(const std::string &value) {
  std::istringstream iss{value};
  T res;
  iss >> res;
  if (iss.fail()) {
    // For bools, check if the string was given as `true/false`
    if constexpr (std::is_same<T, bool>::value) {
      iss.clear();
      iss >> std::boolalpha >> res;
      if (iss.good()) {
        // Secondary parsing succeeded
        return res;
      }
    }
    auto typeName = typeid(T).name();
    throw ParseException{value, typeName};
  }

  return res;
}

template <typename T> std::vector<T> parseVector(const std::string &value) {
  if (value.at(0) != '[' || value.at(value.size() - 1) != ']') {
    auto typeName = std::string{"std::vector<"} + typeid(T).name() + ">";
    throw ParseException{value, typeName};
  }

  std::vector<T> res;

  // get substr between [ and ]
  std::stringstream ss{value.substr(1, value.size() - 2)};
  while(ss.good()) {
    std::string part;
    std::getline(ss, part, ',');
	if (!part.empty()){
	  res.push_back(parse<T>(part));
	}
  }

  return res;
}
} // namespace detail
} // namespace df
