/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>

#include "fbpcs/emp_games/lift/common/Column.h"

/*
 * This DataFrame implementation is loosely based on an answer from
 * https://stackoverflow.com/a/32651111/15625637
 */
namespace df {
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

class DataFrame {
public:
  using TypeInfo = std::pair<std::type_index, std::string>;

  static void checkType(const TypeInfo &expected, const TypeInfo &actual) {
    if (expected.first != actual.first) {
      throw BadTypeException{expected.second, actual.second};
    }
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

} // namespace df
