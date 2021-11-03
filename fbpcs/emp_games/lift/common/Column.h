/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <initializer_list>
#include <optional>
#include <sstream>
#include <vector>

namespace {
// Taken from https://stackoverflow.com/a/28796458/15625637
template <typename Test, template <typename...> class Ref>
struct is_specialization : std::false_type {};

template <template <typename...> class Ref, typename... Args>
struct is_specialization<Ref<Args...>, Ref> : std::true_type {};
} // anonymous namespace

namespace df {

template <typename T> class Column {
public:
  /* Basic constructors */
  Column() {}
  Column(const Column<T> &other) = default;
  Column(Column<T> &&other) = default;
  Column<T> &operator=(Column<T> &other) = default;
  Column<T> &operator=(Column<T> &&other) = default;

  explicit Column(std::size_t count, const T &value = T()) : v_(count, value) {}

  template <class InputIt>
  Column(InputIt first, InputIt last) : v_(first, last) {}

  /* implicit */ Column(std::initializer_list<T> init) : v_(init) {}

  Column<T> &operator=(std::initializer_list<T> init) {
    v_ = init;
    return *this;
  }

  /* Constructors from vector<T> */

  /* implicit */ Column(std::vector<T> &other) : v_(other) {}

  /* implicit */ Column(std::vector<T> &&other) : v_(other) {}

  Column &operator=(std::vector<T> &other) {
    v_ = other;
    return *this;
  }

  Column<T> &operator=(std::vector<T> &&other) {
    v_ = other;
    return *this;
  }

  /* Member functions */

  const T &at(std::size_t pos) const { return v_.at(pos); }

  T &at(std::size_t pos) {
    return const_cast<T &>(const_cast<const Column &>(*this).at(pos));
  }

  void reserve(std::size_t capacity) { v_.reserve(capacity); }

  bool empty() const { return v_.empty(); }

  std::size_t size() const { return v_.size(); }

  void push_back(const T &value) { v_.push_back(value); }

  void push_back(T &&value) { v_.push_back(value); }

  template <class... Args> T &emplace_back(Args &&...args) {
    return v_.emplace_back(args...);
  }

  template <typename F> void apply(F f) {
    for (std::size_t i = 0; i < size(); ++i) {
      f(at(i));
    }
  }

  template <typename F> auto map(F f) const -> Column<decltype(f(at(0)))> {
    Column<decltype(f(at(0)))> res;
    res.reserve(size());
    for (std::size_t i = 0; i < size(); ++i) {
      res.push_back(f(at(i)));
    }
    return res;
  }

  template <typename T2, typename F>
  auto mapWith(const Column<T2> &other, F f) const
      -> Column<decltype(f(at(0), other.at(0)))> {
    Column<decltype(f(at(0), other.at(0)))> res;
    if (size() != other.size()) {
      std::stringstream ss;
      ss << "This Column has size() = " << size()
         << ", but other Column has size() = " << other.size();
      throw std::invalid_argument{ss.str()};
    }

    res.reserve(size());
    for (std::size_t i = 0; i < size(); ++i) {
      res.push_back(f(at(i), other.at(i)));
    }
    return res;
  }

  template <typename T2, typename F>
  auto mapWithScalar(const T2 &other, F f) const
      -> Column<decltype(f(at(0), other))> {
    Column<decltype(f(at(0), other))> res;

    res.reserve(size());
    for (std::size_t i = 0; i < size(); ++i) {
      res.push_back(f(at(i), other));
    }
    return res;
  }

  template <typename F> void mapInPlace(F f) {
    for (std::size_t i = 0; i < size(); ++i) {
      at(i) = f(at(i));
    }
  }

  template <typename T2, typename F>
  void mapWithInPlace(const Column<T2> &other, F f) {
    if (size() != other.size()) {
      std::stringstream ss;
      ss << "This Column has size() = " << size()
         << ", but other Column has size() = " << other.size();
      throw std::invalid_argument{ss.str()};
    }

    for (std::size_t i = 0; i < size(); ++i) {
      at(i) = f(at(i), other.at(i));
    }
  }

  template <typename T2, typename F>
  void mapWithScalarInPlace(const T2 &other, F f) {
    for (std::size_t i = 0; i < size(); ++i) {
      at(i) = f(at(i), other);
    }
  }

  template <typename F>
  T reduce(F f, std::optional<T> acc = std::nullopt) const {
    std::size_t idx = 0;
    T res;
    if (acc == std::nullopt) {
      // Calling reduce on an empty Column with no acc is undefined
      ++idx;
      res = at(0);
    } else {
      res = acc.value();
    }

    for (/* empty */; idx < size(); ++idx) {
      res = f(res, at(idx));
    }
    return res;
  }

  template <typename T2> Column<T2> toColumn() const {
    Column<T2> res;
    res.reserve(size());
    for (std::size_t i = 0; i < size(); ++i) {
      res.push_back(T2(at(i)));
    }
    return res;
  }

  /* Comparison operators */
  friend bool operator==(const Column<T> &a, const Column<T> &b) {
    return a.v_ == b.v_;
  }

  friend bool operator!=(const Column<T> &a, const Column<T> &b) {
    return a.v_ != b.v_;
  }

  /* Binary assignment operators */
  template <typename T2> void operator+=(T2 &other) {
    if constexpr (is_specialization<T2, Column>::value) {
      mapWithInPlace(other, [](const T &a, const T &b) { return a + b; });
    } else {
      mapWithScalarInPlace(other, [](const T &a, const T &b) { return a + b; });
    }
  }

private:
  std::vector<T> v_;
};

} // namespace df

#include "fbpcs/emp_games/lift/common/Column-impl.h"
