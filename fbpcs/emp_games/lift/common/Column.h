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
#include <vector>

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

  template <typename F> Column<T> map(F f) const {
    Column<T> res;
    res.reserve(size());
    for (std::size_t i = 0; i < size(); ++i) {
      res.push_back(f(at(i)));
    }
    return res;
  }

  template <typename F> void mapInPlace(F f) {
    for (std::size_t i = 0; i < size(); ++i) {
      at(i) = f(at(i));
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

  /* Comparison operators */
  friend bool operator==(const Column<T> &a, const Column<T> &b) {
    return a.v_ == b.v_;
  }

  friend bool operator!=(const Column<T> &a, const Column<T> &b) {
    return a.v_ != b.v_;
  }

private:
  std::vector<T> v_;
};

} // namespace df
