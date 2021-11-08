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
// Useful to denote if if a class Test is a specialization of a Reference class
// Example: is_specialization<std::vector<int64_t>, std::vector>::value -> true
template <typename Test, template <typename...> class Ref>
struct is_specialization : std::false_type {};

template <template <typename...> class Ref, typename... Args>
struct is_specialization<Ref<Args...>, Ref> : std::true_type {};
} // anonymous namespace

namespace df {

/**
 * This is an extension of a std::vector to be used for typical data processing.
 * It provides methods for a functional paradigm (map, apply, reduce). It's also
 * made for tight integration with std::vector so that getting the additional
 * features of df::Column should be an easy switch.
 *
 * @tparam T the type of data stored in this Column
 */
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

  /**
   * Retrieve a value at a specific index in the column.
   *
   * @param pos the index into this Column
   * @returns the value at index `pos`
   * @throws `std::out_of_range` if `pos` is larger than `this->size()`
   */
  const T &at(std::size_t pos) const { return v_.at(pos); }

  T &at(std::size_t pos) {
    return const_cast<T &>(const_cast<const Column &>(*this).at(pos));
  }

  /**
   * Reserve capacity in the Column.
   *
   * @param capacity the amount of capacity to reserve in the underlying std::vector
   */
  void reserve(std::size_t capacity) { v_.reserve(capacity); }

  /**
   * Checks whether this Column is empty.
   *
   * @returns true if this Column has no values
   */
  bool empty() const { return v_.empty(); }

  /**
   * Get the number of items stored in this Column.
   *
   * @returns the number of elements in this Column
   */
  std::size_t size() const { return v_.size(); }

  /**
   * Add a new value to this Column.
   * @param value The value to add to this Column
   */
  void push_back(const T &value) { v_.push_back(value); }

  void push_back(T &&value) { v_.push_back(value); }

  /**
   * Constructs an element in-place at the back of this Column.
   *
   * @tparam Args argtypes to pass into the T constructor
   * @param args arguments to pass to the T constructor
   * @returns a reference to the newly constructed element
   */
  template <class... Args> T &emplace_back(Args &&...args) {
    return v_.emplace_back(args...);
  }

  /**
   * Apply a function on each element of this Column.
   *
   * @tparam F an unspecified function type which can be called with each
   *    element from this Column
   * @param f the function to call on each element of this Column
   */
  template <typename F> void apply(F f) {
    for (std::size_t i = 0; i < size(); ++i) {
      f(at(i));
    }
  }

  /**
   * Map this column into a new Column by applying a function to each element.
   *
   * @tparam F an unspecified function type which can be called with each
   *     element from this Column
   * @param f the function to call on each element of this column
   * @returns a new Column where each element is the result of calling f on the
   *     the respective element from this Column
   */
  template <typename F> auto map(F f) const -> Column<decltype(f(at(0)))> {
    Column<decltype(f(at(0)))> res;
    res.reserve(size());
    for (std::size_t i = 0; i < size(); ++i) {
      res.push_back(f(at(i)));
    }
    return res;
  }

  /**
   * Map this column into a new Column by applying a function to each element
   * along with another column at the same time.
   *
   * @tparam T2 the value type of the other Column
   * @param other the Column which will be mapped with this column
   * @tparam F an unspecified function type which can be called with each
   *     element from this Column
   * @param f the function to call on each element of this column
   * @returns a new Column where each element is the result of calling f on the
   *     the respective element from this Column
   */
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

  /**
   * Map this column into a new Column by applying a function to each element
   * along with another scalar value at the same time.
   *
   * @tparam T2 the type of the scalar
   * @tparam F an unspecified function type which can be called with each
   *     element from this Column
   * @param other the scalar value which will be mapped with this column
   * @param f the function to call on each element of this column
   * @returns a new Column where each element is the result of calling f on the
   *     the respective element from this Column
   */
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

  /**
   * Modify this column IN PLACE by applying a function to each element.
   *
   * @tparam F an unspecified function type which can be called with each
   *     element from this Column
   * @param f the function to call on each element of this column
   */
  template <typename F> void mapInPlace(F f) {
    for (std::size_t i = 0; i < size(); ++i) {
      at(i) = f(at(i));
    }
  }

  /**
   * Map this column IN PLACE by applying a function to each element along
   * with another column at the same time.
   *
   * @tparam T2 the value type of the other Column
   * @param other the Column which will be mapped with this column
   * @tparam F an unspecified function type which can be called with each
   *     element from this Column
   * @param f the function to call on each element of this column
   */
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

  /**
   * Map this column IN PLACE by applying a function to each element along
   * with another scalar value at the same time.
   *
   * @tparam T2 the type of the scalar
   * @tparam F an unspecified function type which can be called with each
   *     element from this Column
   * @param other the scalar value which will be mapped with this column
   * @param f the function to call on each element of this column
   */
  template <typename T2, typename F>
  void mapWithScalarInPlace(const T2 &other, F f) {
    for (std::size_t i = 0; i < size(); ++i) {
      at(i) = f(at(i), other);
    }
  }

  /**
   * Perform a left fold of this Column by applying the binary function `f`
   * over each element, starting with the accumulator `acc`.
   *
   * @tparam F an unspecified function type which can be called with each
   *     element from this Column
   * @param f the binary function to apply for reduction
   * @param acc the initial value for the reduction
   * @returns the result of a left fold of this Column over `f`
   */
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

  /**
   * Constructs a new Column from this Column by calling the constructor of `T2`
   * which takes an element of `T`. This is a shorthand method for `map` when
   * the caller just wishes to convert from one type to another and the target
   * type defines a constructor of the form `T2(T)`.
   *
   * @tparam T2 the value type of the other Column
   * @returns a new column mapped to the type T2
   */
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

  template <typename T2> void operator-=(T2 &other) {
    if constexpr (is_specialization<T2, Column>::value) {
      mapWithInPlace(other, [](const T &a, const T &b) { return a - b; });
    } else {
      mapWithScalarInPlace(other, [](const T &a, const T &b) { return a - b; });
    }
  }

  template <typename T2> void operator*=(T2 &other) {
    if constexpr (is_specialization<T2, Column>::value) {
      mapWithInPlace(other, [](const T &a, const T &b) { return a * b; });
    } else {
      mapWithScalarInPlace(other, [](const T &a, const T &b) { return a * b; });
    }
  }

  template <typename T2> void operator/=(T2 &other) {
    if constexpr (is_specialization<T2, Column>::value) {
      mapWithInPlace(other, [](const T &a, const T &b) { return a / b; });
    } else {
      mapWithScalarInPlace(other, [](const T &a, const T &b) { return a / b; });
    }
  }

private:
  std::vector<T> v_;
};

} // namespace df

#include "fbpcs/emp_games/lift/common/Column-impl.h"
