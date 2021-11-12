/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "fbpcs/emp_games/lift/common/Column.h"

/*
 * This DataFrame implementation is loosely based on an answer from
 * https://stackoverflow.com/a/32651111/15625637
 */
namespace df {
/**
 * This class is a convenience wrapper for an error in parsing a string to a
 * specific subType. It allows the caller to gracefully catch an exception
 * from parsing user-supplied data instead of getting an opaque issue related to
 * a stream failure.
 */
class ParseException : public std::exception {
 public:
  /**
   * Construct a new ParseException that represents a failure to parse `s` as
   * the specified type.
   *
   * @param s the string being parsed
   * @param typeName a human-readable name for the type s was being parsed as
   */
  explicit ParseException(const std::string& s, const std::string& typeName) {
    msg_ = "Failed to parse '" + s + "' as type '" + typeName + "'";
  }

  const char* what() const noexcept override {
    return msg_.c_str();
  }

 private:
  std::string msg_;
};

/**
 * A class which should not be instantiated directly, but is useful in the
 * development of our dynamically typed DataFrame.
 */
class BaseMap {
 public:
  virtual ~BaseMap() {}
};

// actual map of Columns
/**
 * A class which represents an unordered_map of strings to type T. It extends
 * our virtual BaseMap above which will let us do the dynamic typing later in
 * DataFrame, which is why we have this (seemingly) useless definition.
 *
 * @tparam T the type of data stored in this MapT
 */
template <typename T>
class MapT : public BaseMap,
             public std::unordered_map<std::string, Column<T>> {};

/**
 * This class is a convenience wrapper for an error in accessing our DataFrame
 * with the wrong type specifier. For example, code might throw this exception
 * when calling `df.get<int>("stringCol")` since int != string.
 */
class BadTypeException : public std::exception {
 public:
  /**
   * Construct a BadTypeException.
   *
   * @param expected a human-readable name for the type that was expected
   * @param actual a human-readable name for the type that was given
   */
  explicit BadTypeException(std::string expected, std::string actual) {
    msg_ = "Expected type '" + expected + "', but got type '" + actual + "'";
  }

  const char* what() const noexcept override {
    return msg_.c_str();
  }

 private:
  std::string msg_;
};

/**
 * A struct for holding lists of expected types when parsing a CSV. Since C++
 * is a statically typed language, we must know *before* reading a CSV what
 * type we expect each column to have if we want to make efficent use of our
 * DataFrame and avoid a bunch of dynamic_casts. By marking ahead of time which
 * columns will have which types, we have a single dynamic_cast to get into
 * the Column, but accesses to individual elements will natively be of type T.
 */
struct TypeMap {
  std::vector<std::string> boolColumns;
  std::vector<std::string> intColumns;
  std::vector<std::string> intVecColumns;
};

/**
 * A generic DataFrame usable much like one would expect in Python's `pandas`!
 * A DataFrame can be constructed by hand or loaded from a CSV/rows for a more
 * abstract interface. Columns can be modified or accessed via the `get`/`at`
 * methods. There is currently no way to apply functions across rows (it is
 * expected that this library will be used in a columnar fashion), but that
 * feature may be added later.
 */
class DataFrame {
 public:
  using TypeInfo = std::pair<std::type_index, std::string>;

  /**
   * DataFrame iterator which is contextual depending on the specific RowType
   * being iterated. The major pro of this setup is that one can iterate over
   * a DataFrame with a DataFrame::RowIterator<MyUseCase> and work with a typed
   * struct instead of raw pointers and more mental overhead.
   *
   * @tparam RowType the type of each row this RowIterator is iterating over
   */
  template <typename RowType>
  class RowIterator {
   public:
    // Tags required for custom-defined iterators
    using iterator_category = std::forward_iterator_tag;
    using value_type = RowType;
    using pointer = value_type*;
    using reference = const value_type&;

    /**
     * Construct a new RowIterator referencing the the DataFrame at `pos`. This
     * is like referencing df[pos] and getting a typed object which is able to
     * refer to its individual fields.
     *
     * @param df the DataFrame being iterated over
     * @param pos the row index within the DataFrame
     */
    RowIterator(DataFrame& df, std::size_t pos) : df_{df}, pos_{pos} {
      tryLoadRow();
    }

    /**
     * Construct a new RowIterator referencing the start of the DataFrame. This
     * is like referencing df[pos] and getting a typed object which is able to
     * refer to its individual fields.
     *
     * @param df the DataFrame being iterated over
     */
    explicit RowIterator(DataFrame& df) : RowIterator{df, 0} {}

    /**
     * Try to load the row at this iterator's position. If the attempt to load
     * this row throws `std::out_of_range`, it will be caught and this iterator
     * will be set as invalid.
     */
    void tryLoadRow() {
      try {
        row_ = df_.rowAt<RowType>(pos_);
        valid_ = true;
      } catch (const std::out_of_range& /* unused */) {
        // If we got std::out_of_range, somewhere a necessary column is missing
        // and we just set this RowIterator as invalid.
        valid_ = false;
      }
    }

    /**
     * Check whether this RowIterator is valid.
     *
     * @returns true if this RowIterator is holding a valid RowType
     */
    bool isValid() const {
      return valid_;
    }

    /**
     * Dereference this iterator to view the underlying RowType.
     *
     * @returns a reference to the RowType at the current iterator position
     */
    reference operator*() const {
      return row_;
    }

    /**
     * Look inside this iterator to get a pointer to the underlying RowType.
     *
     * @returns a pointer to the RowType at the current iterator position
     */
    pointer operator->() {
      return &row_;
    }

    /**
     * Prefix increment. Advanced this iterator forward by one.
     *
     * @returns a reference to this iterator after advancing its position by one
     */
    RowIterator<RowType>& operator++() {
      ++pos_;
      tryLoadRow();
      return *this;
    }

    /**
     * Postfix increment. Advanced this iterator forward by one but return a
     * reference to the iterator before it was incremented.
     *
     * @returns a reference to a copy of this iterator before it was incremented
     */
    RowIterator<RowType>& operator++(int /* unused */) {
      RowIterator<RowType> tmp = *this;
      ++(*this);
      return tmp;
    }

    /**
     * Compare two RowIterators.
     *
     * @returns true if the two RowIterators are equivalent
     */
    friend bool operator==(
        const RowIterator<RowType>& a,
        const RowIterator<RowType>& b) {
      return &a.df_ == &b.df_ && a.pos_ == b.pos_;
    }

    /**
     * Compare two RowIterators for inequality.
     *
     * @returns true if the two RowIterators are *not* equivalent
     */
    friend bool operator!=(
        const RowIterator<RowType>& a,
        const RowIterator<RowType>& b) {
      return !(a == b);
    }

   private:
    DataFrame& df_;
    std::size_t pos_;
    RowType row_;
    bool valid_;
  };

  /**
   * Read a CSV into a new DataFrame. Takes a typeMap to parse strings to typed
   * values during reading. Columns encountered in the CSV which are not listed
   * in the typeMap, will be parsed as `std::string`. The caller may choose
   * later to convert these to a new type via functions like `Column::map`.
   *
   * @param typeMap expected typing for each column that will be read from the
   *     CSV. If a type is given in this map, all values in the Column *must*
   *     parse to that type or a `ParseException` will be thrown.
   * @param filePath a path to the CSV to be loaded
   * @returns a DataFrame object loaded from the filePath with the given types
   * @throws `ParseException` if a type is given but a value in the Column
   *     cannot parse to that type
   * @note for columns not in `typeMap`, `std::string` will be assumed
   */
  static DataFrame readCsv(const TypeMap& typeMap, const std::string& filePath);

  /**
   * Read a CSV into a new DataFrame. Takes a typeMap to parse strings to typed
   * values during reading. Columns encountered in the CSV which are not listed
   * in the typeMap, will be parsed as `std::string`. The caller may choose
   * later to convert these to a new type via functions like `Column::map`.
   *
   * @param typeMap expected typing for each column that will be read from the
   *     CSV. If a type is given in this map, all values in the Column *must*
   *     parse to that type or a `ParseException` will be thrown.
   * @param header the list of column names being loaded
   * @param rows a list of rows where each row is the size of the header; it is
   *     assumed that `header[i]` corresponds to `rows[_][i]`
   * @returns a DataFrame object loaded from the filePath with the given types
   * @throws `ParseException` if a type is given but a value in the Column
   *     cannot parse to that type
   * @note for columns not in `typeMap`, `std::string` will be assumed
   */
  static DataFrame loadFromRows(
      const TypeMap& typeMap,
      const std::vector<std::string>& header,
      const std::vector<std::vector<std::string>>& rows);

  /**
   * Check that two types are equivalent. This is used to avoid a potentially
   * bad `dynamic_cast` in `get`/`at` calls.
   *
   * @param expected the type being expected
   * @param actual the actual type supplied
   * @throws `BadTypeException` if `expected` is not equal to `actual`
   */
  static void checkType(const TypeInfo& expected, const TypeInfo& actual) {
    if (expected.first != actual.first) {
      throw BadTypeException{expected.second, actual.second};
    }
  }

  /**
   * Get the keys contained within this DataFrame.
   *
   * @returns the set of keys stored in this DataFrame
   */
  std::unordered_set<std::string> keys() const {
    std::unordered_set<std::string> res;
    for (const auto& [typ, _] : types_) {
      res.insert(typ);
    }
    return res;
  }

  /**
   * Get the keys of type `T` contained within this DataFrame. For example, if
   * this DataFrame contains two int Columns intsCol1 and intsCol2 and two
   * `std::string` Columns stringsCol1 and stringsCol2, then a call to
   * `keysOf<int>()` would return the set `{"intsCol1", "intsCol2"}.`
   *
   * @tparam T a filter for which keys to ask for
   * @returns the set of keys stored in this DataFrame that have type T
   */
  template <typename T>
  std::unordered_set<std::string> keysOf() const {
    std::unordered_set<std::string> res;
    auto target = std::type_index(typeid(T));
    for (const auto& [typ, info] : types_) {
      if (info.first == target) {
        res.insert(typ);
      }
    }
    return res;
  }

  /**
   * Check if a given key is defined in this DataFrame already
   *
   * @returns true if `key` is stored in this DataFrame
   */
  bool containsKey(const std::string& key) const {
    return types_.find(key) != types_.end();
  }

  /**
   * Get a `Column<T>` at the given key within this DataFrame. A `dynamic_cast`
   * is necessary since we're dynamically altering types at runtime depending
   * on the values being read or set. While there is a small computational cost
   * to this cast, since we can guarantee that all elements of the resulting
   * Column are of type `T`, it is only incurred upon this initial `get<T>`.
   *
   * @tparam T the type of Column<T> stored at the given key
   * @param key the Column key to look up in this DataFrame
   * @returns the `Column<T>` stored at the given key
   * @throws `BadTypeException` if the key is already in the DataFrame but
   *     stored as a type other than `T`
   * @throws `std::out_of_range` if `key` does not exist within this DataFrame
   */
  template <typename T>
  const Column<T>& get(const std::string& key) const {
    auto idx = std::type_index(typeid(T));
    // If this column is defined, ensure the type is correct
    if (types_.find(key) != types_.end()) {
      auto typeName = typeid(T).name();
      checkType(types_.at(key), std::make_pair(idx, typeName));
    }

    auto& ptr = maps_.at(idx);
    return dynamic_cast<MapT<T>&>(*ptr)[key];
  }

  /**
   * The non-const version of `DataFrame::get<T>` which may insert a new Column
   * into this DataFrame if the key doesn't already exist. If the key does
   * exist, we return a reference to its Column in this DataFrame.
   *
   * @tparam T the type of Column<T> stored at the given key (or the type of the
   *     newly created column if `key` doesn't already exist in this DataFrame)
   * @param key the Column key to look up in this DataFrame
   * @returns the `Column<T>` stored at the given key (could be newly created)
   * @throws `BadTypeException` if the key is already in the DataFrame but
   *     stored as a type other than `T`
   */
  template <typename T>
  Column<T>& get(const std::string& key) {
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

    return const_cast<Column<T>&>(
        const_cast<const DataFrame&>(*this).get<T>(key));
  }

  /**
   * Get a `Column<T>` at the given key within this DataFrame.
   *
   * @tparam T the type of Column<T> stored at the given key
   * @param key the Column key to look up in this DataFrame
   * @returns the `Column<T>` stored at the given key
   * @throws `BadTypeException` if the key is already in the DataFrame but
   *     stored as a type other than `T`
   * @throws `std::out_of_range` if `key` does not exist within this DataFrame
   */
  template <typename T>
  const Column<T>& at(const std::string& key) const {
    auto idx = std::type_index(typeid(T));
    auto typeName = typeid(T).name();
    // Ensure the type is correct
    // NOTE: This will throw std::out_of_range if `key` is not present
    checkType(types_.at(key), std::make_pair(idx, typeName));
    return (*this).get<T>(key);
  }

  /**
   * The non-const version of `DataFrame::at<T>`. This method will *not* insert
   * a new Column into this DataFrame if an entry does not exist at the key.
   *
   * @tparam T the type of Column<T> stored at the given key
   * @param key the Column key to look up in this DataFrame
   * @returns the `Column<T>` stored at the given key
   * @throws `BadTypeException` if the key is already in the DataFrame but
   *     stored as a type other than `T`
   * @throws `std::out_of_range` if `key` does not exist within this DataFrame
   */
  template <typename T>
  Column<T>& at(const std::string& key) {
    return const_cast<Column<T>&>(
        const_cast<const DataFrame&>(*this).at<T>(key));
  }

  /**
   * Remove a column from this DataFrame by erasing the reference from it in
   * the internal map. This is useful if you no longer need access to a given
   * Column and wish to aggressively free memory related to it.
   *
   * @tparam T the type of Column<T> stored at the given key
   *
   * @param key the key to remove from this DataFrame
   */
  template <typename T>
  void drop(const std::string& key) {
    auto idx = std::type_index(typeid(T));
    auto& ptr = maps_.at(idx);

    // First erase from column map
    dynamic_cast<MapT<T>&>(*ptr).erase(key);

    // Then erase from types_
    auto typeIt = types_.find(key);
    types_.erase(typeIt);
  }

  /**
   * Get a row view into this DataFrame at the given index.
   *
   * @tparam RowType a type implementing `fromDataFrame(DataFrame, std::size_t)`
   *     which represents a view into a specific row of a DataFrame
   *
   * @param idx the index of the row the new RowType should point to
   * @returns a RowType view of this DataFrame at the given index
   */
  template <typename RowType>
  RowType rowAt(std::size_t idx) {
    return RowType::fromDataFrame(*this, idx);
  }

 private:
  std::unordered_map<std::string, TypeInfo> types_;
  std::unordered_map<std::type_index, std::unique_ptr<BaseMap>> maps_;
};

namespace detail {
/**
 * Parse a `std::string` into the given type using a `std::istringstream`.
 *
 * @tparam T the type to parse `value` into
 * @param value the string to be parsed into a new T
 * @returns `value` parsed as a `T`
 * @throws `ParseException` if the value cannot be parsed into a `T`
 */
template <typename T>
T parse(const std::string& value) {
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

/**
 * Parse a `std::string` into a vector the given type by splitting across commas
 * and calling `parse<T>` on each relevant substring.
 *
 * @tparam T the type to parse `value` into
 * @param value the string to be parsed into a new T
 * @returns `value` parsed as a `T`
 * @throws `ParseException` if the value cannot be parsed into a `T`
 * @note requires that `value` begin with `[` and end with `]`
 */
template <typename T>
std::vector<T> parseVector(const std::string& value) {
  if (value.at(0) != '[' || value.at(value.size() - 1) != ']') {
    auto typeName = std::string{"std::vector<"} + typeid(T).name() + ">";
    throw ParseException{value, typeName};
  }

  std::vector<T> res;

  // get substr between [ and ]
  std::stringstream ss{value.substr(1, value.size() - 2)};
  while (ss.good()) {
    std::string part;
    std::getline(ss, part, ',');
    if (!part.empty()) {
      res.push_back(parse<T>(part));
    }
  }

  return res;
}
} // namespace detail
} // namespace df
