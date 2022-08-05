/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <variant>
#include <vector>

#include <folly/logging/xlog.h>

#include <fbpcf/exception/exceptions.h>
#include <fbpcf/frontend/mpcGame.h>
#include <fbpcs/emp_games/common/Constants.h>
#include <fbpcs/emp_games/common/Util.h>

namespace shard_combiner {

// Enum used to define the type of AggMetrics object,
// We need this information to parse folly::dynamic objects to
// AggMetrics object.
enum class AggMetricType {
  kValue, // used to define a metric type that holds the value defined by the
          // type common::InputEncryption.

  kList, // used to define a container that holds a list of AggMetrics object

  kDict // used to define a container that holds a Dictionary of AggMetrics type
};

const size_t kMetricBitWidth = 64;

template <int schedulerId, bool usingBatch = true>
using SecInt = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecSignedInt<kMetricBitWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecBit =
    typename fbpcf::frontend::MpcGame<schedulerId>::template SecBit<usingBatch>;

template <int schedulerId, bool usingBatch = true>
using BitVariant = std::variant<bool, SecBit<schedulerId, usingBatch>>;

template <
    int schedulerId = 0,
    bool usingBatch = false,
    common::InputEncryption inputEncryption =
        common::InputEncryption::Plaintext>
class AggMetrics {
 public:
  using MetricsValue = int64_t;
  using MetricsList = std::vector<
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>>;
  using MetricsDict = std::map<
      std::string,
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>>;
  using MetricsVariant = std::variant<MetricsValue, MetricsList, MetricsDict>;
  // add more types if necessary, like arithmetic secret share, constructor will
  // assign as needed.
  using SecMetricVariant = std::variant<SecInt<schedulerId, usingBatch>>;

  explicit AggMetrics(MetricsValue val)
      : type_{AggMetricType::kValue}, val_{val} {}

  explicit AggMetrics(MetricsList& valList)
      : type_{AggMetricType::kList}, val_(std::move(valList)) {}
  explicit AggMetrics(MetricsDict& valDict)
      : type_{AggMetricType::kDict}, val_(std::move(valDict)) {}

  explicit AggMetrics(AggMetricType type) : type_{type} {
    if (type_ == AggMetricType::kDict) {
      val_ = MetricsDict{};
    } else if (type_ == AggMetricType::kList) {
      val_ = MetricsList{};
    } else if (type_ == AggMetricType::kValue) {
      val_ = MetricsValue{0};
    } else {
      std::string errMsg = folly::sformat(
          "Construction not supported for: [{}], should be one of kValue(0), kList(1), or kDict(2)",
          static_cast<std::underlying_type<AggMetricType>::type>(type));
      XLOG(ERR) << errMsg;
      throw common::exceptions::ConstructionError(errMsg);
    }
  }

  ~AggMetrics() {}

  // Adds rhs to self. this is the main function that performs the job of
  // combining. Also, see `accumulateFinal()` for the complete understanding.
  static void accumulate(
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>&
          lhs,
      const std::shared_ptr<
          AggMetrics<schedulerId, usingBatch, inputEncryption>>& rhs);

  // checks if *this >= rhs metrics and returns a variant which could be
  // bool or SecBit<> based on the instantiation.
  BitVariant<schedulerId, usingBatch> isGreaterOrEqual(const AggMetrics& rhs);

  // if the condition is evaluates to be logic '1' then retains current
  // value, else chooses newVal.
  void mux(
      const BitVariant<schedulerId, usingBatch>& condition,
      std::shared_ptr<AggMetrics>& newVal);

  AggMetricType getType() const {
    return type_;
  }

  MetricsValue getValue() const;
  SecInt<schedulerId, usingBatch> getSecValueXor() const {
    return std::get<SecInt<schedulerId, usingBatch>>(secVal_);
  }
  const MetricsList& getAsList() const;
  const MetricsDict& getAsDict() const;

  void setValue(MetricsValue v);

  // setter for XorSecretShareValue
  void setSecValueXor(SecInt<schedulerId, usingBatch>& v) {
    secVal_ = std::move(v);
  }

  // reads val_ that contains the intShare and inits secretValue holding data
  // structure SecMetricVariant.
  void updateSecValueFromRawInt();

  // reads val_ that contains the publicValue (k-anon threshold, sentinel
  // values, etc.,) and inits secretValue holding data structure
  // SecMetricVariant.
  void updateSecValueFromPublicInt();

  // Traverses through all children and calls updateSecValueFromRawInt.
  void updateAllSecVals();

  // Value is moved to val_.
  void setList(MetricsList& v);

  void pushBack(
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>&
          val);

  // Value is moved to val_.
  void setDict(MetricsDict& v);

  // inserts a new/replace Metric at key
  void insert(
      std::pair<
          std::string,
          std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>>
          kv);

  // append metric to a list at key.
  // creates a new entry if not exists.
  void appendAtKey(
      std::string key,
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
          value);

  // if dict looks up the key and returns the Metric shared pointer.
  std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
      getAtKey(std::string) const;

  // if list returns AggMetric shared pointer.
  std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
  getAtIndex(size_t i) const;

  // creates a Metrics blob with 0 initialized values following the schema
  // of rhs.
  static std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
  newLike(const std::shared_ptr<
          AggMetrics<schedulerId, usingBatch, inputEncryption>>& rhs);

  // Parses the Json into AggMetrics object.
  static std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
  fromJson(std::string filePath);

  // Emits dynamic object which can be converted to json
  folly::dynamic toDynamic() const;

  // Emits dynamic object which can be converted to json.
  // Note: use this method reveal final output metric.
  folly::dynamic toRevealedDynamic(int party) const;

  // writes object with indentation to the ostream obj.
  void print(std::ostream& os, int32_t tabstop) const;

  friend std::ostream& operator<<(
      std::ostream& os,
      const AggMetrics<schedulerId, usingBatch, inputEncryption>& metrics) {
    metrics.print(os, 0);
    return os;
  }

  friend std::ostream& operator<<(
      std::ostream& os,
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
          metrics) {
    metrics->print(os, 0);
    return os;
  }

 private:
  // This is the actual accumulate operation that gets called on the leaf node.
  // Reason for writing this function separately is that, newer backends
  // can be easily configured. (like Arithemetic-SS for instance).
  static void accumulateFinal(
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>&
          lhs,
      const std::shared_ptr<
          AggMetrics<schedulerId, usingBatch, inputEncryption>>& rhs);

  // helper for print
  void printSpaces(std::ostream& os, int32_t n) const;

  // Every metric should bear a type, can be either of List, Dict, Value.
  AggMetricType type_;
  // Holds the final value or another metric blob.
  MetricsVariant val_;
  // this is a std::variant because we'd like to support many frontend/backend
  // interface like XOR-SS, Arithmetic, etc.
  SecMetricVariant secVal_;
};

// AggMetrics shared pointer.
template <
    int schedulerId = 0,
    bool usingBatch = false,
    common::InputEncryption inputEncryption =
        common::InputEncryption::Plaintext>
using AggMetrics_sp =
    std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>;

} // namespace shard_combiner
