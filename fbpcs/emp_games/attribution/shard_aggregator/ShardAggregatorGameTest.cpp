/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/attribution/shard_aggregator/ShardAggregatorGame.h"

#include <memory>
#include <thread>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>
#include <folly/Random.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>
#include <folly/test/JsonTestUtil.h>
#include <gtest/gtest.h>

#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/mpc/EmpGame.h>
#include <fbpcf/mpc/EmpTestUtil.h>
#include <fbpcf/mpc/QueueIO.h>
#include "../../common/TestUtil.h"
#include "ShardAggregatorGame.h"
#include "fbpcs/emp_games/attribution/shard_aggregator/AggMetricsThresholdCheckers.h"

namespace measurement::private_attribution {
using AggMetrics = private_measurement::AggMetrics;
using AggMetricsTag = private_measurement::AggMetricsTag;

class ShardAggregatorGameTest : public ::testing::Test {
 protected:
  folly::dynamic outputMetricsObjFromPath(const std::string& path) {
    return folly::parseJson(
        fbpcf::io::FileIOWrappers::readFile(baseDir_ + path));
  }

  std::shared_ptr<AggMetrics> outputAggMetricsObjFromPath(
      const std::string& path) {
    return std::make_shared<AggMetrics>(
        AggMetrics::fromDynamic(folly::parseJson(
            fbpcf::io::FileIOWrappers::readFile(baseDir_ + path))));
  }

  // asserts that actual and expected structures, but not inner values, are
  // equal. Expects that where the expected structure has integers, the actual
  // structure has emp::Integers
  void assertSameStructure(
      const std::shared_ptr<AggMetrics>& actual,
      const std::shared_ptr<AggMetrics>& expected) {
    switch (expected->getTag()) {
      case AggMetricsTag::Map: {
        ASSERT_EQ(actual->getTag(), AggMetricsTag::Map);
        for (const auto& [key, value] : expected->getAsMap()) {
          ASSERT_TRUE(actual->getAsMap().find(key) != actual->getAsMap().end());
          assertSameStructure(actual->getAtKey(key), value);
        }
        break;
      }
      case AggMetricsTag::List: {
        ASSERT_EQ(actual->getTag(), AggMetricsTag::List);
        ASSERT_EQ(actual->getAsList().size(), expected->getAsList().size());
        for (std::size_t i = 0; i < expected->getAsList().size(); ++i) {
          assertSameStructure(actual->getAtIndex(i), expected->getAtIndex(i));
        }
        break;
      }
      case AggMetricsTag::Integer: {
        ASSERT_EQ(actual->getTag(), AggMetricsTag::EmpInteger);
        break;
      }
      default: {
        XLOG(FATAL) << "Invalid expected tag";
      }
    }
  }

  // returns aliceResult, bobResult pair
  template <class InputDataType>
  std::pair<std::shared_ptr<AggMetrics>, std::shared_ptr<AggMetrics>>
  runGameFunctionTest(
      std::function<std::shared_ptr<AggMetrics>(
          InputDataType input,
          ShardAggregatorGame<fbpcf::QueueIO>& game)> funcToTest,
      const InputDataType& aliceInput,
      const InputDataType& bobInput,
      std::function<void(std::shared_ptr<AggMetrics>)> thresholdChecker) {
    auto queueA = std::make_shared<folly::Synchronized<std::queue<char>>>();
    auto queueB = std::make_shared<folly::Synchronized<std::queue<char>>>();

    auto lambda = [&queueA, &queueB, &funcToTest, &thresholdChecker](
                      fbpcf::Party party, const InputDataType& input) {
      auto io = std::make_unique<fbpcf::QueueIO>(
          party == fbpcf::Party::Alice ? fbpcf::QueueIO{queueA, queueB}
                                       : fbpcf::QueueIO{queueB, queueA});
      ShardAggregatorGame<fbpcf::QueueIO> game{
          std::move(io), party, thresholdChecker};

      return funcToTest(input, game);
    };

    auto futureAlice = std::async(lambda, fbpcf::Party::Alice, aliceInput);
    auto futureBob = std::async(lambda, fbpcf::Party::Bob, bobInput);

    auto resAlice = futureAlice.get();
    auto resBob = futureBob.get();

    return std::pair<std::shared_ptr<AggMetrics>, std::shared_ptr<AggMetrics>>{
        resAlice, resBob};
  }

  void runReconstructTest(
      const std::string& aliceInputFile,
      const std::string& bobInputFile) {
    std::shared_ptr<AggMetrics> aliceInput =
        outputAggMetricsObjFromPath(aliceInputFile);
    std::shared_ptr<AggMetrics> bobInput =
        outputAggMetricsObjFromPath(bobInputFile);

    auto lambda = [](std::shared_ptr<AggMetrics> input,
                     const ShardAggregatorGame<fbpcf::QueueIO>& game) {
      return game.applyReconstruct(input);
    };

    auto result = runGameFunctionTest<std::shared_ptr<AggMetrics>>(
        lambda, aliceInput, bobInput, placeholderThresholdChecker_);

    assertSameStructure(result.first, aliceInput);
    assertSameStructure(result.second, bobInput);
  }

  // idxForStructureCheck is important for the lift format, where we want to
  // check against the input with the most cohorts
  void runPlayTest(
      const std::vector<std::string>& aliceInputFiles,
      const std::vector<std::string>& bobInputFiles,
      std::function<void(std::shared_ptr<AggMetrics>)> thresholdChecker,
      const std::size_t idxForStructureCheck = 0) {
    std::vector<std::shared_ptr<AggMetrics>> aliceInput;
    std::vector<std::shared_ptr<AggMetrics>> bobInput;
    for (std::size_t i = 0; i < aliceInputFiles.size(); ++i) {
      aliceInput.push_back(outputAggMetricsObjFromPath(aliceInputFiles.at(i)));
      bobInput.push_back(outputAggMetricsObjFromPath(bobInputFiles.at(i)));
    }

    auto lambda = [](std::vector<std::shared_ptr<AggMetrics>> input,
                     ShardAggregatorGame<fbpcf::QueueIO>& game) {
      return game.play(input);
    };

    auto result = runGameFunctionTest<std::vector<std::shared_ptr<AggMetrics>>>(
        lambda, aliceInput, bobInput, thresholdChecker);

    assertSameStructure(result.first, aliceInput.at(idxForStructureCheck));
    assertSameStructure(result.second, bobInput.at(idxForStructureCheck));
  }

  void SetUp() override {
    baseDir_ =
        private_measurement::test_util::getBaseDirFromPath(__FILE__) + "/test/";
    placeholderThresholdChecker_ =
        [](std::shared_ptr<AggMetrics> metrics /* unused */) {};
  }

  std::string baseDir_;
  std::function<void(std::shared_ptr<AggMetrics>)> placeholderThresholdChecker_;
};

// reconstruct function tests
TEST_F(ShardAggregatorGameTest, TestReconstructAdObject) {
  runReconstructTest(
      "ad_object_format/publisher_attribution_out.json_0",
      "ad_object_format/partner_attribution_out.json_0");
}

TEST_F(ShardAggregatorGameTest, TestReconstructLift) {
  runReconstructTest("lift/aggregator_alice_0", "lift/aggregator_bob_0");
}

TEST_F(ShardAggregatorGameTest, TestReconstructGeneric) {
  runReconstructTest(
      "test_new_parser/simple_map.json", "test_new_parser/simple_map.json");
}

// play function tests
TEST_F(ShardAggregatorGameTest, TestPlayAdObject) {
  const std::vector<std::string> aliceInput = {
      "ad_object_format/publisher_attribution_out.json_0",
      "ad_object_format/publisher_attribution_out.json_1",
  };
  const std::vector<std::string> bobInput = {
      "ad_object_format/partner_attribution_out.json_0",
      "ad_object_format/partner_attribution_out.json_1",
  };
  runPlayTest(
      aliceInput, bobInput, constructAdObjectFormatThresholdChecker(100));
}

TEST_F(ShardAggregatorGameTest, TestPlayLift) {
  const std::vector<std::string> aliceInput = {
      "lift/aggregator_alice_0",
      "lift/aggregator_alice_1",
  };
  const std::vector<std::string> bobInput = {
      "lift/aggregator_bob_0",
      "lift/aggregator_bob_1",
  };
  // need to use the file with the most cohorts for the same structure check,
  // since the aggregated result will contain all of the cohorts
  runPlayTest(aliceInput, bobInput, constructLiftThresholdChecker(100), 1);
}

TEST_F(ShardAggregatorGameTest, TestPlayGeneric) {
  const std::vector<std::string> aliceInput = {
      "test_new_parser/simple_map.json",
      "test_new_parser/simple_map.json",
  };
  const std::vector<std::string> bobInput = {
      "test_new_parser/simple_map.json",
      "test_new_parser/simple_map.json",
  };
  runPlayTest(aliceInput, bobInput, placeholderThresholdChecker_);
}

TEST_F(ShardAggregatorGameTest, TestPlayGenericList) {
  const std::vector<std::string> aliceInput = {
      "test_new_parser/list_metrics.json",
      "test_new_parser/list_metrics.json",
  };
  const std::vector<std::string> bobInput = {
      "test_new_parser/list_metrics.json",
      "test_new_parser/list_metrics.json",
  };
  runPlayTest(aliceInput, bobInput, placeholderThresholdChecker_);
}

TEST_F(ShardAggregatorGameTest, TestPlaySingleValue) {
  const std::vector<std::string> aliceInput = {
      "test_new_parser/single_value.json",
      "test_new_parser/single_value.json",
  };
  const std::vector<std::string> bobInput = {
      "test_new_parser/single_value.json",
      "test_new_parser/single_value.json",
  };
  runPlayTest(aliceInput, bobInput, placeholderThresholdChecker_);
}
} // namespace measurement::private_attribution
