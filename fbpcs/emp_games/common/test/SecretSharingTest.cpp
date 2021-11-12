/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <vector>

#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "fbpcs/emp_games/common/SecretSharing.h"

namespace private_measurement {

using namespace secret_sharing;

template <typename TIn, typename TOut>
std::vector<TOut> revealVector(std::vector<TIn>& in) {
  return map<TIn, TOut>(
      in, [](auto empVal) { return empVal.template reveal<TOut>(); });
}

template <typename TIn, typename TOut>
std::vector<std::vector<TOut>> revealVectorOfVectors(
    std::vector<std::vector<TIn>>& in) {
  return map<std::vector<TIn>, std::vector<TOut>>(
      in, [](auto empVec) { return revealVector<TIn, TOut>(empVec); });
}

TEST(SecretSharingTest, TestPrivatelyShareBool) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        bool expected = true;
        emp::Bit b = privatelyShare(fbpcf::Party::Alice, expected);
        auto actual = b.reveal<bool>();
        EXPECT_EQ(expected, actual);
      });
}

TEST(SecretSharingTest, TestPrivatelyShareInt) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        int64_t expected = 12345;
        emp::Integer i = privatelyShare(fbpcf::Party::Alice, expected);
        auto actual = i.reveal<int64_t>();
        EXPECT_EQ(expected, actual);
      });
}

TEST(SecretSharingTest, TestPrivatelyShareBoolVector) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        std::vector<bool> expected{true, false, false, true};
        std::vector<emp::Bit> bVec = privatelyShare(fbpcf::Party::Alice, expected, expected.size());
        auto actual = revealVector<emp::Bit, bool>(bVec);
        EXPECT_EQ(expected, actual);
      });
}

TEST(SecretSharingTest, TestPrivatelyShareIntVector) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        std::vector<int64_t> expected{12, 34, 56, 78};
        std::vector<emp::Integer> iVec  = privatelyShare(fbpcf::Party::Alice, expected, expected.size());
        auto actual = revealVector<emp::Integer, int64_t>(iVec);
        EXPECT_EQ(expected, actual);
      });
}

TEST(SecretSharingTest, TestPrivatelyShareIntsFromAlice) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        std::vector<int64_t> aliceInput{10, 11, 12, 13, 14, 15};
        auto numVals = aliceInput.size();
        std::vector<emp::Integer> output;

        if (party == fbpcf::Party::Alice) {
          output = privatelyShareIntsFromAlice<emp::ALICE>(aliceInput, numVals);
        } else {
          output = privatelyShareIntsFromAlice<emp::BOB>(
              // Bob passes in a dummy array
              std::vector<int64_t>(),
              numVals);
        }

        auto revealedInts = revealVector<emp::Integer, int64_t>(output);

        EXPECT_EQ(aliceInput, revealedInts);
      });
}

TEST(SecretSharingTest, TestPrivatelyShareArraysFromBob) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        std::vector<std::vector<bool>> bobInput{
            {true, true, false},
            {false, false, true},
            {true, false, false, true}};
        auto numVals = bobInput.size();
        auto maxArraySize = 4;
        auto paddingValue = false;
        std::vector<std::vector<emp::Bit>> output;

        if (party == fbpcf::Party::Alice) {
          output = privatelyShareArraysFromBob<emp::ALICE, bool, emp::Bit>(
              // Alice passes in a dummy array
              std::vector<std::vector<bool>>(),
              numVals,
              // Alice may not know the max array size
              0,
              paddingValue);
        } else {
          output = privatelyShareArraysFromBob<emp::BOB, bool, emp::Bit>(
              bobInput, numVals, maxArraySize, paddingValue);
        }

        auto revealedBoolVecs = revealVectorOfVectors<emp::Bit, bool>(output);

        // Pad the arrays so they all have length 4
        bobInput.at(0).push_back(paddingValue);
        bobInput.at(1).push_back(paddingValue);

        EXPECT_EQ(bobInput, revealedBoolVecs);
      });
}

TEST(SecretSharingTest, TestPrivatelyShareIntArraysNoPaddingFromBob) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        std::vector<std::vector<int64_t>> bobInput{
            {10, 11, 12}, {20, 21, 22}, {30, 31, 32}};
        auto numVals = bobInput.size();
        auto arraySize = bobInput.at(0).size();
        auto bitLen = 64;
        std::vector<std::vector<emp::Integer>> output;

        if (party == fbpcf::Party::Alice) {
          output = privatelyShareIntArraysNoPaddingFromBob<emp::ALICE>(
              // Alice passes in a dummy array
              std::vector<std::vector<int64_t>>(),
              numVals,
              arraySize,
              bitLen);
        } else {
          output = privatelyShareIntArraysNoPaddingFromBob<emp::BOB>(
              bobInput, numVals, arraySize, bitLen);
        }

        auto revealedIntVecs =
            revealVectorOfVectors<emp::Integer, int64_t>(output);

        EXPECT_EQ(bobInput, revealedIntVecs);
      });
}

} // namespace private_measurement
