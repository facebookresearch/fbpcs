/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "../SecretSharing.h"

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

TEST(SecretSharingTest, TestMultiplyBitmask) {
  fbpcf::mpc::wrapTestWithParty<std::function<void(fbpcf::Party party)>>(
      [](fbpcf::Party party) {
        auto bitLen = 64;
        // Test 1: vector of ints
        std::vector<int64_t> expected{123, 0, 789};
        std::vector<emp::Integer> input{emp::Integer{bitLen, 123},
                                        emp::Integer{bitLen, 456},
                                        emp::Integer{bitLen, 789}};
        std::vector<emp::Bit> bitmask{emp::Bit{true}, emp::Bit{false},
                                      emp::Bit{true}};

        auto actual = multiplyBitmask(input, bitmask);
        auto revealed = revealVector<emp::Integer, int64_t>(actual);

        EXPECT_EQ(expected, revealed);

        // Test 2: vector of bits
        std::vector<bool> expected2{false, true, false};
        std::vector<emp::Bit> input2{emp::Bit{true}, emp::Bit{true},
                                     emp::Bit{true}};
        std::vector<emp::Bit> bitmask2{emp::Bit{false}, emp::Bit{true},
                                       emp::Bit{false}};

        auto actual2 = multiplyBitmask(input2, bitmask2);
        auto revealed2 = revealVector<emp::Bit, bool>(actual2);

        EXPECT_EQ(expected2, revealed2);

        // Test 3: vector of vector of ints
        std::vector<std::vector<int64_t>> expected3{{1, 2}, {3, 4}, {0, 0}};
        std::vector<std::vector<emp::Integer>> input3{
            {emp::Integer{bitLen, 1}, emp::Integer{bitLen, 2}},
            {emp::Integer{bitLen, 3}, emp::Integer{bitLen, 4}},
            {emp::Integer{bitLen, 5}, emp::Integer{bitLen, 6}}};
        std::vector<emp::Bit> bitmask3{emp::Bit{true}, emp::Bit{true},
                                       emp::Bit{false}};

        auto actual3 = multiplyBitmask(input3, bitmask3);
        auto revealed3 = revealVectorOfVectors<emp::Integer, int64_t>(actual3);

        EXPECT_EQ(expected3, revealed3);
      });
}

} // namespace private_measurement
