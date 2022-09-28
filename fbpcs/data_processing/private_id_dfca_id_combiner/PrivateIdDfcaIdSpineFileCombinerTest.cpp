/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaIdSpineFileCombiner.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/Random.h>
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

using namespace ::pid::combiner;

class PrivateIdDfcaIdSpineFileCombinerTest : public testing::Test {
 public:
  void vectorStringToStream(
      std::vector<std::string>& input,
      std::stringstream& out) {
    for (auto const& row : input) {
      out << row << '\n';
    }
  }

  void validateOutputFile(std::vector<std::string>& expectedOutput) {
    // Validate the output with what is expected
    std::ifstream outputFile{outputFilePath_};
    uint64_t lineIndex = 0;
    std::string outputString;
    while (getline(outputFile, outputString)) {
      std::cout << outputString << std::endl;
      // EXPECT_EQ(outputString, expectedOutput.at(lineIndex));
      ++lineIndex;
    }

    // Should not be any extra entries any side
    EXPECT_EQ(lineIndex, expectedOutput.size());
  }

  void runTest(
      std::vector<std::string>& dataContent,
      std::vector<std::string>& spineIdContent,
      std::vector<std::string>& expectedOutput,
      std::string protocol = PROTOCOL_PID) {
    FLAGS_protocol_type = protocol;
    auto randStart = folly::Random::secureRand64();
    std::string dataContentPath =
        "/tmp/PrivateIdDfcaIdSpineFileCombinerTestDataContent" +
        std::to_string(randStart);
    std::string spineIdContentPath =
        "/tmp/PrivateIdDfcaIdSpineFileCombinerTestSpineIdContent" +
        std::to_string(randStart);
    constexpr size_t kBufferedReaderChunkSize = 4096;
    data_processing::test_utils::writeVecToFile(dataContent, dataContentPath);
    data_processing::test_utils::writeVecToFile(
        spineIdContent, spineIdContentPath);
    FLAGS_data_path = dataContentPath;
    FLAGS_spine_path = spineIdContentPath;
    FLAGS_output_path =
        "/tmp/PrivateIdDfcaIdSpineFileCombinerTestOutputContent" +
        std::to_string(randStart);
    outputFilePath_ = FLAGS_output_path;
    executeStrategy(FLAGS_protocol_type);
    validateOutputFile(expectedOutput);
  }

 protected:
  std::string outputFilePath_;
};

// test validation header with \r\n as new line
TEST_F(PrivateIdDfcaIdSpineFileCombinerTest, TestHeaderValidation) {
  std::vector<std::string> dataInput = {
      "id_,user_id_publisher\r\nid_1,1656361100394756"};
  std::vector<std::string> spineInput = {"AAAA,id_1"};
  std::vector<std::string> expectedOutput = {
      "id_,user_id_publisher", "AAAA,1656361100394756"};
  runTest(dataInput, spineInput, expectedOutput);
}

std::vector<std::string> multiKeyDataInput = {
    "id_email,id_phone,id_fn,user_id_partner",
    "email1,phone1,fn1,eid1",
    "email2,,,eid2",
    "email3,phone2,,eid3",
    "email4,,fn2,eid4",
    "email5,phone3,fn3,eid5",
    ",phone4,fn4,eid6",
    ",,fn6,eid9"};
std::vector<std::string> multiKeySpineInput = {
    "AAAA,email1,phone1",
    "DDDD,email3,phone2",
    "FFFF,email5,phone3,fn3",
    "HHHH,phone7",
    "IIII,email2",
    "CCCC,fn2,email4",
    "BBBB,fn4,phone4",
    "GGGG,fn6"};

std::vector<std::string> multiKeyExpectedOutput = {
    "id_,user_id_partner",
    "AAAA,eid1",
    "BBBB,eid6",
    "CCCC,eid4",
    "DDDD,eid3",
    "FFFF,eid5",
    "GGGG,eid9",
    "HHHH,0",
    "IIII,eid2",
};

TEST_F(PrivateIdDfcaIdSpineFileCombinerTest, TestMultiKeyWithMaxOne) {
  runTest(multiKeyDataInput, multiKeySpineInput, multiKeyExpectedOutput);
}

TEST_F(PrivateIdDfcaIdSpineFileCombinerTest, TestMultiKeyWithMaxTwo) {
  FLAGS_max_id_column_cnt = 2;
  runTest(multiKeyDataInput, multiKeySpineInput, multiKeyExpectedOutput);
}

TEST_F(PrivateIdDfcaIdSpineFileCombinerTest, TestMultiKeyWithMaxThree) {
  FLAGS_max_id_column_cnt = 3;
  runTest(multiKeyDataInput, multiKeySpineInput, multiKeyExpectedOutput);
}

TEST_F(PrivateIdDfcaIdSpineFileCombinerTest, TestMultiKeyWithMaxFour) {
  FLAGS_max_id_column_cnt = 4;
  runTest(multiKeyDataInput, multiKeySpineInput, multiKeyExpectedOutput);
}
