/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fbpcf/io/LocalFileManager.h"
#include "fbpcs/data_processing/common/BufferedReader.h"

namespace fbpcs {
TEST(BufferedReaderTest, testReadLineWithLocalReader) {
  std::string runPath = __FILE__;
  std::string basePath = runPath.substr(0, runPath.rfind("/") + 1);
  auto filePath = "buffered_reader_example_file.txt";
  auto fullFilePath = basePath + filePath;

  auto fileManager = std::make_unique<fbpcf::LocalFileManager>();
  auto reader = BufferedReader(std::move(fileManager), fullFilePath);
  auto firstLine = reader.readLine();
  auto secondLine = reader.readLine();

  EXPECT_EQ(firstLine, "this is a test file");
  EXPECT_EQ(secondLine, "this is the second line");
  EXPECT_FALSE(reader.eof());
  auto thirdLine = reader.readLine(); // end of file
  EXPECT_EQ(thirdLine, "");
  reader.readLine();
  EXPECT_TRUE(reader.eof());
}

} // namespace fbpcs
