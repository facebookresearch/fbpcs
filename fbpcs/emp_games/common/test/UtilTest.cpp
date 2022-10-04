/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <tuple>
#include <vector>

#include "../Util.h"

namespace private_measurement {

TEST(UtilTest, TestGetTlsInfoFromArguments) {
  auto tlsInfo = common::getTlsInfoFromArgs(
      false,
      "cert_path",
      "server_cert_path",
      "private_key_path",
      "passphrase_path");

  EXPECT_FALSE(tlsInfo.useTls);
  EXPECT_STREQ(tlsInfo.rootCaCertPath.c_str(), "");
  EXPECT_STREQ(tlsInfo.certPath.c_str(), "");
  EXPECT_STREQ(tlsInfo.keyPath.c_str(), "");
  EXPECT_STREQ(tlsInfo.passphrasePath.c_str(), "");

  const char* home_dir = std::getenv("HOME");
  if (home_dir == nullptr) {
    home_dir = "";
  }

  std::string home_dir_string(home_dir);

  tlsInfo = common::getTlsInfoFromArgs(
      true,
      "cert_path",
      "server_cert_path",
      "private_key_path",
      "passphrase_path");

  EXPECT_TRUE(tlsInfo.useTls);
  EXPECT_STREQ(
      tlsInfo.rootCaCertPath.c_str(), (home_dir_string + "/cert_path").c_str());
  EXPECT_STREQ(
      tlsInfo.certPath.c_str(),
      (home_dir_string + "/server_cert_path").c_str());
  EXPECT_STREQ(
      tlsInfo.keyPath.c_str(), (home_dir_string + "/private_key_path").c_str());
  EXPECT_STREQ(
      tlsInfo.passphrasePath.c_str(),
      (home_dir_string + "/passphrase_path").c_str());
}
} // namespace private_measurement
