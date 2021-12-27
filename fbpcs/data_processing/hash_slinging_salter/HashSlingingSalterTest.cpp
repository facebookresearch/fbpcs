/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "HashSlingingSalter.hpp"
#include <gtest/gtest.h>
#include <string>

TEST(HashSalterTest, HashSalterSameAsPythonTest) {
  /*
  Testing if our c++ implementation of HMAC SHA 256 hashing returns the same
  hash as the python implementation we use in our data pipeline.

  b64Salt and b64SaltedHashFromPy were generated
  from the following python code:

  import base64
  import os
  import hashlib
  import hmac

  piiKey = "super_secret_email@example.com"
  salt = os.urandom(32)
  b64Salt = base64.b64encode(salt).decode()
  b64SaltedHashFromPy = base64.b64encode(
      hmac.new(salt, msg=pii_key.encode("utf-8"),
  digestmod=hashlib.sha256).digest()
  ).decode()
  */

  auto piiKey = "super_secret_email@example.com";
  auto b64Salt = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0=";
  auto b64SaltedHashFromPy = "xz/QtZYtVrksTpkZUCkCf4OGzZJ99iN4EMDJIJ1g+KY=";
  auto b64SaltedHashFromCpp =
      private_lift::hash_slinging_salter::base64SaltedHashFromBase64Key(
          piiKey, b64Salt);
  EXPECT_EQ(b64SaltedHashFromCpp, b64SaltedHashFromPy);
};
