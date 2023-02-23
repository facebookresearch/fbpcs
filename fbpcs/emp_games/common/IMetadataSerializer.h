/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

namespace common {

/*
** This abstract class will be implemented by each product to serialize data in
*order for UDP encryption to consume.
*/
class IMetadataSerializer {
 public:
  virtual ~IMetadataSerializer() = default;

  virtual std::vector<std::vector<unsigned char>>
  serializePublisherMetadata() = 0;

  virtual std::vector<std::vector<unsigned char>>
  serializePartnerMetadata() = 0;

 private:
};

} // namespace common
