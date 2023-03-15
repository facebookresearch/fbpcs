/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcf/io/api/FileIOWrappers.h"

#include "fbpcs/emp_games/data_processing/global_parameters/GlobalParameters.h"

namespace global_parameters {

std::string serialize(const GlobalParameters& src) {
  std::ostringstream s;
  boost::archive::text_oarchive oa(s);
  oa << src;
  return s.str();
}

GlobalParameters deserialize(const std::string& src) {
  GlobalParameters data;
  std::istringstream s(src);
  boost::archive::text_iarchive ia(s);
  ia >> data;
  return data;
}

void writeToFile(const std::string& file, const GlobalParameters& gp) {
  fbpcf::io::FileIOWrappers::writeFile(file, serialize(gp));
}

GlobalParameters readFromFile(const std::string& file) {
  return deserialize(fbpcf::io::FileIOWrappers::readFile(file));
}

} // namespace global_parameters
