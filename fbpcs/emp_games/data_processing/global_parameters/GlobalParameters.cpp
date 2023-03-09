/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbpcf/io/api/BufferedReader.h>
#include <fbpcf/io/api/BufferedWriter.h>
#include <fbpcf/io/api/FileReader.h>
#include <fbpcf/io/api/FileWriter.h>

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
  auto writer = std::make_unique<fbpcf::io::BufferedWriter>(
      std::make_unique<fbpcf::io::FileWriter>(file));
  auto string = global_parameters::serialize(gp);
  writer->writeString(string);
  writer->close();
}

GlobalParameters readFromFile(const std::string& file) {
  auto reader = std::make_unique<fbpcf::io::BufferedReader>(
      std::make_unique<fbpcf::io::FileReader>(file));
  auto serializedGlobalParameters = reader->readLine();
  while (!reader->eof()) {
    auto line = reader->readLine();
    serializedGlobalParameters += "\n" + line;
  }
  reader->close();
  return global_parameters::deserialize(serializedGlobalParameters);
}

} // namespace global_parameters
