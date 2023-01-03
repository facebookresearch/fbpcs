/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <exception>
#include <future>
#include <random>
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/dotproduct/DotproductGame.h"

#include "fbpcf/engine/tuple_generator/oblivious_transfer/EmpShRandomCorrelatedObliviousTransferFactory.h"
#include "fbpcf/engine/tuple_generator/oblivious_transfer/ExtenderBasedRandomCorrelatedObliviousTransferFactory.h"
#include "fbpcf/engine/tuple_generator/oblivious_transfer/ferret/RcotExtenderFactory.h"
#include "fbpcf/engine/tuple_generator/oblivious_transfer/ferret/RegularErrorMultiPointCotFactory.h"
#include "fbpcf/engine/tuple_generator/oblivious_transfer/ferret/SinglePointCotFactory.h"
#include "fbpcf/engine/tuple_generator/oblivious_transfer/ferret/TenLocalLinearMatrixMultiplierFactory.h"
#include "fbpcf/engine/util/AesPrgFactory.h"

#include "fbpcf/mpc_std_lib/walr_multiplication/IWalrMatrixMultiplication.h"
#include "fbpcf/mpc_std_lib/walr_multiplication/IWalrMatrixMultiplicationFactory.h"
#include "fbpcf/mpc_std_lib/walr_multiplication/OTBasedMatrixMultiplication.h"
#include "fbpcf/mpc_std_lib/walr_multiplication/OTBasedMatrixMultiplicationFactory.h"
#include "fbpcf/mpc_std_lib/walr_multiplication/util/COTWithRandomMessageFactory.h"

namespace pcf2_dotproduct {

template <int schedulerId>
std::vector<double> DotproductGame<schedulerId>::computeDotProduct(
    const int myRole,
    const std::tuple<
        std::vector<std::vector<double>>,
        std::vector<std::vector<bool>>> inputTuple,
    size_t nLabels,
    size_t nFeatures,
    double delta,
    double eps,
    const bool addDpNoise) {
  // Plaintext label secret share
  std::vector<std::vector<bool>> labels = std::get<1>(inputTuple);

  // Create label secret shares
  auto labelShare = createSecretLabelShare(labels);
  XLOG(INFO, "Created Label secret shares");

  // Do ORing of all the labels
  auto finalLabel = orAllLabels(labelShare);
  XLOG(INFO, "Performed the OR for all labels");

  constexpr uint64_t divisor = static_cast<uint64_t>(1e9);
  constexpr double tolerance = 1e-7;

  auto prgFactory = std::make_unique<fbpcf::engine::util::AesPrgFactory>();

  auto rcotFactory = std::make_unique<
      fbpcf::engine::tuple_generator::oblivious_transfer::
          ExtenderBasedRandomCorrelatedObliviousTransferFactory>(
      std::make_unique<fbpcf::engine::tuple_generator::oblivious_transfer::
                           EmpShRandomCorrelatedObliviousTransferFactory>(
          std::make_unique<fbpcf::engine::util::AesPrgFactory>(1024)),
      std::make_unique<fbpcf::engine::tuple_generator::oblivious_transfer::
                           ferret::RcotExtenderFactory>(
          std::make_unique<fbpcf::engine::tuple_generator::oblivious_transfer::
                               ferret::TenLocalLinearMatrixMultiplierFactory>(),
          std::make_unique<fbpcf::engine::tuple_generator::oblivious_transfer::
                               ferret::RegularErrorMultiPointCotFactory>(
              std::make_unique<
                  fbpcf::engine::tuple_generator::oblivious_transfer::ferret::
                      SinglePointCotFactory>())),
      fbpcf::engine::tuple_generator::oblivious_transfer::ferret::kExtendedSize,
      fbpcf::engine::tuple_generator::oblivious_transfer::ferret::kBaseSize,
      fbpcf::engine::tuple_generator::oblivious_transfer::ferret::kWeight);

  auto cotWRMFactory = std::make_unique<
      fbpcf::mpc_std_lib::walr::util::COTWithRandomMessageFactory>(
      std::move(rcotFactory));

  std::vector<double> rst;
  if (myRole == common::PUBLISHER) {
    // Read features
    std::vector<std::vector<double>> features = std::get<0>(inputTuple);

    // Create matrix multiplication factory
    auto matMulFactoryPublisher = std::make_unique<
        fbpcf::mpc_std_lib::walr::
            OTBasedMatrixMultiplicationFactory<schedulerId, uint64_t>>(
        myRole,
        1 - myRole,
        true,
        divisor,
        *communicationAgentFactory_,
        std::move(prgFactory),
        std::move(cotWRMFactory),
        metricCollector_);

    XLOG(INFO, "Created Matrix Multiplication Factory");

    rst = matMulFactoryPublisher->create()->matrixVectorMultiplication(
        features, finalLabel);

  } else if (myRole == common::PARTNER) {
    // Create noise vector
    const std::vector<double> dpNoise =
        generateDpNoise(nFeatures, delta, eps, addDpNoise);

    // Create matrix multiplication factory
    auto matMulFactoryPartner = std::make_unique<
        fbpcf::mpc_std_lib::walr::
            OTBasedMatrixMultiplicationFactory<schedulerId, uint64_t>>(
        myRole,
        1 - myRole,
        false,
        divisor,
        *communicationAgentFactory_,
        std::move(prgFactory),
        std::move(cotWRMFactory),
        metricCollector_);
    XLOG(INFO, "Created Matrix Multiplication Factory");

    matMulFactoryPartner->create()->matrixVectorMultiplication(
        finalLabel, dpNoise);
  }
  return rst;
}

template <int schedulerId>
std::vector<double> DotproductGame<schedulerId>::generateDpNoise(
    const int nFeatures,
    const double delta,
    const double eps,
    const bool addDpNoise) {
  std::vector<double> dpNoise(nFeatures, 0.0);
  if (addDpNoise) {
    // Noise generator
    std::random_device rd;
    std::mt19937_64 gen(rd());

    // calculate variance  k * 2 * ln ( 1 / delta) / (eps^2)
    const double variance = nFeatures * 2 * log(1 / delta) / pow(eps, 2);

    std::normal_distribution<double> gaussianNoise{0, std::sqrt(variance)};

    for (auto& item : dpNoise) {
      item = gaussianNoise(gen);
    }
  }
  return dpNoise;
}

template <int schedulerId>
std::vector<fbpcf::frontend::Bit<true, schedulerId, true>>
DotproductGame<schedulerId>::createSecretLabelShare(
    const std::vector<std::vector<bool>>& labelValues) {
  std::vector<fbpcf::frontend::Bit<true, schedulerId, true>> label0;
  for (size_t i = 0; i < labelValues.size(); i++) {
    // XLOG(INFO, labelValues.at(i)[0]);
    label0.push_back(fbpcf::frontend::Bit<true, schedulerId, true>(
        typename fbpcf::frontend::Bit<true, schedulerId, true>::ExtractedBit(
            labelValues.at(i))));
  }
  return label0;
}

template <int schedulerId>
fbpcf::frontend::Bit<true, schedulerId, true>
DotproductGame<schedulerId>::orAllLabels(
    const std::vector<fbpcf::frontend::Bit<true, schedulerId, true>>& labels) {
  if (labels.size() == 1) {
    return labels.at(0);
  }
  size_t size = labels.size();
  std::vector<fbpcf::frontend::Bit<true, schedulerId, true>> const* src =
      &labels;
  std::vector<fbpcf::frontend::Bit<true, schedulerId, true>> dst(
      (size + 1) / 2);

  while (size > 1) {
    for (size_t i = 0; i < size / 2; i++) {
      dst[i] = src->at(2 * i) | src->at(2 * i + 1);
    }
    if (size & 1) {
      dst[size / 2] = src->at(size - 1);
    }
    src = &dst;
    size = (size + 1) / 2;
  }
  return dst.at(0);
}

} // namespace pcf2_dotproduct
