/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/frontend/mpcGame.h>
#include <fbpcf/mpc_std_lib/oram/IWriteOnlyOram.h>
#include <fbpcf/mpc_std_lib/oram/LinearOramFactory.h>
#include <fbpcf/scheduler/IScheduler.h>
#include <fbpcf/scheduler/SchedulerHelper.h>

constexpr int32_t PUBLISHER_ROLE = 0;
constexpr int32_t PARTNER_ROLE = 1;

using fbpcf::engine::communication::IPartyCommunicationAgentFactory;
using fbpcf::engine::communication::SocketPartyCommunicationAgentFactory;
using fbpcf::engine::util::AesPrgFactory;
using fbpcf::frontend::MpcGame;
using fbpcf::mpc_std_lib::oram::IWriteOnlyOram;
using fbpcf::mpc_std_lib::oram::LinearOram;
using fbpcf::mpc_std_lib::oram::LinearOramFactory;
using fbpcf::scheduler::createLazySchedulerWithRealEngine;
using fbpcf::scheduler::IScheduler;
using fbpcf::scheduler::SchedulerKeeper;

template <int schedulerId, bool usingBatch = false>
class SimpleAddingGame : public MpcGame<schedulerId> {
  using SecInt =
      typename MpcGame<schedulerId>::template SecSignedInt<64, usingBatch>;
  using AggType = uint32_t;
  using OramRole = typename IWriteOnlyOram<AggType>::Role;

 public:
  SimpleAddingGame(
      std::unique_ptr<IScheduler> scheduler,
      std::unique_ptr<IPartyCommunicationAgentFactory> f)
      : MpcGame<schedulerId>(std::move(scheduler)),
        commAgentFactory_{std::move(f)} {}

  static OramRole getOramRoleFromInt(int32_t role) {
    switch (role) {
      case PUBLISHER_ROLE:
        return OramRole::Alice;
      case PARTNER_ROLE:
        return OramRole::Bob;
      default:
        std::cerr << "Unknown role: " << role << '\n';
        std::exit(1);
    }
  }

  int64_t run(int32_t myRole, std::vector<int64_t> myInput) {
    SecInt publisher{myInput, PUBLISHER_ROLE};
    SecInt partner{myInput, PARTNER_ROLE};
    SecInt res = publisher + partner;
    return sum(myRole, res);
  }

  int64_t sum(int32_t myRole, const SecInt& v) {
    std::cout << "Creating ORAM\n";
    auto oram = makeOram<AggType>(myRole, 1);
    std::vector<std::vector<bool>> indexShares;
    auto valueShares = v.extractIntShare().getBooleanShares();
    for (size_t i = 0; i < valueShares.size(); ++i) {
      // All index shares can simply be zero since we're not actually
      // grouping values in any way
      indexShares.emplace_back(valueShares.at(0).size(), 0);
    }
    oram->obliviousAddBatch(indexShares, valueShares);
    auto a = oram->publicRead(0, getOramRoleFromInt(PUBLISHER_ROLE));
    auto b = oram->publicRead(0, getOramRoleFromInt(PARTNER_ROLE));
    return myRole == PUBLISHER_ROLE ? a : b;
  }

 private:
  template <typename T>
  std::unique_ptr<IWriteOnlyOram<T>> makeOram(int32_t myRole, size_t oramSize) {
    return std::make_unique<
               fbpcf::mpc_std_lib::oram::LinearOramFactory<T, schedulerId>>(
               myRole == PUBLISHER_ROLE ? OramRole::Alice : OramRole::Bob,
               myRole,
               1 - myRole, // convert 0 to 1 and 1 to 0
               *commAgentFactory_,
               std::make_unique<AesPrgFactory>())
        ->create(oramSize);
  }

  std::unique_ptr<IPartyCommunicationAgentFactory> commAgentFactory_;
};

std::vector<int64_t> readInput(const std::string& filename) {
  std::ifstream ifs{filename};
  std::string line;

  std::vector<int64_t> res;
  while (ifs >> line) {
    res.push_back(std::stoi(line));
  }
  return res;
}

int main(int argc, char** argv) {
  if (argc != 3 && argc != 5) {
    std::cerr << "Usage: " << argv[0] << " filename role [host port]\n";
    std::cerr << "  role = 0 for publisher, 1 for partner\n";
    std::cerr << "  default host=localhost, default port=8080\n";
    std::exit(1);
  }

  std::string filename{argv[1]};
  int32_t role = std::atoi(argv[2]);
  std::string host{argc == 5 ? argv[3] : "localhost"};
  int32_t port = argc == 5 ? std::atoi(argv[4]) : 8080;

  auto input = readInput(filename);

  std::cout << "Creating communication agent factory\n";
  std::map<int, SocketPartyCommunicationAgentFactory::PartyInfo> partyInfos{
      {{PUBLISHER_ROLE, {host, port}}, {PARTNER_ROLE, {host, port}}}};
  auto commAgentFactory =
      std::make_unique<SocketPartyCommunicationAgentFactory>(
          role, std::move(partyInfos));

  std::cout << "Creating scheduler\n";
  auto scheduler = createLazySchedulerWithRealEngine(role, *commAgentFactory);

  // TODO: Check scheduler Id?
  std::cout << "Starting game\n";
  auto game = SimpleAddingGame<0, true>(
      std::move(scheduler), std::move(commAgentFactory));

  auto res = game.run(role, input);
  std::cout << "Game done!\n";
  std::cout << "Output: " << res << '\n';

  auto stats = SchedulerKeeper<0>::getTrafficStatistics();
  std::cout << "Tx bytes: " << stats.first << '\n';
  std::cout << "Rx bytes: " << stats.second << '\n';

  return 0;
}
