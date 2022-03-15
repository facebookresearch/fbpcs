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
#include <vector>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/frontend/mpcGame.h>
#include <fbpcf/scheduler/IScheduler.h>
#include <fbpcf/scheduler/SchedulerHelper.h>

constexpr int32_t PUBLISHER_ROLE = 0;
constexpr int32_t PARTNER_ROLE = 1;

using fbpcf::engine::communication::SocketPartyCommunicationAgentFactory;
using fbpcf::frontend::MpcGame;
using fbpcf::scheduler::createLazySchedulerWithRealEngine;
using fbpcf::scheduler::IScheduler;
using fbpcf::scheduler::SchedulerKeeper;

template <int schedulerId, bool usingBatch = false>
class SimpleAddingGame : public MpcGame<schedulerId> {
  // TODO: Batching false, SecSignedInt<64, true> for batch support
  using SecInt =
      typename fbpcf::frontend::MpcGame<schedulerId>::template SecSignedInt<64>;

 public:
  explicit SimpleAddingGame(std::unique_ptr<IScheduler> scheduler)
      : MpcGame<schedulerId>(std::move(scheduler)) {}

  int64_t run(int32_t myRole, std::vector<int64_t> myInput) {
    SecInt res;
    for (size_t i = 0; i < myInput.size(); ++i) {
      SecInt publisher{myInput.at(i), PUBLISHER_ROLE};
      SecInt partner{myInput.at(i), PARTNER_ROLE};
      // TODO: Int::operator+= isn't defined either :(
      res = res + publisher + partner;
    }
    // TODO: Support XOR share
    // NOTE: Even though we only care about *myRole* below, we need
    // to execute both lines so both parties get output.
    auto a = res.openToParty(PUBLISHER_ROLE).getValue();
    auto b = res.openToParty(PARTNER_ROLE).getValue();
    return myRole == PUBLISHER_ROLE ? a : b;
  }
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
      {{0, {host, port}}, {1, {host, port}}}};
  auto commAgentFactory =
      std::make_unique<SocketPartyCommunicationAgentFactory>(
          role, std::move(partyInfos));

  std::cout << "Creating scheduler\n";
  auto scheduler = createLazySchedulerWithRealEngine(role, *commAgentFactory);

  // TODO: Check scheduler Id?
  std::cout << "Starting game\n";
  auto game = SimpleAddingGame<0>(std::move(scheduler));
  auto res = game.run(role, input);
  std::cout << "Game done!\n";
  std::cout << "Output: " << res << '\n';

  auto stats = SchedulerKeeper<0>::getTrafficStatistics();
  std::cout << "Tx bytes: " << stats.first << '\n';
  std::cout << "Rx bytes: " << stats.second << '\n';

  return 0;
}
