/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionRule.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"

namespace pcf2_attribution {

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class LastClick_1Day
    : public AttributionRule<schedulerId, usingBatch, inputEncryption> {
 public:
  LastClick_1Day()
      : AttributionRule<schedulerId, usingBatch, inputEncryption>(
            /* id */ 1,
            /* name */ common::LAST_CLICK_1D) {}

  SecBit<schedulerId, usingBatch> isAttributable(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>& tp,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>& conv,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>& thresholds)
      const override {
    return (tp.ts < conv.ts) & (conv.ts <= thresholds.at(0));
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPlaintext(
      const Touchpoint<usingBatch>& tp) const override {
    ConditionalVector<uint32_t, usingBatch> thresholdOneDayClick;
    if constexpr (usingBatch) {
      for (size_t i = 0; i < tp.ts.size(); ++i) {
        bool isValidClick = tp.isClick.at(i) & (tp.ts.at(i) > 0);
        uint32_t thresholdOneDay = tp.ts.at(i) + kSecondsInOneDay;
        thresholdOneDayClick.push_back(isValidClick ? thresholdOneDay : 0);
      }
    } else {
      bool isValidClick = tp.isClick & (tp.ts > 0);
      uint32_t thresholdOneDay = tp.ts + kSecondsInOneDay;
      thresholdOneDayClick = isValidClick ? thresholdOneDay : 0;
    }
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        SecTimestamp<schedulerId, usingBatch>(
            thresholdOneDayClick, common::PUBLISHER)};
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPrivate(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&
          privateTp,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&
          privateIsClick,
      size_t batchSize) const override {
    PubTimestamp<schedulerId, usingBatch> zero;
    PubTimestamp<schedulerId, usingBatch> secondsInOneDay;
    if constexpr (usingBatch) {
      zero = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, 0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInOneDay));
    } else {
      zero = PubTimestamp<schedulerId, usingBatch>(uint32_t(0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(kSecondsInOneDay);
    }
    auto isValidClick = privateIsClick.isClick & (zero < privateTp.ts);
    auto thresholdOneDay = privateTp.ts + secondsInOneDay;
    auto thresholdOneDayClick = zero.mux(isValidClick, thresholdOneDay);
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        thresholdOneDayClick};
  }
};

/**
 * Attribute if the conversion took place within 28 days of the touchpoint
 */
template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class LastClick_28Days
    : public AttributionRule<schedulerId, usingBatch, inputEncryption> {
 public:
  LastClick_28Days()
      : AttributionRule<schedulerId, usingBatch, inputEncryption>(
            /* id */ 2,
            /* name */ common::LAST_CLICK_28D) {}

  SecBit<schedulerId, usingBatch> isAttributable(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>& tp,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>& conv,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>& thresholds)
      const override {
    return (tp.ts < conv.ts) & (conv.ts <= thresholds.at(0));
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPlaintext(
      const Touchpoint<usingBatch>& tp) const override {
    ConditionalVector<uint32_t, usingBatch> thresholdTwentyEightDaysClick;
    if constexpr (usingBatch) {
      for (size_t i = 0; i < tp.ts.size(); ++i) {
        bool isValidClick = tp.isClick.at(i) & (tp.ts.at(i) > 0);
        auto thresholdTwentyEightDays = tp.ts.at(i) + kSecondsInTwentyEightDays;
        thresholdTwentyEightDaysClick.push_back(
            isValidClick ? thresholdTwentyEightDays : 0);
      }
    } else {
      bool isValidClick = tp.isClick & (tp.ts > 0);
      auto thresholdTwentyEightDays = tp.ts + kSecondsInTwentyEightDays;
      thresholdTwentyEightDaysClick =
          isValidClick ? thresholdTwentyEightDays : 0;
    }
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        SecTimestamp<schedulerId, usingBatch>(
            thresholdTwentyEightDaysClick, common::PUBLISHER)};
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPrivate(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&
          privateTp,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&
          privateIsClick,
      size_t batchSize) const override {
    PubTimestamp<schedulerId, usingBatch> zero;
    PubTimestamp<schedulerId, usingBatch> secondsInTwentyEightDays;
    if constexpr (usingBatch) {
      zero = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, 0));
      secondsInTwentyEightDays = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInTwentyEightDays));
    } else {
      zero = PubTimestamp<schedulerId, usingBatch>(uint32_t(0));
      secondsInTwentyEightDays =
          PubTimestamp<schedulerId, usingBatch>(kSecondsInTwentyEightDays);
    }
    auto isValidClick = privateIsClick.isClick & (zero < privateTp.ts);
    auto thresholdTwentyEightDays = privateTp.ts + secondsInTwentyEightDays;
    auto thresholdTwentyEightDaysClick =
        zero.mux(isValidClick, thresholdTwentyEightDays);
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        thresholdTwentyEightDaysClick};
  }
};

/**
 * The last touch attribution model gives 100% of the credit for a conversion
 * to the last click that happened in a conversion path. If there was no
 * click, then it will credit the last impression.
 */
template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class LastTouch_CT1Day_Impression1Day
    : public AttributionRule<schedulerId, usingBatch, inputEncryption> {
 public:
  LastTouch_CT1Day_Impression1Day()
      : AttributionRule<schedulerId, usingBatch, inputEncryption>(
            /* id */ 3,
            /* name */ common::LAST_TOUCH_1D) {}

  /*  if click within 1d, if touch within 1d */
  SecBit<schedulerId, usingBatch> isAttributable(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>& tp,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>& conv,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>& thresholds)
      const override {
    return (tp.ts < conv.ts) & (conv.ts <= thresholds.at(0));
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPlaintext(
      const Touchpoint<usingBatch>& tp) const override {
    ConditionalVector<uint32_t, usingBatch> thresholdOneDayTouch;
    if constexpr (usingBatch) {
      for (size_t i = 0; i < tp.ts.size(); ++i) {
        bool isValid = tp.ts.at(i) > 0;
        auto thresholdOneDay = tp.ts.at(i) + kSecondsInOneDay;
        thresholdOneDayTouch.push_back(isValid ? thresholdOneDay : 0);
      }
    } else {
      bool isValid = tp.ts > 0;
      auto thresholdOneDay = tp.ts + kSecondsInOneDay;
      thresholdOneDayTouch = isValid ? thresholdOneDay : 0;
    }
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        SecTimestamp<schedulerId, usingBatch>(
            thresholdOneDayTouch, common::PUBLISHER)};
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPrivate(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&
          privateTp,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&
      /* privateIsClick */,
      size_t batchSize) const override {
    PubTimestamp<schedulerId, usingBatch> zero;
    PubTimestamp<schedulerId, usingBatch> secondsInOneDay;
    if constexpr (usingBatch) {
      zero = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, 0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInOneDay));
    } else {
      zero = PubTimestamp<schedulerId, usingBatch>(uint32_t(0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(kSecondsInOneDay);
    }
    auto isValid = zero < privateTp.ts;
    auto thresholdOneDay = privateTp.ts + secondsInOneDay;
    auto thresholdOneDayTouch = zero.mux(isValid, thresholdOneDay);
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        thresholdOneDayTouch};
  }
};

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class LastTouch_CT28Day_Impression1Day
    : public AttributionRule<schedulerId, usingBatch, inputEncryption> {
 public:
  LastTouch_CT28Day_Impression1Day()
      : AttributionRule<schedulerId, usingBatch, inputEncryption>(
            /* id */ 4,
            /* name */ common::LAST_TOUCH_28D) {}

  /* if click within 28d, if touch within 1d */
  SecBit<schedulerId, usingBatch> isAttributable(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>& tp,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>& conv,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>& thresholds)
      const override {
    auto validConv = tp.ts < conv.ts;
    auto touchWithinOneDay = conv.ts <= thresholds.at(0);
    auto clickWithinTwentyEightDays = conv.ts <= thresholds.at(1);

    return validConv & (touchWithinOneDay | clickWithinTwentyEightDays);
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPlaintext(
      const Touchpoint<usingBatch>& tp) const override {
    ConditionalVector<uint32_t, usingBatch> thresholdOneDayTouch;
    ConditionalVector<uint32_t, usingBatch> thresholdTwentyEightDaysClick;
    if constexpr (usingBatch) {
      for (size_t i = 0; i < tp.ts.size(); ++i) {
        bool isValid = tp.ts.at(i) > 0;
        bool isValidClick = tp.isClick.at(i) & isValid;

        auto thresholdOneDay = tp.ts.at(i) + kSecondsInOneDay;
        thresholdOneDayTouch.push_back(isValid ? thresholdOneDay : 0);

        auto thresholdTwentyEightDays = tp.ts.at(i) + kSecondsInTwentyEightDays;
        thresholdTwentyEightDaysClick.push_back(
            isValidClick ? thresholdTwentyEightDays : 0);
      }
    } else {
      bool isValid = tp.ts > 0;
      bool isValidClick = tp.isClick & isValid;

      auto thresholdOneDay = tp.ts + kSecondsInOneDay;
      thresholdOneDayTouch = isValid ? thresholdOneDay : 0;

      auto thresholdTwentyEightDays = tp.ts + kSecondsInTwentyEightDays;
      thresholdTwentyEightDaysClick =
          isValidClick ? thresholdTwentyEightDays : 0;
    }
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        SecTimestamp<schedulerId, usingBatch>(
            thresholdOneDayTouch, common::PUBLISHER),
        SecTimestamp<schedulerId, usingBatch>(
            thresholdTwentyEightDaysClick, common::PUBLISHER)};
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPrivate(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&
          privateTp,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&
          privateIsClick,
      size_t batchSize) const override {
    PubTimestamp<schedulerId, usingBatch> zero;
    PubTimestamp<schedulerId, usingBatch> secondsInOneDay;
    PubTimestamp<schedulerId, usingBatch> secondsInTwentyEightDays;
    if constexpr (usingBatch) {
      zero = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, 0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInOneDay));
      secondsInTwentyEightDays = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInTwentyEightDays));
    } else {
      zero = PubTimestamp<schedulerId, usingBatch>(uint32_t(0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(kSecondsInOneDay);
      secondsInTwentyEightDays =
          PubTimestamp<schedulerId, usingBatch>(kSecondsInTwentyEightDays);
    }
    auto isValid = zero < privateTp.ts;
    auto isValidClick = privateIsClick.isClick & isValid;

    auto thresholdOneDay = privateTp.ts + secondsInOneDay;
    auto thresholdOneDayTouch = zero.mux(isValid, thresholdOneDay);

    auto thresholdTwentyEightDays = privateTp.ts + secondsInTwentyEightDays;
    auto thresholdTwentyEightDaysClick =
        zero.mux(isValidClick, thresholdTwentyEightDays);
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        thresholdOneDayTouch, thresholdTwentyEightDaysClick};
  }
};

/*
  Attribute if the conversion took place within 7 days but
  more than 1 day after the touchpoint
*/
template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class LastClick_2_7Days
    : public AttributionRule<schedulerId, usingBatch, inputEncryption> {
 public:
  LastClick_2_7Days()
      : AttributionRule<schedulerId, usingBatch, inputEncryption>(
            /* id */ 5,
            /* name */ common::LAST_CLICK_2_7D) {}

  /* if click is within 7d but after 1d */
  SecBit<schedulerId, usingBatch> isAttributable(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>& tp,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>& conv,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>& thresholds)
      const override {
    auto validConv = tp.ts < conv.ts;
    auto clickAfterOneDay = thresholds.at(0) < conv.ts;
    auto clickWithinSevenDays = conv.ts <= thresholds.at(1);

    return validConv & clickAfterOneDay & clickWithinSevenDays;
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPlaintext(
      const Touchpoint<usingBatch>& tp) const override {
    ConditionalVector<uint32_t, usingBatch> lowerBoundOneDayClick;
    ConditionalVector<uint32_t, usingBatch> upperBoundSevenDaysClick;

    if constexpr (usingBatch) {
      for (size_t i = 0; i < tp.ts.size(); ++i) {
        bool isValidClick = tp.isClick.at(i) & (tp.ts.at(i) > 0);
        uint32_t lowerBoundOneDay = tp.ts.at(i) + kSecondsInOneDay;
        uint32_t upperBoundSevenDays = tp.ts.at(i) + kSecondsInSevenDays;

        lowerBoundOneDayClick.push_back(isValidClick ? lowerBoundOneDay : 0);
        upperBoundSevenDaysClick.push_back(
            isValidClick ? upperBoundSevenDays : 0);
      }
    } else {
      bool isValidClick = tp.isClick & (tp.ts > 0);
      uint32_t lowerBoundOneDay = tp.ts + kSecondsInOneDay;
      uint32_t upperBoundSevenDays = tp.ts + kSecondsInSevenDays;

      lowerBoundOneDayClick = isValidClick ? lowerBoundOneDay : 0;
      upperBoundSevenDaysClick = isValidClick ? upperBoundSevenDays : 0;
    }

    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        SecTimestamp<schedulerId, usingBatch>(
            lowerBoundOneDayClick, common::PUBLISHER),
        SecTimestamp<schedulerId, usingBatch>(
            upperBoundSevenDaysClick, common::PUBLISHER)};
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPrivate(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&
          privateTp,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&
          privateIsClick,
      size_t batchSize) const override {
    PubTimestamp<schedulerId, usingBatch> zero;
    PubTimestamp<schedulerId, usingBatch> secondsInOneDay;
    PubTimestamp<schedulerId, usingBatch> secondsInSevenDays;
    if constexpr (usingBatch) {
      zero = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, 0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInOneDay));
      secondsInSevenDays = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInSevenDays));
    } else {
      zero = PubTimestamp<schedulerId, usingBatch>(uint32_t(0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(kSecondsInOneDay);
      secondsInSevenDays =
          PubTimestamp<schedulerId, usingBatch>(kSecondsInSevenDays);
    }

    auto isValidClick = privateIsClick.isClick & (zero < privateTp.ts);

    auto lowerBoundOneDay = privateTp.ts + secondsInOneDay;
    auto lowerBoundOneDayClick = zero.mux(isValidClick, lowerBoundOneDay);

    auto upperBoundSevenDay = privateTp.ts + secondsInSevenDays;
    auto upperBoundSevenDayClick = zero.mux(isValidClick, upperBoundSevenDay);

    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        lowerBoundOneDayClick, upperBoundSevenDayClick};
  }
};

/*
  Attribute to any click in the 2-7D window, favoring the
  most recent. If no such clicks exist, attribute to any
  impression in 1d, favoring the most recent.
*/
template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class LastTouch_2_7Days
    : public AttributionRule<schedulerId, usingBatch, inputEncryption> {
 public:
  LastTouch_2_7Days()
      : AttributionRule<schedulerId, usingBatch, inputEncryption>(
            /* id */ 6,
            /* name */ common::LAST_TOUCH_2_7D) {}

  SecBit<schedulerId, usingBatch> isAttributable(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>& tp,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>& conv,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>& thresholds)
      const override {
    auto validConv = tp.ts < conv.ts;
    auto clickAfterOneDay = thresholds.at(0) < conv.ts;
    auto clickWithinSevenDays = conv.ts <= thresholds.at(1);

    auto touchWithinOneDay = conv.ts <= thresholds.at(2);

    return validConv &
        ((clickAfterOneDay & clickWithinSevenDays) | touchWithinOneDay);
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPlaintext(
      const Touchpoint<usingBatch>& tp) const override {
    ConditionalVector<uint32_t, usingBatch> lowerBoundOneDayClick;
    ConditionalVector<uint32_t, usingBatch> upperBoundSevenDaysClick;
    ConditionalVector<uint32_t, usingBatch> upperBoundOneDayTouch;

    if constexpr (usingBatch) {
      for (size_t i = 0; i < tp.ts.size(); ++i) {
        bool isValid = tp.ts.at(i) > 0;
        bool isValidClick = tp.isClick.at(i) & isValid;
        uint32_t lowerBoundAndUpperBoundOneDay = tp.ts.at(i) + kSecondsInOneDay;
        uint32_t upperBoundSevenDays = tp.ts.at(i) + kSecondsInSevenDays;

        lowerBoundOneDayClick.push_back(
            isValidClick ? lowerBoundAndUpperBoundOneDay : 0);
        upperBoundSevenDaysClick.push_back(
            isValidClick ? upperBoundSevenDays : 0);
        upperBoundOneDayTouch.push_back(
            (isValid && !isValidClick) ? lowerBoundAndUpperBoundOneDay : 0);
      }
    } else {
      bool isValid = tp.ts > 0;
      bool isValidClick = tp.isClick & isValid;
      uint32_t lowerBoundAndUpperBoundOneDay = tp.ts + kSecondsInOneDay;
      uint32_t upperBoundSevenDays = tp.ts + kSecondsInSevenDays;

      lowerBoundOneDayClick = isValidClick ? lowerBoundAndUpperBoundOneDay : 0;
      upperBoundSevenDaysClick = isValidClick ? upperBoundSevenDays : 0;
      upperBoundOneDayTouch =
          (isValid & !isValidClick) ? lowerBoundAndUpperBoundOneDay : 0;
    }

    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        SecTimestamp<schedulerId, usingBatch>(
            lowerBoundOneDayClick, common::PUBLISHER),
        SecTimestamp<schedulerId, usingBatch>(
            upperBoundSevenDaysClick, common::PUBLISHER),
        SecTimestamp<schedulerId, usingBatch>(
            upperBoundOneDayTouch, common::PUBLISHER)};
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPrivate(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&
          privateTp,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&
          privateIsClick,
      size_t batchSize) const override {
    PubTimestamp<schedulerId, usingBatch> zero;
    PubTimestamp<schedulerId, usingBatch> secondsInOneDay;
    PubTimestamp<schedulerId, usingBatch> secondsInSevenDays;
    if constexpr (usingBatch) {
      zero = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, 0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInOneDay));
      secondsInSevenDays = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInSevenDays));
    } else {
      zero = PubTimestamp<schedulerId, usingBatch>(uint32_t(0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(kSecondsInOneDay);
      secondsInSevenDays =
          PubTimestamp<schedulerId, usingBatch>(kSecondsInSevenDays);
    }

    auto isValid = zero < privateTp.ts;
    auto isValidClick = privateIsClick.isClick & isValid;

    auto lowerBoundAndUpperBoundOneDay = privateTp.ts + secondsInOneDay;
    auto lowerBoundOneDayClick =
        zero.mux(isValidClick, lowerBoundAndUpperBoundOneDay);

    auto upperBoundSevenDay = privateTp.ts + secondsInSevenDays;
    auto upperBoundSevenDayClick = zero.mux(isValidClick, upperBoundSevenDay);

    auto upperBoundOneDayTouch =
        zero.mux((isValid & !isValidClick), lowerBoundAndUpperBoundOneDay);

    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        lowerBoundOneDayClick, upperBoundSevenDayClick, upperBoundOneDayTouch};
  }
};

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class LastClick_1Day_TargetId
    : public AttributionRule<schedulerId, usingBatch, inputEncryption> {
 public:
  LastClick_1Day_TargetId()
      : AttributionRule<schedulerId, usingBatch, inputEncryption>(
            /* id */ 7,
            /* name */ common::LAST_CLICK_1D_TARGETID) {}

  SecBit<schedulerId, usingBatch> isAttributable(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>& tp,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>& conv,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>& thresholds)
      const override {
    return (tp.targetId == conv.targetId) & (tp.actionType == conv.actionType) &
        (tp.ts < conv.ts) & (conv.ts <= thresholds.at(0));
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPlaintext(
      const Touchpoint<usingBatch>& tp) const override {
    ConditionalVector<uint32_t, usingBatch> thresholdOneDayClick;
    if constexpr (usingBatch) {
      for (size_t i = 0; i < tp.ts.size(); ++i) {
        bool isValidClick = tp.isClick.at(i) & (tp.ts.at(i) > 0);
        uint32_t thresholdOneDay = tp.ts.at(i) + kSecondsInOneDay;
        thresholdOneDayClick.push_back(isValidClick ? thresholdOneDay : 0);
      }
    } else {
      bool isValidClick = tp.isClick & (tp.ts > 0);
      uint32_t thresholdOneDay = tp.ts + kSecondsInOneDay;
      thresholdOneDayClick = isValidClick ? thresholdOneDay : 0;
    }
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        SecTimestamp<schedulerId, usingBatch>(
            thresholdOneDayClick, common::PUBLISHER)};
  }

  std::vector<SecTimestamp<schedulerId, usingBatch>> computeThresholdsPrivate(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&
          privateTp,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&
          privateIsClick,
      size_t batchSize) const override {
    PubTimestamp<schedulerId, usingBatch> zero;
    PubTimestamp<schedulerId, usingBatch> secondsInOneDay;
    if constexpr (usingBatch) {
      zero = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, 0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(
          std::vector<uint32_t>(batchSize, kSecondsInOneDay));
    } else {
      zero = PubTimestamp<schedulerId, usingBatch>(uint32_t(0));
      secondsInOneDay = PubTimestamp<schedulerId, usingBatch>(kSecondsInOneDay);
    }
    auto isValidClick = privateIsClick.isClick & (zero < privateTp.ts);
    auto thresholdOneDay = privateTp.ts + secondsInOneDay;
    auto thresholdOneDayClick = zero.mux(isValidClick, thresholdOneDay);
    return std::vector<SecTimestamp<schedulerId, usingBatch>>{
        thresholdOneDayClick};
  }
};

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
inline const auto SUPPORTED_ATTRIBUTION_RULES = std::vector<
    std::shared_ptr<AttributionRule<schedulerId, usingBatch, inputEncryption>>>{
    std::make_shared<
        LastClick_1Day<schedulerId, usingBatch, inputEncryption>>(),
    std::make_shared<
        LastClick_28Days<schedulerId, usingBatch, inputEncryption>>(),
    std::make_shared<LastTouch_CT1Day_Impression1Day<
        schedulerId,
        usingBatch,
        inputEncryption>>(),
    std::make_shared<LastTouch_CT28Day_Impression1Day<
        schedulerId,
        usingBatch,
        inputEncryption>>(),
    std::make_shared<
        LastClick_2_7Days<schedulerId, usingBatch, inputEncryption>>(),
    std::make_shared<
        LastTouch_2_7Days<schedulerId, usingBatch, inputEncryption>>(),
    std::make_shared<
        LastClick_1Day_TargetId<schedulerId, usingBatch, inputEncryption>>()};

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::shared_ptr<const AttributionRule<schedulerId, usingBatch, inputEncryption>>
AttributionRule<schedulerId, usingBatch, inputEncryption>::fromNameOrThrow(
    const std::string& name) {
  for (auto& rule :
       SUPPORTED_ATTRIBUTION_RULES<schedulerId, usingBatch, inputEncryption>) {
    if (rule->name == name) {
      return rule;
    }
  }

  throw std::runtime_error("Unknown attribution rule name: " + name);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::shared_ptr<const AttributionRule<schedulerId, usingBatch, inputEncryption>>
AttributionRule<schedulerId, usingBatch, inputEncryption>::fromIdOrThrow(
    std::int64_t id) {
  for (auto& rule :
       SUPPORTED_ATTRIBUTION_RULES<schedulerId, usingBatch, inputEncryption>) {
    if (rule->id == id) {
      return rule;
    }
  }

  throw std::runtime_error(fmt::format("Unknown attribution id: {}", id));
}
}; // namespace pcf2_attribution
