#include <gtest/gtest.h>

#include "helpers/sensors/GPSUpdateSchedule.h"

TEST(GPSUpdateScheduleTest, ScheduleNowIsImmediatelyDue) {
  GPSUpdateSchedule schedule;

  schedule.scheduleNow(1234);

  EXPECT_TRUE(schedule.isScheduled());
  EXPECT_TRUE(schedule.isDue(1234));
}

TEST(GPSUpdateScheduleTest, ZeroIntervalCancelsPeriodicUpdates) {
  GPSUpdateSchedule schedule;
  schedule.scheduleNow(100);

  schedule.scheduleAfter(100, 0);

  EXPECT_FALSE(schedule.isScheduled());
  EXPECT_FALSE(schedule.isDue(UINT32_MAX));
}

TEST(GPSUpdateScheduleTest, ConsumedDeadlineDoesNotRepeat) {
  GPSUpdateSchedule schedule;
  schedule.scheduleNow(100);
  ASSERT_TRUE(schedule.isDue(100));

  schedule.consume();

  EXPECT_FALSE(schedule.isDue(101));
  EXPECT_FALSE(schedule.isDue(UINT32_MAX));
}

TEST(GPSUpdateScheduleTest, PeriodicDeadlineSurvivesMillisWrap) {
  GPSUpdateSchedule schedule;
  constexpr uint32_t start = UINT32_MAX - 499;

  schedule.scheduleAfter(start, 1);

  EXPECT_EQ(500U, schedule.deadline());
  EXPECT_FALSE(schedule.isDue(499));
  EXPECT_TRUE(schedule.isDue(500));
  EXPECT_TRUE(schedule.isDue(501));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
