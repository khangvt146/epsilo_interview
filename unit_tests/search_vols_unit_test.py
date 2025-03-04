import unittest
import warnings
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import yaml

from models.mysql import MySQLConnector
from services.search_vols import SearchVolumeService


class TestSearchVolumeService(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore")
        with open("config.yml", encoding="utf-8") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        mysql = MySQLConnector(config["MYSQL_CONNECT"])
        self.service = SearchVolumeService(mysql)

    # ======================== Input Validation Tests ===============================
    def test_validate_input_missing_fields(self):
        # Test case: Missing required fields (e.g., timing, start_time, end_time)
        request = {"user_id": 1, "keywords_id": "1,2"}
        valid, errors = self.service._validate_input(request)
        self.assertFalse(valid)
        self.assertIn("Missing required fields timing, start_time, end_time", errors)

    def test_validate_input_invalid_timing(self):
        # Test case: Invalid timing value (not HOURLY or DAILY)
        request = {
            "user_id": 1,
            "keywords_id": "1,2",
            "timing": "MONTHLY",
            "start_time": "1609459200",
            "end_time": "1609545600",
        }
        valid, errors = self.service._validate_input(request)
        self.assertFalse(valid)
        self.assertEqual(errors, "Only support 'HOURLY' and 'DAILY' timing.")

    def test_validate_input_valid(self):
        # Test case: Valid input with all required fields
        request = {
            "user_id": 1,
            "keywords_id": "1,2",
            "timing": "DAILY",
            "start_time": "1609459200",
            "end_time": "1609545600",
        }
        valid, errors = self.service._validate_input(request)
        self.assertTrue(valid)
        self.assertEqual(errors, "")

    # ======================== Query Time Range Tests ===============================
    def test_check_query_time_range_within_range(self):
        # Test case: Query time range fully within subscription range
        start_time = datetime(2025, 1, 5, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 8, tzinfo=timezone.utc)
        subscription_range = [
            {
                "START_TIME": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "END_TIME": datetime(2025, 1, 4, tzinfo=timezone.utc),
            },
            {
                "START_TIME": datetime(2025, 1, 5, tzinfo=timezone.utc),
                "END_TIME": datetime(2025, 1, 10, tzinfo=timezone.utc),
            },
        ]
        result = self.service._check_query_time_range(
            start_time, end_time, subscription_range
        )
        self.assertTrue(result)

    def test_check_query_time_range_outside_range(self):
        # Test case: Query time range outside subscription range
        start_time = datetime(2025, 1, 5, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 6, tzinfo=timezone.utc)
        subscription_range = [
            {
                "START_TIME": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "END_TIME": datetime(2025, 1, 4, tzinfo=timezone.utc),
            },
            {
                "START_TIME": datetime(2025, 1, 7, tzinfo=timezone.utc),
                "END_TIME": datetime(2025, 1, 10, tzinfo=timezone.utc),
            },
        ]
        result = self.service._check_query_time_range(
            start_time, end_time, subscription_range
        )
        self.assertFalse(result)

    # ======================== Subscription Union Tests ===============================
    def test_union_subscription_time_overlapping(self):
        # Test case: Overlapping subscription time ranges
        df = pd.DataFrame(
            {
                "START_TIME": [
                    datetime(2023, 1, 1, tzinfo=timezone.utc),
                    datetime(2023, 1, 3, tzinfo=timezone.utc),
                ],
                "END_TIME": [
                    datetime(2023, 1, 4, tzinfo=timezone.utc),
                    datetime(2023, 1, 5, tzinfo=timezone.utc),
                ],
            }
        )
        merged = self.service._union_subscription_time(df)
        self.assertEqual(len(merged), 1)
        self.assertEqual(
            merged.iloc[0]["START_TIME"], datetime(2023, 1, 1, tzinfo=timezone.utc)
        )
        self.assertEqual(
            merged.iloc[0]["END_TIME"], datetime(2023, 1, 5, tzinfo=timezone.utc)
        )

    def test_union_subscription_time_non_overlapping(self):
        # Test case: Non-overlapping subscription time ranges
        df = pd.DataFrame(
            {
                "START_TIME": [
                    datetime(2023, 1, 1, tzinfo=timezone.utc),
                    datetime(2023, 1, 5, tzinfo=timezone.utc),
                ],
                "END_TIME": [
                    datetime(2023, 1, 2, tzinfo=timezone.utc),
                    datetime(2023, 1, 6, tzinfo=timezone.utc),
                ],
            }
        )
        merged = self.service._union_subscription_time(df)
        self.assertEqual(len(merged), 2)

    # ======================== Subscription Validation Tests ===============================
    def test_check_user_subscriptions_no_hourly_subscription(self):
        # Test case: Hourly query with no hourly subscription
        params = {
            "timing": "HOURLY",
            "start_time": datetime(2023, 1, 2, tzinfo=timezone.utc),
            "end_time": datetime(2023, 1, 3, tzinfo=timezone.utc),
        }
        user_subs = pd.DataFrame({"SUBSCRIPTION_TYPE": ["DAILY"]})
        valid, status = self.service._check_user_subscriptions(params, user_subs)
        self.assertFalse(valid)
        self.assertEqual(status, "Hourly data requires an hourly subscription")

    def test_check_user_subscriptions_valid_hourly(self):
        # Test case: Valid hourly subscription and time range
        params = {
            "timing": "HOURLY",
            "start_time": datetime(2023, 1, 2, tzinfo=timezone.utc),
            "end_time": datetime(2023, 1, 3, tzinfo=timezone.utc),
        }
        user_subs = pd.DataFrame(
            {
                "SUBSCRIPTION_TYPE": ["HOURLY"],
                "START_TIME": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                "END_TIME": [datetime(2023, 1, 4, tzinfo=timezone.utc)],
            }
        )
        valid, status = self.service._check_user_subscriptions(params, user_subs)
        self.assertTrue(valid)
        self.assertIsNone(status)

    def test_check_user_subscriptions_invalid_time_range(self):
        # Test case: Daily query with time range outside subscription
        params = {
            "timing": "DAILY",
            "start_time": datetime(2023, 1, 5, tzinfo=timezone.utc),
            "end_time": datetime(2023, 1, 6, tzinfo=timezone.utc),
        }
        user_subs = pd.DataFrame(
            {
                "SUBSCRIPTION_TYPE": ["DAILY"],
                "START_TIME": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                "END_TIME": [datetime(2023, 1, 4, tzinfo=timezone.utc)],
            }
        )
        valid, status = self.service._check_user_subscriptions(params, user_subs)
        self.assertFalse(valid)
        self.assertEqual(
            status, "DAILY query time range is out of subscription time range."
        )

    # =================================================================
    # | -------------------- FULL QUERY FLOW TEST --------------------|
    # =================================================================

    # ===================== Input validation test =====================
    def test_input_validation_failed_1(self):
        """
        Test input validation failed (missing required fields)
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "timing": "HOURLY",
        }
        errors, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 400)
        self.assertIn("Missing required fields start_time, end_time", errors)

    def test_input_validation_failed_2(self):
        """
        Test input validation failed (missing required fields)
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "start_time": "1672531200",
            "end_time": "1672790400",
        }
        errors, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 400)
        self.assertIn("Missing required fields timing", errors)

    def test_input_validation_failed_3(self):
        """
        Test input validation failed (timing is not 'HOURLY' or 'DAILY')
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "timing": "MONTHLY",
            "start_time": "1672531200",
            "end_time": "1672790400",
        }
        errors, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 400)
        self.assertIn("Only support 'HOURLY' and 'DAILY' timing.", errors)

    def test_input_validation_failed_4(self):
        """
        Test input validation failed (invalid timestamp format)
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "timing": "DAILY",
            "start_time": "Khang",
            "end_time": "1672790400",
        }
        errors, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 500)
        self.assertIn(
            "Internal Server Error. Details: invalid literal for int() with base 10: 'Khang'",
            errors,
        )

    # =================== No/Insufficient subscription test =====================
    def test_no_subscription_for_keyword(self):
        """
        Test query with no subscription for the keyword.
        """
        request = {
            "user_id": 1,
            "keywords_id": "3,2",
            "timing": "DAILY",
            "start_time": "1672531200",
            "end_time": "1672790400",
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 403)
        self.assertIn(
            f"User doesn't have any subscriptions with keywords_id {request['keywords_id']}",
            result,
        )

    def test_insufficient_subscription_type(self):
        """
        Test HOURLY query with only DAILY subscription.
        """
        request = {
            "user_id": 2,
            "keywords_id": "5",
            "timing": "HOURLY",
            "start_time": "1672531200",
            "end_time": "1736899200",
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertTrue(result[0]["error"])
        self.assertIn(
            "Hourly data requires an hourly subscription", result[0]["status"]
        )

    # ================= Hourly Subscription for a Single Keyword test =====================
    def test_valid_hourly_subscription_single_keyword_1(self):
        """
        Test valid hourly subscription with single_keyword (non-overlap subscription time)
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "timing": "HOURLY",
            "start_time": "1735689600",  # 2025-01-01
            "end_time": "1736035200",  # 2025-01-05
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertFalse(result[0]["error"])
        self.assertIn("Successful", result[0]["status"])
        self.assertGreater(len(result[0]["data"]), 0)

    def test_valid_hourly_subscription_single_keyword_2(self):
        """
        Test valid hourly subscription with single_keyword (overlap subscription time)
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "timing": "HOURLY",
            "start_time": "1736294400",  # 2025-01-08
            "end_time": "1736985600",  # 2025-01-16
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertFalse(result[0]["error"])
        self.assertIn("Successful", result[0]["status"])
        self.assertGreater(len(result[0]["data"]), 0)

    def test_invalid_hourly_subscription_single_keyword(self):
        """
        Test invalid hourly subscription with single_keyword (out of subscription time)
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "timing": "HOURLY",
            "start_time": "1736985600",  # 2025-01-16
            "end_time": "1737763200",  # 2025-01-25
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertTrue(result[0]["error"])
        self.assertIn(
            "HOURLY query time range is out of subscription time range.",
            result[0]["status"],
        )
        self.assertEqual(len(result[0]["data"]), 0)

    # ================= Daily Subscription for a Single Keyword test =====================
    def test_valid_daily_subscription_single_keyword_1(self):
        """
        Test valid daily subscription with single_keyword (non-overlap subscription time)
        """
        request = {
            "user_id": 2,
            "keywords_id": "5",
            "timing": "DAILY",
            "start_time": "1735689600",  # 2025-01-01
            "end_time": "1736035200",  # 2025-01-05
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertFalse(result[0]["error"])
        self.assertIn("Successful", result[0]["status"])
        self.assertGreater(len(result[0]["data"]), 0)

    def test_valid_daily_subscription_single_keyword_2(self):
        """
        Test valid daily subscription with single_keyword (overlap subscription time)
        """
        request = {
            "user_id": 2,
            "keywords_id": "5",
            "timing": "DAILY",
            "start_time": "1735689600",  # 2025-01-01
            "end_time": "1737331200",  # 2025-01-20
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertFalse(result[0]["error"])
        self.assertIn("Successful", result[0]["status"])
        self.assertGreater(len(result[0]["data"]), 0)

    def test_invalid_daily_subscription_single_keyword(self):
        """
        Test invalid daily subscription with single_keyword (out of subscription time)
        """
        request = {
            "user_id": 1,
            "keywords_id": "1",
            "timing": "DAILY",
            "start_time": "1737763200",  # 2025-01-25
            "end_time": "1738195200",  # 2025-01-30
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertTrue(result[0]["error"])
        self.assertIn(
            "DAILY query time range is out of subscription time range.",
            result[0]["status"],
        )
        self.assertEqual(len(result[0]["data"]), 0)

    # ================ Hourly Subscription for a Multiple Keywords test =====================
    def test_valid_hourly_subscription_multiple_keyword_1(self):
        """
        Test valid hourly subscription with multiple keyword.
        (2 keywords are valid subscription time)
        """
        request = {
            "user_id": 3,
            "keywords_id": "1,2",
            "timing": "HOURLY",
            "start_time": "1736035200",  # 2025-01-05
            "end_time": "1736467200",  # 2025-01-10
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)

        # First keyword result
        result_1 = result[0]
        self.assertFalse(result_1["error"])
        self.assertIn("Successful", result_1["status"])
        self.assertGreater(len(result_1["data"]), 0)

        # Second keyword result
        result_2 = result[1]
        self.assertFalse(result_2["error"])
        self.assertIn("Successful", result_2["status"])
        self.assertGreater(len(result_2["data"]), 0)

    def test_valid_hourly_subscription_multiple_keyword_2(self):
        """
        Test valid hourly subscription with multiple keyword.
        (1 keywords is valid subscription time, otherwise is not)
        """
        request = {
            "user_id": 3,
            "keywords_id": "1,2",
            "timing": "HOURLY",
            "start_time": "1736467200",  # 2025-01-10
            "end_time": "1736899200",  # 2025-01-15
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)

        # First keyword result
        result_1 = result[0]
        self.assertTrue(result_1["error"])
        self.assertIn(
            "HOURLY query time range is out of subscription time range.",
            result_1["status"],
        )
        self.assertEqual(len(result_1["data"]), 0)

        # Second keyword result
        result_2 = result[1]
        self.assertFalse(result_2["error"])
        self.assertIn("Successful", result_2["status"])
        self.assertGreater(len(result_2["data"]), 0)

    def test_invalid_hourly_subscription_multiple_keyword(self):
        """
        Test invalid hourly subscription with multiple keyword.
        (2 keywords is invalid subscription time)
        """
        request = {
            "user_id": 3,
            "keywords_id": "1,2",
            "timing": "HOURLY",
            "start_time": "1738368000",  # 2025-02-01
            "end_time": "1738454400",  # 2025-02-02
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)

        # First keyword result
        result_1 = result[0]
        self.assertTrue(result_1["error"])
        self.assertIn(
            "HOURLY query time range is out of subscription time range.",
            result_1["status"],
        )
        self.assertEqual(len(result_1["data"]), 0)

        # Second keyword result
        result_2 = result[1]
        self.assertTrue(result_2["error"])
        self.assertIn(
            "HOURLY query time range is out of subscription time range.",
            result_2["status"],
        )
        self.assertEqual(len(result_2["data"]), 0)

    # ================ Daily Subscription for a Multiple Keywords test =====================
    def test_valid_daily_subscription_multiple_keyword_1(self):
        """
        Test valid daily subscription with multiple keyword.
        (2 keywords are valid subscription time)
        """
        request = {
            "user_id": 4,
            "keywords_id": "6,7,8",
            "timing": "DAILY",
            "start_time": "1736035200",  # 2025-01-05
            "end_time": "1736121600",  # 2025-01-06
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)

        # First keyword result
        result_1 = result[0]
        self.assertFalse(result_1["error"])
        self.assertIn("Successful", result_1["status"])
        self.assertGreater(len(result_1["data"]), 0)

        # Second keyword result
        result_2 = result[1]
        self.assertFalse(result_2["error"])
        self.assertIn("Successful", result_2["status"])
        self.assertGreater(len(result_2["data"]), 0)

        # Third keyword result
        result_3 = result[2]
        self.assertFalse(result_3["error"])
        self.assertIn("Successful", result_3["status"])
        self.assertGreater(len(result_3["data"]), 0)

    def test_valid_daily_subscription_multiple_keyword_2(self):
        """
        Test valid daily subscription with multiple keyword.
        (2 keywords are valid subscription time, 1 invalid)
        """
        request = {
            "user_id": 4,
            "keywords_id": "6,7,8",
            "timing": "DAILY",
            "start_time": "1736553600",  # 2025-01-11
            "end_time": "1736640000",  # 2025-01-12
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)

        # First keyword result
        result_1 = result[0]
        self.assertTrue(result_1["error"])
        self.assertIn(
            "DAILY query time range is out of subscription time range.",
            result_1["status"],
        )
        self.assertEqual(len(result_1["data"]), 0)

        # Second keyword result
        result_2 = result[1]
        self.assertFalse(result_2["error"])
        self.assertIn("Successful", result_2["status"])
        self.assertGreater(len(result_2["data"]), 0)

        # Third keyword result
        result_3 = result[2]
        self.assertFalse(result_3["error"])
        self.assertIn("Successful", result_3["status"])
        self.assertGreater(len(result_3["data"]), 0)

    def test_valid_daily_subscription_multiple_keyword_3(self):
        """
        Test valid daily subscription with multiple keyword.
        (3 keywords are invalid subscription time)
        """
        request = {
            "user_id": 4,
            "keywords_id": "6,7,8",
            "timing": "DAILY",
            "start_time": "1738368000",  # 2025-02-01
            "end_time": "1738454400",  # 2025-02-02
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)

        # First keyword result
        result_1 = result[0]
        self.assertTrue(result_1["error"])
        self.assertIn(
            "DAILY query time range is out of subscription time range.",
            result_1["status"],
        )
        self.assertEqual(len(result_1["data"]), 0)

        # Second keyword result
        result_2 = result[1]
        self.assertTrue(result_2["error"])
        self.assertIn(
            "DAILY query time range is out of subscription time range.",
            result_2["status"],
        )
        self.assertEqual(len(result_2["data"]), 0)

        # Third keyword result
        result_3 = result[2]
        self.assertTrue(result_3["error"])
        self.assertIn(
            "DAILY query time range is out of subscription time range.",
            result_3["status"],
        )
        self.assertEqual(len(result_3["data"]), 0)

    # ========== Hourly/Daily Subscription for a Same Keyword test =====================
    def test_overlap_hourly_daily_query_1(self):
        """
        Test overlap hourly/daily query with single keyword.
        (subscribe hourly will see the daily data)
        """
        request = {
            "user_id": 5,
            "keywords_id": "2",
            "timing": "DAILY",
            "start_time": "1735689600",  # 2025-01-01
            "end_time": "1735948800",  # 2025-01-04
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertFalse(result[0]["error"])
        self.assertIn("Successful", result[0]["status"])
        self.assertGreater(len(result[0]["data"]), 0)

    def test_overlap_hourly_daily_query_2(self):
        """
        Test overlap hourly/daily query with single keyword.
        (subscribe daily will not see data hourly)
        """
        request = {
            "user_id": 5,
            "keywords_id": "2",
            "timing": "HOURLY",
            "start_time": "1736726400",  # 2025-01-13
            "end_time": "1736899200",  # 2025-01-15
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)
        self.assertTrue(result[0]["error"])
        self.assertIn("HOURLY query time range is out of subscription time range.", result[0]["status"])
        self.assertEqual(len(result[0]["data"]), 0)

    # ========== Hourly/Daily Subscription for Multiple Keywords test =====================
    def test_valid_daily_hourly_subscription_multiple_keyword_1(self):
        """
        Test valid daily subscription with multiple keyword (including overlap case).
        """
        request = {
            "user_id": 6,
            "keywords_id": "2,4",
            "timing": "HOURLY",
            "start_time": "1736035200",  # 2025-01-05
            "end_time": "1736640000",  # 2025-01-12
        }
        result, status_code = self.service.execute_query_data(request)
        self.assertEqual(status_code, 200)

        # First keyword result
        result_1 = result[0]
        self.assertFalse(result_1["error"])
        self.assertIn("Successful", result_1["status"])
        self.assertGreater(len(result_1["data"]), 0)

        # Second keyword result
        result_2 = result[1]
        self.assertFalse(result_2["error"])
        self.assertIn("Successful", result_2["status"])
        self.assertGreater(len(result_2["data"]), 0)



if __name__ == "__main__":
    unittest.main()
