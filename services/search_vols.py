import bisect
import traceback
from datetime import datetime, timezone

import pandas as pd
import yaml

from models.mysql import MySQLConnector


class SearchVolumeService:
    """
    Service class for executing search volume queries with user subscription validation.
    """

    def __init__(self, sql: MySQLConnector) -> None:
        """
        Initializes class instance.

        Parameters:
            config (dict): The configuration in dictionary format.
        """
        self.mysql = sql
        self.mysql.init_engine()

    def _validate_input(self, request: dict) -> tuple:
        """
        Validates the request payload.

        Parameters:
            request (dict): The request data.

        Returns:
            tuple: (bool, dict) - True if valid, otherwise False with error messages.
        """
        expected_fields = ["user_id", "keywords_id", "timing", "start_time", "end_time"]
        missing_fields = []
        errors_msg = ""

        for field in expected_fields:
            field_value = request.get(field)
            if not field_value:
                # errors[field] = f"Missing required field. Please check !!!"
                missing_fields.append(field)

        if len(missing_fields) > 0:
            errors_msg = f"Missing required fields {', '.join(missing_fields)}."
            return False, errors_msg

        if request["timing"] not in ["HOURLY", "DAILY"]:
            errors_msg = f"Only support 'HOURLY' and 'DAILY' timing."

        return (len(errors_msg) == 0), errors_msg

    def _check_query_time_range(
        self, start_time: datetime, end_time: datetime, subscription_range: list
    ) -> bool:
        """
        Checks if the given time range falls within the provided subscription ranges.

        Parameters:
            - start_time (datetime): Query start time.
            - end_time (datetime): Query end time.
            - subscription_ranges (list): List of dictionaries with "START_TIME" and "END_TIME".

        Returns:
            bool: True if the query range is within a subscription range, else False.
        """
        range_starts = [r["START_TIME"] for r in subscription_range]
        range_ends = [r["END_TIME"] for r in subscription_range]

        # Binary search
        pos = bisect.bisect_right(range_starts, start_time) - 1

        if pos >= 0 and start_time >= range_starts[pos] and end_time <= range_ends[pos]:
            return True

        return False

    def _union_subscription_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Merges overlapping subscription time ranges in a given DataFrame.

        Parameters:
            - df (DataFrame): DataFrame containing "START_TIME" and "END_TIME".

        Returns:
            DataFrame: A DataFrame with merged time ranges.
        """
        merged_range = []

        for row in df.to_dict("records"):
            start, end = row["START_TIME"], row["END_TIME"]

            if len(merged_range) == 0 or start > merged_range[-1][1]:
                merged_range.append([start, end])
            else:
                merged_range[-1][1] = max(merged_range[-1][1], end)

        merged_df = pd.DataFrame(merged_range, columns=["START_TIME", "END_TIME"])
        return merged_df

    def _check_user_subscriptions(self, params: dict, user_subs: pd.DataFrame) -> tuple:
        """
        Validates if the user's subscription covers the requested time range.

        Parameters:
            - params (dict): Request parameters.
            - user_subs (DataFrame): DataFrame containing user subscription details.

        Returns:
            tuple: (bool, str) - True if valid, otherwise False with an error message.
        """
        # Determine if the user has the required subscription level
        available_subscriptions = user_subs["SUBSCRIPTION_TYPE"].unique().tolist()

        if (
            params["timing"] == "HOURLY"
            and params["timing"] not in available_subscriptions
        ):
            status = "Hourly data requires an hourly subscription"
            return False, status

        # Validate query date range with `subscription_type = "HOURLY"`
        if params["timing"] == "HOURLY":
            relevant_subs = user_subs[user_subs["SUBSCRIPTION_TYPE"] == "HOURLY"]

        # Validate query date range with `subscription_type = "DAILY"`
        else:
            relevant_subs = user_subs

        merged_subs = self._union_subscription_time(relevant_subs)
        valid = self._check_query_time_range(
            params["start_time"], params["end_time"], merged_subs.to_dict("records")
        )

        if not valid:
            return (
                False,
                f"{params['timing']} query time range is out of subscription time range.",
            )

        return True, None

    def _query_search_volume_data(
        self, keyword_id: str, start_time: datetime, end_time: datetime, subs_type: str
    ) -> list:
        """
        Queries search volume data.

        Parameters:
            - keyword_id (str): Keyword ID.
            - start_time (datetime): Start time of the query.
            - end_time (datetime): End time of the query.
            - subs_type (str): "HOURLY" or "DAILY".

        Returns:
            tuple: (list, str) - Search volume data records and keyword name.
        """
        table = None
        keywords_df = self.mysql.query_with_in_list_condition(
            "keywords", keyword_id=[keyword_id]
        )
        keyword_name = keywords_df.iloc[0]["KEYWORD_NAME"]

        if subs_type == "HOURLY":
            table = "keyword_search_volume"
            query = f"""
                SELECT created_datetime, search_volume
                FROM {table}
                WHERE keyword_id = {keyword_id}
                AND created_datetime BETWEEN '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
                ORDER BY created_datetime
            """
            df = self.mysql.query_with_sql_command(query)
            df["CREATED_DATETIME"] = pd.to_datetime(df["CREATED_DATETIME"]).dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

        elif subs_type == "DAILY":
            table = "keyword_search_volume_daily"
            query = f"""
                SELECT created_date, search_volume
                FROM {table}
                WHERE keyword_id = {keyword_id}
                AND created_date BETWEEN '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
                ORDER BY created_date
            """
            df = self.mysql.query_with_sql_command(query)
            df["CREATED_DATE"] = pd.to_datetime(df["CREATED_DATE"]).dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

        return df.to_dict("records"), keyword_name

    def execute_query_data(self, request: dict) -> tuple:
        """
        Processes the search volume query for the user.

        Parameters:
            request (dict): The request payload.
        """
        # Validate input step
        valid, errors = self._validate_input(request)
        if not valid:
            return errors, 400

        try:
            keywords_id_lst = [
                int(kw.strip()) for kw in request["keywords_id"].split(",")
            ]
            params = {
                "user_id": request["user_id"],
                "keywords_id": keywords_id_lst,
                "timing": request["timing"],
                "start_time": datetime.fromtimestamp(
                    int(request["start_time"]), tz=timezone.utc
                ).date(),
                "end_time": datetime.fromtimestamp(
                    int(request["end_time"]), tz=timezone.utc
                ).date(),
            }

            # Check for user subscription validation
            users_sub_df: pd.DataFrame = self.mysql.query_with_in_list_condition(
                "users_subscription",
                ",".join(["keyword_id", "subscription_type", "start_time", "end_time"]),
                user_id=[params["user_id"]],
                keyword_id=params["keywords_id"],
            )

            if len(users_sub_df) == 0:
                status = f"User doesn't have any subscriptions with keywords_id {','.join(str(x) for x in keywords_id_lst)}"
                return status, 403

            query_result = []
            user_subs_kw_ids = users_sub_df["KEYWORD_ID"].unique().tolist()
            for keyword in keywords_id_lst:
                if keyword not in user_subs_kw_ids:
                    status = f"No subscriptions found for the keyword_id {keyword}"
                    result = {
                        "keyword_id": keyword,
                        "error": True,
                        "status": status,
                        "data": [],
                    }
                    query_result.append(result)
                    continue

                # Check user subscriptions for each keyword only
                keyword_subs_df = users_sub_df[users_sub_df["KEYWORD_ID"] == keyword]
                valid, status = self._check_user_subscriptions(params, keyword_subs_df)

                if not valid:
                    result = {
                        "keyword_id": keyword,
                        "error": True,
                        "status": status,
                        "data": [],
                    }
                    query_result.append(result)
                    continue

                query_data, keyword_name = self._query_search_volume_data(
                    keyword, params["start_time"], params["end_time"], params["timing"]
                )
                result = {
                    "keyword_id": keyword,
                    "keyword_name": keyword_name,
                    "error": False,
                    "status": "Successful",
                    "data": query_data,
                }
                query_result.append(result)

            return query_result, 200

        except Exception as e:
            # print(traceback.format_exc())
            status = f"Internal Server Error. Details: {e}"
            return status, 500


if __name__ == "__main__":
    # Read config file (config.yml)
    config = None
    with open("config.yml", encoding="utf-8") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    mysql = MySQLConnector(config["MYSQL_CONNECT"])

    search_vols_service = SearchVolumeService(mysql)
    request = {
        "user_id": 6,
        "keywords_id": "2,4",
        "timing": "HOURLY",
        "start_time": "1735689600",  # 2025-01-01
        "end_time": "1736467200",  # 2025-01-12
    }
    status, status_code = search_vols_service.execute_query_data(request)
    pass
