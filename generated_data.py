import random

import loguru
import numpy as np
import pandas as pd
import yaml

from models.mysql import MySQLConnector

MAX_KEYWORDS = 10

logger = loguru.logger


def generate_sample_keywords_data() -> pd.DataFrame:
    """
    Function for generating sample keywords data with 10 keywords.
    """
    keywords = [
        "floating shelves",
        "fireplace mantel",
        "wall shelf",
        "butcher block countertop",
        "fireplace surround",
        "work bench",
        "countertop",
        "work table",
        "floating shelf",
        "bed frame",
        "floating wall shelf",
        "display shelf",
        "mantel",
        "wood shelves",
    ]
    df_keywords = pd.DataFrame(
        {
            "keyword_id": range(1, MAX_KEYWORDS + 1),
            "keyword_name": [f"{keywords[i]}" for i in range(MAX_KEYWORDS)],
        }
    )
    return df_keywords


def generate_sample_search_vols_data(df_keywords: pd.DataFrame) -> tuple:
    """
    Function for generating sample search volume data for 10 keywords in 3 months.
    """
    np.random.seed(42)
    random.seed(42)

    # Generate hourly search volume data for 3 months
    start_date = "2025-01-01 00:00:00"
    end_date = "2025-03-31 23:00:00"
    timestamps = pd.date_range(start=start_date, end=end_date, freq="H")

    data = []
    for keyword_id in df_keywords["keyword_id"]:
        for ts in timestamps:
            # Random search volume between 100 and 5000
            search_volume = np.random.randint(100, 5000)
            data.append(
                {
                    "keyword_id": keyword_id,
                    "created_datetime": ts,
                    "search_volume": search_volume,
                }
            )
    df_keyword_search_volume = pd.DataFrame(data)

    # Create noise for case "if 9:00AM data is not available".
    # Step 1: Randomly select 3 keywords and 10 days to create exception.
    # Step 2: Generate exception data by filtered out a selected value.
    random_keywords = random.sample(range(1, len(df_keywords) + 1), 3)
    random_ts = random.sample(timestamps.tolist(), 10)

    total_exception_ts = []
    for ts in random_ts:
        random_hour = random.choice([[9], [8, 9], [8, 9, 10], [7, 8, 9, 10]])
        for i in random_hour:
            remove_data = ts.replace(hour=i)
            total_exception_ts.append(remove_data)

    df_remove_data = df_keyword_search_volume[
        (df_keyword_search_volume["keyword_id"].isin(random_keywords))
        & (df_keyword_search_volume["created_datetime"].isin(total_exception_ts))
    ]
    df_keyword_search_volume = df_keyword_search_volume.drop(index=df_remove_data.index)

    return df_keyword_search_volume, df_remove_data


def generate_subscribes_sample():
    """
    Function for generating subscribe examples for all cases in the requirements.
    """
    total_users = []
    total_users_subscription = []

    #   - Test scenarios:
    #       + Between START_TIME and END_TIME (overlap and non-overlap)
    #       + (QUERY_START_TIME < START_TIME) & (START_TIME <= QUERY_END_TIME <= END_TIME)
    #       + (START_TIME <= QUERY_START_TIME <= END_TIME) & (QUERY_END_TIME > END_TIME))
    #       + QUERY RANGE not between START_TIME and END_TIME

    # Case 1:
    #   - Hourly Subscription for a Single Keyword
    #   - Including overlap case
    user_1 = {
        "USER_ID": 1,
        "USERNAME": "user_1",
        "EMAIL": "user_1@gmail.com",
        "FIRST_NAME": "User",
        "LAST_NAME": "1",
    }
    user_sub_1_1 = {
        "USER_ID": 1,
        "KEYWORD_ID": 1,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-01",
        "END_TIME": "2025-01-10",
    }
    user_sub_1_2 = {
        "USER_ID": 1,
        "KEYWORD_ID": 1,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-07",
        "END_TIME": "2025-01-20",
    }
    total_users.append(user_1)
    total_users_subscription.extend([user_sub_1_1, user_sub_1_2])

    # Case 2:
    #   - Daily Subscription for a Single Keyword
    #   - Including overlap case
    user_2 = {
        "USER_ID": 2,
        "USERNAME": "user_2",
        "EMAIL": "user_2@gmail.com",
        "FIRST_NAME": "User",
        "LAST_NAME": "2",
    }
    user_sub_2_1 = {
        "USER_ID": 2,
        "KEYWORD_ID": 5,
        "SUBSCRIPTION_TYPE": "DAILY",
        "START_TIME": "2025-01-01",
        "END_TIME": "2025-01-12",
    }
    user_sub_2_2 = {
        "USER_ID": 2,
        "KEYWORD_ID": 5,
        "SUBSCRIPTION_TYPE": "DAILY",
        "START_TIME": "2025-01-10",
        "END_TIME": "2025-01-25",
    }
    total_users.append(user_2)
    total_users_subscription.extend([user_sub_2_1, user_sub_2_2])

    # Case 3:
    #   - Hourly Subscription for Multiple Keywords.
    user_3 = {
        "USER_ID": 3,
        "USERNAME": "user_3",
        "EMAIL": "user_3@gmail.com",
        "FIRST_NAME": "User",
        "LAST_NAME": "3",
    }
    user_sub_3_1 = {
        "USER_ID": 3,
        "KEYWORD_ID": 1,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-01",
        "END_TIME": "2025-01-10",
    }
    user_sub_3_2 = {
        "USER_ID": 3,
        "KEYWORD_ID": 2,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-03",
        "END_TIME": "2025-01-15",
    }
    total_users.append(user_3)
    total_users_subscription.extend([user_sub_3_1, user_sub_3_2])

    # Case 4:
    #   - Daily Subscription for Multiple Keywords.
    user_4 = {
        "USER_ID": 4,
        "USERNAME": "user_4",
        "EMAIL": "user_4@gmail.com",
        "FIRST_NAME": "User",
        "LAST_NAME": "4",
    }
    user_sub_4_1 = {
        "USER_ID": 4,
        "KEYWORD_ID": 6,
        "SUBSCRIPTION_TYPE": "DAILY",
        "START_TIME": "2025-01-01",
        "END_TIME": "2025-01-10",
    }
    user_sub_4_2 = {
        "USER_ID": 4,
        "KEYWORD_ID": 7,
        "SUBSCRIPTION_TYPE": "DAILY",
        "START_TIME": "2025-01-03",
        "END_TIME": "2025-01-15",
    }
    user_sub_4_3 = {
        "USER_ID": 4,
        "KEYWORD_ID": 8,
        "SUBSCRIPTION_TYPE": "DAILY",
        "START_TIME": "2025-01-05",
        "END_TIME": "2025-01-12",
    }
    total_users.append(user_4)
    total_users_subscription.extend([user_sub_4_1, user_sub_4_2, user_sub_4_3])

    # Case 5:
    #   - Hourly and Daily for the Same Keyword
    #   - Including overlap case between DAILY and HOURLY
    user_5 = {
        "USER_ID": 5,
        "USERNAME": "user_5",
        "EMAIL": "user_5@gmail.com",
        "FIRST_NAME": "User",
        "LAST_NAME": "5",
    }
    user_sub_5_1 = {
        "USER_ID": 5,
        "KEYWORD_ID": 2,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-01",
        "END_TIME": "2025-01-10",
    }
    user_sub_5_2 = {
        "USER_ID": 5,
        "KEYWORD_ID": 2,
        "SUBSCRIPTION_TYPE": "DAILY",
        "START_TIME": "2025-01-04",
        "END_TIME": "2025-01-15",
    }
    total_users.append(user_5)
    total_users_subscription.extend([user_sub_5_1, user_sub_5_2])

    # Case 6:
    #   - Hourly and Daily for the Multiple Keywords
    #   - Including overlap case
    user_6 = {
        "USER_ID": 6,
        "USERNAME": "user_6",
        "EMAIL": "user_6@gmail.com",
        "FIRST_NAME": "User",
        "LAST_NAME": "6",
    }
    user_sub_6_1 = {
        "USER_ID": 6,
        "KEYWORD_ID": 2,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-01",
        "END_TIME": "2025-01-12",
    }
    user_sub_6_2 = {
        "USER_ID": 6,
        "KEYWORD_ID": 3,
        "SUBSCRIPTION_TYPE": "DAILY",
        "START_TIME": "2025-01-01",
        "END_TIME": "2025-01-15",
    }
    user_sub_6_3 = {
        "USER_ID": 6,
        "KEYWORD_ID": 4,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-05",
        "END_TIME": "2025-01-10",
    }
    user_sub_6_4 = {
        "USER_ID": 6,
        "KEYWORD_ID": 4,
        "SUBSCRIPTION_TYPE": "HOURLY",
        "START_TIME": "2025-01-10",
        "END_TIME": "2025-01-18",
    }
    total_users.append(user_6)
    total_users_subscription.extend(
        [user_sub_6_1, user_sub_6_2, user_sub_6_3, user_sub_6_4]
    )

    # Case 7: Subscription with No Data
    user_7 = {
        "USER_ID": 7,
        "USERNAME": "user_7",
        "EMAIL": "user_7@gmail.com",
        "FIRST_NAME": "User",
        "LAST_NAME": "7",
    }
    total_users.append(user_7)

    df_total_users = pd.DataFrame(total_users)
    df_total_users_sub = pd.DataFrame(total_users_subscription)

    return df_total_users, df_total_users_sub


# Read config file (config.yml)
config = None
with open("config.yml", encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# Init MySQL Connector
mysql = MySQLConnector(config["MYSQL_CONNECT"])
mysql.init_engine()

# Step 1: Execute the SQL file to initialize schema (init_schema.sql)
with open("sql/init_schema.sql", "r") as file:
    sql_script = file.read()

for command in sql_script.split(";"):
    if len(command) > 0:
        mysql.execute_sql_command(command.strip())
logger.info("Initialize database schema successful")

# Step 2: Add SQL Procedure for automate update `keyword_search_volume_daily` table
with open("sql/procedure.sql", "r") as file:
    sql_script = file.read()
mysql.execute_sql_command(sql_script)
logger.info("Initialize database procedure successful")

mysql.execute_sql_command("call UpdateDailySearchVolumes()")
logger.info("Running database procedure successful")

# Step 3: Generate sample seach volumes data
df_keywords = generate_sample_keywords_data()
df_keyword_search_volume, df_remove_data = generate_sample_search_vols_data(df_keywords)

mysql.insert_to_table("keywords", df_keywords)
logger.info("Insert data into `keywords` table successful")

mysql.insert_to_table("keyword_search_volume", df_keyword_search_volume)
logger.info("Insert data into `keyword_search_volume` table successful")

mysql.insert_to_table("rm_keyword_search_volume", df_remove_data)
logger.info("Insert data into `rm_keyword_search_volume` table successful")

# Step 4: Generate users subscription data
df_total_users, df_total_users_sub = generate_subscribes_sample()
mysql.insert_to_table("users", df_total_users)
logger.info("Insert data into `users` table successful")

mysql.insert_to_table("users_subscription", df_total_users_sub)
logger.info("Insert data into `users_subscription` table successful")