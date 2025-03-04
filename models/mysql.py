import copy
from functools import wraps
from typing import Any, Callable, Optional

import loguru
import numpy as np
import pandas as pd
import sqlalchemy as sqla
from sqlalchemy.orm import Session
from sqlalchemy.sql import text


class MySQLConnector:
    """
    Modules that provides functionality to interact with database through sqlalchemy.
    """

    def __init__(self, config: dict) -> None:
        """
        Initialize the MySQL connector.
        """
        self.config: Optional[dict] = config
        self.engine: Optional[sqla.Engine] = None
        self.engine_path: Optional[str] = None
        self.logger = loguru.logger

    @staticmethod
    def check_driver_engine(func: Callable) -> Callable:
        """
        Decorator to check if the engine and the engine's path is None or not
        """

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.engine is not None and self.engine_path is not None:
                return func(self, *args, **kwargs)

            self.logger.error(
                "The Driver engine is None. Please running init_engine() first !!!"
            )

        return wrapper

    def init_engine(self):
        """
        Initialize the MySQL connector engine.

        Parameters:
            - config (dict): A dictionary containing configuration settings such as
                USERNAME, PASSWORD, HOST, PORT, etc.
        """
        username = self.config["USERNAME"]
        password = self.config["PASSWORD"]
        host = self.config["HOST"]
        port = self.config["PORT"]
        db_name = self.config["DATABASE"]

        self.engine_path = (
            f"mysql+pymysql://{username}:{password}@" f"{host}:{port}/{db_name}"
        )
        self.engine = sqla.create_engine(self.engine_path)

    @check_driver_engine
    def execute_sql_command(self, sql: str) -> None:
        """
        Execute provided SQL command.

        Parameters:
            sql (str): The SQL command to execute.
        """
        with Session(self.engine) as session, session.begin():
            session.execute(text(sql))

    @check_driver_engine
    def insert_to_table(
        self, table_name: str, df: pd.DataFrame, schema: str = None
    ) -> None:
        """
        Insert a provided Pandas DataFrame into the specified table in the database.

        Parameters:
            - table_name (str): The name of the table in the database.
            - df (DataFrame): The DataFrame to be inserted into the database.
            - schema (str): The name of the schema where the table created in the database
        """
        if not df.empty:
            data = copy.deepcopy(df)
            table_name = table_name.lower()
            data.columns = data.columns.str.lower()
            data.to_sql(
                table_name,
                self.engine,
                if_exists="append",
                index=False,
                chunksize=10000,
                schema=schema,
            )

    @check_driver_engine
    def query_with_in_list_condition(
        self, table_name: str, *column, **args
    ) -> pd.DataFrame:
        """
        Query data from the database with provided list of equal query condition.

        Parameters:
            - table_name (str): The table name in the database.
            - column (list): The column names to retrieve data from.
            - args (dict): The query conditions

        Returns:
            pd.DataFrame: The resulting query DataFrame

        Example: Query data from column "A" and "B" of the table name "TEST TABLE" with conditions that
            the values in column "A" is 10 and 20.
            ------------------------------------------------------------------
            sql.query_with_in_list_condition("TEST TABLE", ["A", "B"], A = [10, 20])
            ------------------------------------------------------------------
        """
        table_name = table_name.lower()

        # The column names parser
        if len(column) == 0:
            col_sql = "*"
        else:
            col_sql = ", ".join(column)

        # The condition parser
        if len(args) == 0:
            condition_query = ""
        else:
            condition_query = "where "

            condition_list = []
            for key, value in args.items():
                if isinstance(value, list):
                    condition_list.append(
                        f"{key} in "
                        + "('"
                        + "', '".join(np.array(value, dtype=str))
                        + "')"
                    )
                else:
                    condition_list.append(f"{key} = {value}")
            condition_query += " and ".join(condition_list)

        sql = f"select {col_sql} " f"from {table_name} " f"{condition_query}"

        result = pd.read_sql_query(text(sql), self.engine)
        result.columns = map(str.upper, result.columns)
        return result

    @check_driver_engine
    def query_with_sql_command(self, sql: str) -> pd.DataFrame:
        """
        Query data from the database with provided SQL command.

        Parameters:
            sql (str): The SQL command to execute.

        Returns:
            pd.DataFrame: The resulting query DataFrame
        """
        result = pd.read_sql_query(text(sql), self.engine)
        result.columns = map(str.upper, result.columns)
        return result