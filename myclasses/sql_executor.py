import pandas as pd
from sqlalchemy import create_engine, text

class SQLExecutor:
    """Class to connect to a MySQL Database and execute SQL queries, returning results as pandas DataFrames.

    This class provides methods to connect to a MySQL database using SQLAlchemy, execute SQL queries,
    and return the results as pandas DataFrames.

    Attributes:
        DB_URI (str): The URI for connecting to the MySQL database.

    Methods:
        __init__(): Initialize the SQLExecutor object and create an SQLAlchemy engine.
        execute_query(sql_query: str) -> pd.DataFrame: Execute the provided SQL query and return the results as a DataFrame.
    """

    DB_URI = "mysql+mysqlconnector://josh:go$T4GS@localhost/data_4999"
    
    def __init__(self):
        """Initialize the SQLExecutor object and create an SQLAlchemy engine."""
        self.engine = create_engine(SQLExecutor.DB_URI)

    def execute_query(self, sql_query: str) -> pd.DataFrame:
        """Execute the provided SQL query and return the results as a DataFrame.

        Args:
            sql_query (str): The SQL query to be executed.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the results of the SQL query.
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(sql_query))
            dataframe = pd.DataFrame(result.fetchall(), columns=result.keys())
            self.engine.dispose()
            return dataframe
