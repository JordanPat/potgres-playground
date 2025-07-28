import os
import pandas as pd
from sqlalchemy import create_engine, text


class PostgresHandler:
    """
    params:
    return:
    """
    def __init__(self, pg_host=None, pg_port=None, pg_user=None, pg_password=None, pg_db=None):
        if pg_host is None or pg_port is None or pg_user is None or pg_password is None or pg_db is None:
            self.db_user = os.getenv("DB_USER", "user")
            self.db_password = os.getenv("DB_PASSWORD", "password")
            self.db_host = os.getenv("DB_HOST", "postgres-service")
            self.db_port = os.getenv("DB_PORT", "5432")
            self.db_name = os.getenv("DB_NAME", "dev_db")
        else:
            self.db_user = pg_user
            self.db_password = pg_password
            self.db_host = pg_host
            self.db_port = pg_port
            self.db_name = pg_db
        print("[__init__]db variables:",self.db_host, self.db_port, self.db_user, self.db_password, self.db_name)
        self.connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        print("[__init__]connection_string:", self.connection_string)
        # Create SQLAlchemy engine for PostgreSQL connection
        self.engine = create_engine(
            self.connection_string)


    def test_connection(self):
        """
        params:
        return:
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
                )
                tables = [row[0] for row in result]
                print("Hello World! Connected to PostgreSQL successfully.")
                print("Tables in the DB:", tables)
        except Exception as e:
            print("Failed to connect to PostgreSQL:", e)


    def get_existing_uids(self, engine, table_name, uid_list) -> set:
        """
        params:
        return:
        """
        query = str(f"SELECT uid FROM {table_name} WHERE uid = ANY(:uids)")
        with engine.connect() as conn:
            existing = pd.read_sql(query, conn, params={"uids": uid_list})
        return set(existing["uid"].tolist())


    def insert_nonduplicates(self, df: pd.DataFrame):
        """
        params:
        return:
        """
        incoming_uids = df['uid'].unique().tolist()
        existing_uids = self.get_existing_uids(self.engine, 'my_table', incoming_uids)

        # Separate
        df_new = df[~df['uid'].isin(existing_uids)]         # Truly new

        # Insert only new
        df_new.to_sql("my_table", self.engine, if_exists='append', index=False)

        # Save df_existing for later comparison or upsert
        return df[df['uid'].isin(existing_uids)]


    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists='append'):
        """
        params:
        return:
        """
        try:
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            print(f"Data inserted into table '{table_name}' successfully.")
        except Exception as e:
            print(f"Failed to insert data into '{table_name}':", e)
