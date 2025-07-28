# import pandas as pd
# import psycopg2
# import os
# import sqlalchemy


# data1 = {
#     "id":[1,2,3,4,5],
#     "data": [
#         ["listitem1","listitem2","listitem3"],
#         ["li1","li1","li1"],
#         ["li2","li2","li2"],
#         ["li3","li3","li3"],
#         ["li4","li4","li4"],
#     ],
#     "name": ["john1", "john2", "john3", "john4", "john5"]
# }
# data2 = {
#     "id":[1,2,3,4,5],
#     "data": [
#         [{"item":"value"}],[{"item":"value"}],[{"item":"value"}],[{"item":"value"}],[{"item":"value"}]
#     ],
#     "name": ["john1", "john2", "john3", "john4", "john5"]
# }

# def connect_and_test():
#     try:
#         conn = psycopg2.connect(
#             host=os.getenv("DB_HOST", "postgres"),
#             port=os.getenv("DB_PORT", "5432"),
#             database=os.getenv("DB_NAME", "dev_db"),
#             user=os.getenv("DB_USER", "user"),
#             password=os.getenv("DB_PASSWORD", "password")
#         )
#         print("Hello World! Connected to PostgreSQL successfully.")

#         # Example use of pandas: list tables
#         df = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public'", conn)
#         print("Tables in the DB:", df)

#         conn.close()
#     except Exception as e:
#         print("Failed to connect to PostgreSQL:", e)

# if __name__ == "__main__":
#     connect_and_test()
#     df = pd.DataFrame(data=data1)
#     df.to_sql()

import os
import pandas as pd

from pg_manager import PostgresHandler


if __name__ == "__main__":
    print("__main__ running...")
    pg_host = os.getenv("POSTGRES_HOST", "postgres-service")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_user = os.getenv("POSTGRES_USER", "user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "password")
    pg_db = os.getenv("POSTGRES_DB", "dev_db")
    print("[__main__]db variables:", pg_host, pg_port, pg_user, pg_password, pg_db)
    data1 = {
        "id":[1,2,3,4,5],
        "data": [
            ["listitem1","listitem2","listitem3"],
            ["li1","li1","li1"],
            ["li2","li2","li2"],
            ["li3","li3","li3"],
            ["li4","li4","li4"],
        ],
        "name": ["john1", "john2", "john3", "john4", "john5"]
    }
    df = pd.DataFrame(data1)

    pg_handler = PostgresHandler(pg_host, pg_port, pg_user, pg_password, pg_db)
    pg_handler.test_connection()
    pg_handler.insert_dataframe(df, "test_table")
    print("insertion complete...")

