import psycopg2
import pandas as pd


def load_data_from_postgres(database: object, user: object, password: object, host: object, port: object, table: object) -> object:
    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    data = cur.fetchall()

    cur.close()
    conn.close()
    col_names = [desc[0] for desc in cur.description]

    df = pd.DataFrame(data, columns=col_names)
    return df