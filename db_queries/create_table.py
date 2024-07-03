from sqlalchemy import create_engine
from sqlalchemy import text
import os

database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, client_encoding='latin-1')  

# Create the table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS bus_positions (
    ordem VARCHAR(20),
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    datahora BIGINT,
    velocidade SMALLINT,
    linha VARCHAR(10),
    datahoraenvio BIGINT,
    datahoraservidor BIGINT,
    datahora_ts TIMESTAMP,
    datahoraenvio_ts TIMESTAMP,
    datahoraservidor_ts TIMESTAMP
);
"""
with engine.connect() as conn:
    conn.execute(text(create_table_query))

