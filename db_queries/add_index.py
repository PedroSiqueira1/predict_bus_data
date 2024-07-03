from sqlalchemy import create_engine
from sqlalchemy import text
import os

database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, client_encoding='latin-1')  

create_index_sql = """
CREATE INDEX idx_vw_buses_order_linha ON vw_buses_order(linha);

CREATE INDEX idx_vw_buses_order_ordem ON vw_buses_order(ordem);

CREATE INDEX idx_vw_buses_order_x_y ON vw_buses_order(x, y);

CREATE INDEX idx_vw_buses_order_ordem_linha ON vw_buses_order(ordem, linha);

CREATE INDEX idx_vw_buses_order_datahoraservidor ON vw_buses_order (datahoraservidor);

CREATE INDEX idx_vw_buses_order_time_ranking ON vw_buses_order(time_ranking);

"""

with engine.connect() as conn:
    conn.execute(text(create_index_sql))
    conn.commit()