from sqlalchemy import create_engine, text
import os

database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, client_encoding='latin-1')


create_table_query = f"""
CREATE MATERIALIZED VIEW vw_buses_order AS
    WITH filtered_data AS (
        SELECT 
            dense_rank() OVER (PARTITION BY ordem, linha ORDER BY datahoraservidor) AS time_ranking,
            ordem,
            linha,
            geom,    
            width_bucket(longitude, -43.726090, -42.951470, 1587) AS x,
            width_bucket(latitude, -23.170790, -22.546410, 1389) AS y,
            velocidade,
            datahoraservidor
        FROM 
            public.bus_positions_filtered_materialize
        WHERE longitude <= -42.951470
            AND longitude >= -43.726090
            AND latitude <= -22.546410
            AND latitude >= -23.170790
    ),
    counts AS (
        SELECT 
            x, 
            y, 
            COUNT(*) AS cnt
        FROM filtered_data
        GROUP BY x, y
    )
SELECT 
    fd.time_ranking,
    fd.ordem,
    fd.linha,
    fd.geom,
    fd.x,
    fd.y,
    fd.velocidade,
    fd.datahoraservidor
FROM 
    filtered_data fd
JOIN 
    counts c ON fd.x = c.x AND fd.y = c.y
WHERE 
    c.cnt >= 3;

"""


# Executar as consultas
with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()
