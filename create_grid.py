from sqlalchemy import create_engine, text
import os

database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, client_encoding='latin-1')

LAT_MIN = -23.0
LON_MIN = -44.0
CELL_SIZE = 0.0001 # Approximately 10 meters

create_table_query = f"""
CREATE TABLE IF NOT EXISTS bus_positions_grid AS
WITH params AS (
    SELECT
        linha,
        ST_XMin(ST_Extent(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326))) AS xmin,
        ST_YMin(ST_Extent(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326))) AS ymin,
        ST_XMax(ST_Extent(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326))) AS xmax,
        ST_YMax(ST_Extent(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326))) AS ymax,
        {CELL_SIZE} AS cell_size
    FROM bus_positions_filtered

    GROUP BY linha
),
grid AS (
    SELECT
        p.linha,
        row_number() OVER (PARTITION BY p.linha ORDER BY j, i) AS id,
        ST_SetSRID(ST_MakeEnvelope(
            p.xmin + i * p.cell_size,
            p.ymin + j * p.cell_size,
            p.xmin + (i + 1) * p.cell_size,
            p.ymin + (j + 1) * p.cell_size,
            4326
        ), 4326) AS geom
    FROM params p,
    generate_series(0, floor((p.xmax - p.xmin) / p.cell_size)::int) AS i,
    generate_series(0, floor((p.ymax - p.ymin) / p.cell_size)::int) AS j
)
SELECT linha, geom, id FROM grid;

"""


# Executar as consultas
with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()
