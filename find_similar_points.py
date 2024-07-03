from sqlalchemy import create_engine
from sqlalchemy import text
import os

database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, client_encoding='latin-1')  

linha = '557'
lat1 = -22.96783
lon1 = -43.33611
date = 1715612389

lat2 = -22.96784
lon2 = -43.33611

find_similar_points = f"""

WITH initial_similar_points AS (
        SELECT time_ranking,
               ordem,
               linha,
               x,
               y,
               datahoraservidor
        FROM vw_buses_order
        WHERE linha = :linha
        AND x = width_bucket(:lon1, -43.726090, -42.951470, 1587)
        AND y = width_bucket(:lat1, -23.170790, -22.546410, 1389)
        AND (
                (datahoraservidor >= TO_TIMESTAMP(:last_date) - interval '7 day' - interval '2 hour'  
                AND datahoraservidor < TO_TIMESTAMP(:last_date) - interval '7 day' + interval '2 hour') 
                OR 
                (datahoraservidor >= TO_TIMESTAMP(:last_date) - interval '14 day' - interval '2 hour'  
                AND datahoraservidor < TO_TIMESTAMP(:last_date) - interval '14 day' + interval '2 hour')
                OR 
                (datahoraservidor >= TO_TIMESTAMP(:last_date) - interval '21 day' - interval '2 hour'  
                AND datahoraservidor < TO_TIMESTAMP(:last_date) - interval '21 day' + interval '2 hour')
            )
        AND time_ranking > 1
        LIMIT 10
    ), anterior_points AS (
        SELECT DISTINCT ON (time_ranking, ordem, linha) 
            time_ranking,
            ordem,
            linha,
            x,
            y,
            datahoraservidor
        FROM vw_buses_order
        WHERE (ordem, linha, time_ranking) IN (
            SELECT ordem, linha, time_ranking - 1
            FROM initial_similar_points
            )
    ), direction_points AS (
         SELECT 
            sp.ordem,
            sp.time_ranking,
            sp.datahoraservidor
        FROM initial_similar_points sp
        INNER JOIN anterior_points ap
            ON sp.ordem = ap.ordem
            AND sp.linha = ap.linha
            AND sp.time_ranking = ap.time_ranking + 1
        WHERE ((ap.x - sp.x) * (:lon2 - :lon1) + (ap.y - sp.y) * (:lat2 - :lat1)) >= 0
    ), first_future_points AS (
        SELECT DISTINCT ON (vo.ordem, vo.time_ranking)
            vo.x,
            vo.y,
            vo.ordem,
            vo.datahoraservidor              
        FROM (
                SELECT time_ranking,
                          ordem,
                          linha,
                          x,
                          y,
                          datahoraservidor
                FROM vw_buses_order
                WHERE linha = :linha
                AND ordem IN (SELECT DISTINCT ordem FROM direction_points)
             ) vo
        INNER JOIN direction_points dp
            ON vo.ordem = dp.ordem
            AND vo.datahoraservidor > dp.datahoraservidor
            AND vo.datahoraservidor < dp.datahoraservidor + interval '1 hour'
        WHERE vo.datahoraservidor > dp.datahoraservidor + interval '25 minutes'
        AND vo.datahoraservidor < dp.datahoraservidor + interval '35 minutes'
    ), selected_future_points AS (
        SELECT x,y
        FROM first_future_points
    )
    SELECT *
        
    FROM first_future_points;
"""

params = {
    'linha': linha,
    'lat1': lat1,
    'lon1': lon1,
    'lat2': lat2,
    'lon2': lon2,
    'last_date': date,
    
}

with engine.connect() as connection:
    result = connection.execute(text(find_similar_points), params)
    for row in result:
        print(row)

