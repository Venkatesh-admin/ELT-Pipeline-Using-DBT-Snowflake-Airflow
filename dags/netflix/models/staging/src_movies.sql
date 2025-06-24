
WITH RAW_MOVIES AS
(
    SELECT * FROM MOVIELENS.RAW.RAW_MOVIES
)

SELECT 
movieID as movie_id,
title,genres
from raw_movies