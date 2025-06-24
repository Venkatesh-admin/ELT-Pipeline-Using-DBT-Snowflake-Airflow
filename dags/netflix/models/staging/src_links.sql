WITH RAW_LINKS AS
(
    SELECT * FROM MOVIELENS.RAW.RAW_LINKS
)

SELECT 
movieID AS movie_id,
imdbID AS imdb_id,
tmdbID AS tmdb_id
from RAW_LINKS
