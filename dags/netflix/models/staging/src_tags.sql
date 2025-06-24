
{{
    config(materialized="table")
}}
WITH RAW_TAGS AS
(
    SELECT * FROM MOVIELENS.RAW.RAW_TAGS
)

SELECT 
movieID as movie_id,
TAG,
timestamp as tag_timestamp,
userID as user_id
from RAW_TAGS