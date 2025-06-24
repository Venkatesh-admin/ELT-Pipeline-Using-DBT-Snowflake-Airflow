{{
    config(materialized="table")
}}

WITH RAW_RATINGS AS
(
    SELECT * FROM MOVIELENS.RAW.RAW_RATINGS
)

SELECT 
movieID as movie_id,
rating,
TO_TIMESTAMP_LTZ(timestamp) AS rating_timestamp,
userID as user_id
from RAW_RATINGS