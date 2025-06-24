{{
    config(
        materialized = 'incremental',
        on_schema_changes = 'fail'
    )
}}

WITH src_ratings AS
(
    SELECT * FROM {{ref("src_ratings")}}
)

SELECT 
user_id,
movie_id,
rating,
rating_timestamp
from src_ratings
WHERE rating is not NULL

{% if is_incremental() %}
    AND rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{this}})
{% endif %}