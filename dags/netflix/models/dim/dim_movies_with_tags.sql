{{
    config(materialized="ephemeral")
}}

WITH movies AS
(
    SELECT * FROM {{ref('dim_movies')}}
),
tags AS
(
     SELECT * FROM {{ref('dim_genome_tags')}}
),
scores AS
(
    SELECT * FROM {{ref('fct_genome_scores')}}
)

SELECT 
m.movie_id,
m.movie_title,
m.genres,
t.tag_id,
s.relevance_score
FROM movies m 
LEFT JOIN scores s on  m.movie_id= s.movie_id
LEFT JOIN tags t on t.tag_id = s.tag_id

