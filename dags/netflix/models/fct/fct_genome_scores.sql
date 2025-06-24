WITH src_genome_scores AS
(
    SELECT * FROM {{ref('src_genome_scores')}}
)
SELECT 
tag_id,
ROUND(relevance,4) AS relevance_score,
movie_id
FROM src_genome_scores
WHERE relevance_score>0
