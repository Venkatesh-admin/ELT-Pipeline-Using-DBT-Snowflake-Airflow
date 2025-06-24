
-- copy movies.csv
CREATE OR REPLACE table raw_movies
(
movieId INTEGER,
title STRING,
genres STRING
);

COPY INTO raw_movies
FROM '@netflix_stage/movies.csv';

SELECT * FROM raw_movies;


-- copy ratings.csv
CREATE OR REPLACE table raw_ratings
(
userId INTEGER ,
movieId INTEGER ,
rating FLOAT,timestamp BIGINT
);
COPY INTO raw_ratings
FROM '@netflix_stage/ratings.csv';

SELECT * FROM raw_ratings;


-- copy tags.csv
CREATE OR REPLACE table raw_tags
(
userId INTEGER ,
movieId INTEGER ,
tag STRING;
timestamp BIGINT
);
COPY INTO raw_tags
FROM '@netflix_stage/tags.csv'
ON_ERROR = CONTINUE;

SELECT * FROM raw_tags;


--copy links
CREATE OR REPLACE table raw_links
(
movieId INTEGER,
imdbId INTEGER,
tmdbId INTEGER
);
COPY INTO raw_links
FROM '@netflix_stage/links.csv';

SELECT * FROM raw_links;


-- copy genome score
CREATE OR REPLACE table raw_genome_scores
(
movieId INTEGER,
tagId INTEGER,
relevance FLOAT
);
COPY INTO raw_genome_scores
FROM '@netflix_stage/genome-scores.csv';

SELECT * FROM raw_genome_scores;

-- create genome tags

CREATE OR REPLACE table raw_genome_tags
(
tagId INTEGER,
tag STRING
);
COPY INTO raw_genome_tags
FROM '@netflix_stage/genome-tags.csv';

SELECT * FROM raw_genome_tags;


