BEGIN;

CREATE TABLE IF NOT EXISTS stocktwits_posts
(
    id serial NOT NULL,
    symbol varchar(255) NOT NULL,
    post_id bigint NOT NULL,
    post_author varchar(255) NOT NULL,
    post_date timestamp NOT NULL,
    post_text text NOT NULL,
    post_likes bigint,
    post_comments bigint,
    post_reshares bigint,
    post_img_path text,
    sentiment varchar(255),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS symbols
(
    id serial NOT NULL,
    symbol varchar(255) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS execution_logs
(
    id serial NOT NULL,
    log text NOT NULL,
    created_at TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stocktwits_authors
(
    id serial NOT NULL,
    author text NOT NULL,
    total_followers bigint,
    total_following bigint,
    avg_likes bigint,
    avg_comments bigint,
    avg_reshares bigint,
    updated_at TIMESTAMP DEFAULT current_timestamp,
    execution_counter bigint DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS stocktwits_posts_post_id_index ON stocktwits_posts(post_id);
CREATE INDEX IF NOT EXISTS stocktwits_posts_symbol_index ON stocktwits_posts(symbol);
ALTER TABLE public.stocktwits_posts ADD CONSTRAINT stocktwits_posts_post_id_symbol_key UNIQUE (post_id, symbol);

END;




