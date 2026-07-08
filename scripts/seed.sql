-- Local dev seed data. Creates one active run and a handful of dummy
-- pending jobs across a few categories, for exercising the claim/process/
-- ack path against docker-compose Postgres.
--
-- Usage:
--   docker exec -i <postgres-container> psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < scripts/seed.sql

BEGIN;

INSERT INTO runs DEFAULT VALUES RETURNING id AS run_id;

INSERT INTO jobs (category, url, idem_key)
SELECT
  categories.category,
  'https://example.com/' || categories.category || '/item-' || n,
  md5(categories.category || 'https://example.com/' || categories.category || '/item-' || n)
FROM
  (VALUES ('news'), ('blogs'), ('forums')) AS categories(category),
  generate_series(1, 10) AS n;

COMMIT;
