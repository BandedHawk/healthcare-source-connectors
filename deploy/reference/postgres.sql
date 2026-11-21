-- A pipeline's own database, which only its stages may connect to (README.md here; HIPAA-OPERATIONS.md §2.8).
-- Each stage logs in as its own role and acts as the pipeline's catalog role, which owns the Iceberg JDBC catalog's
-- tables, the leases and the registries: a JDBC catalog keeps every namespace in the same tables, so it can't keep
-- one pipeline from another inside a database (SANDBOX.md, "Not yet as in production"). A database each can. The
-- refusal across databases is tried by file-connector's PipelineIsolationIT.
--
--   psql "host=$POSTGRES_HOST dbname=postgres sslmode=verify-full" -v pipeline=claims -f deploy/reference/postgres.sql
--
-- Then give each login role its password from the secret store, for example with psql's \password, never in a
-- file. The server takes connections over TLS only: in pg_hba.conf, hostssl lines and no host lines (PostgresTlsIT
-- runs with that).
\set ON_ERROR_STOP on
\set database sdh_:pipeline
\set catalog catalog_:pipeline

CREATE ROLE :"catalog" NOLOGIN;
CREATE DATABASE :"database" OWNER :"catalog";
REVOKE ALL ON DATABASE :"database" FROM PUBLIC;

\set landing landing_:pipeline
\set splitter splitter_:pipeline
\set convert convert_:pipeline
\set fhir fhir_:pipeline
CREATE ROLE :"landing" LOGIN IN ROLE :"catalog";
CREATE ROLE :"splitter" LOGIN IN ROLE :"catalog";
CREATE ROLE :"convert" LOGIN IN ROLE :"catalog";
CREATE ROLE :"fhir" LOGIN IN ROLE :"catalog";
GRANT CONNECT ON DATABASE :"database" TO :"landing", :"splitter", :"convert", :"fhir";
ALTER ROLE :"landing" SET role = :'catalog';
ALTER ROLE :"splitter" SET role = :'catalog';
ALTER ROLE :"convert" SET role = :'catalog';
ALTER ROLE :"fhir" SET role = :'catalog';

\connect :"database"
-- Postgres 15 and later let no one but the owner create in public: the catalog role owns the database, so it may.
GRANT ALL ON SCHEMA public TO :"catalog";
