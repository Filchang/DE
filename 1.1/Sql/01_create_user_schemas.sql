DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ds_owner') THEN
        EXECUTE 'DROP SCHEMA IF EXISTS ds CASCADE';
        EXECUTE 'DROP ROLE ds_owner';
    END IF;
END
$$;

CREATE USER ds_owner WITH PASSWORD 'ds';
CREATE SCHEMA ds AUTHORIZATION ds_owner;



DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'logs_owner') THEN
        EXECUTE 'DROP SCHEMA IF EXISTS logs CASCADE';
        EXECUTE 'DROP ROLE logs_owner';
    END IF;
END
$$;
CREATE USER logs_owner WITH PASSWORD 'logs';
DROP SCHEMA IF EXISTS logs CASCADE;
CREATE SCHEMA logs AUTHORIZATION logs_owner;
