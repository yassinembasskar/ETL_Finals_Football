-- Setting up the session
ALTER SESSION SET CONTAINER = XEPDB1;

-- Creating a user
CREATE USER etl_app
IDENTIFIED BY etladmin
DEFAULT TABLESPACE users
TEMPORARY TABLESPACE temp;

-- Granting 
GRANT CREATE SESSION TO etl_app;
GRANT CREATE TABLE TO etl_app;
GRANT CREATE VIEW TO etl_app;
GRANT CREATE SEQUENCE TO etl_app;
GRANT CREATE PROCEDURE TO etl_app;
ALTER USER etl_app QUOTA UNLIMITED ON users;
GRANT CREATE TRIGGER TO etl_app;


-- User Verification
SELECT username, account_status
FROM dba_users
WHERE username = 'ETL_APP';
