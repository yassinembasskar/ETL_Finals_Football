CREATE OR REPLACE TRIGGER trg_matches_last_modified
BEFORE UPDATE ON matches
FOR EACH ROW
BEGIN
    :NEW.last_modified := SYSDATE;
END;
/

