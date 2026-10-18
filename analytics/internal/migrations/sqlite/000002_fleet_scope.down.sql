DROP INDEX IF EXISTS declarations_fleet;

ALTER TABLE declarations DROP COLUMN scope;
ALTER TABLE declarations DROP COLUMN node_type;
