-- One row per declared query, not one row per node.
--
-- A node's manifest is a set of named queries, and every read the service
-- performs is keyed by (node, query name), so making that pair the primary key
-- turns each lookup into a primary-key hit instead of a read of the whole
-- manifest followed by a scan of it. `collection` is per node rather than per
-- query and is therefore repeated on each row: a declare replaces every row for
-- that node anyway, so there is no update path that could let the copies drift.
--
-- `pipeline` and `params_schema` are TEXT holding JSON. SQLite has no JSON
-- storage class; the alternative, normalising the stages into their own table,
-- would mean reconstructing the exact operand order of every stage on read,
-- which is precisely the information that has to survive (see models.Query).
--
-- node_id is BLOB, never TEXT: SQLite compares by storage class, so a TEXT bind
-- against a BLOB column matches zero rows silently. Every bind is id[:].
CREATE TABLE IF NOT EXISTS declarations (
  node_id BLOB NOT NULL,
  name TEXT NOT NULL,
  collection TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  params_schema TEXT NOT NULL DEFAULT '{}',
  pipeline TEXT NOT NULL,
  updated_at INTEGER NOT NULL,

  PRIMARY KEY (node_id, name)
);
