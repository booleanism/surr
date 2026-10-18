-- Fleet declarations: a query that reads across every node of a type.
--
-- Two columns, not a second table. A fleet query is a declared query with a
-- different scope, so it wants the same name, pipeline, params schema and
-- validation -- the only differences are who may read it and which collections
-- the executor unions. A second table would duplicate that machinery and let the
-- two drift.
--
-- `node_type` is sent by the node. It cannot be derived from `collection`: that
-- name is the node id with its dashes stripped and carries no type at all.
--
-- `scope` is TEXT with 'node' as the default so existing rows keep their meaning
-- without a backfill: every row written before this migration was node-scoped.
--
-- The index is what makes the fleet lookup a key range rather than a scan. Fleet
-- rows are keyed by (node_type, scope) and there is exactly one fleet manifest
-- per type -- the last node of that type to declare replaces it whole, so a
-- stale fleet query cannot outlive the nodes that declared it.
ALTER TABLE declarations ADD COLUMN node_type TEXT NOT NULL DEFAULT '';
ALTER TABLE declarations ADD COLUMN scope TEXT NOT NULL DEFAULT 'node';

CREATE INDEX IF NOT EXISTS declarations_fleet ON declarations (node_type, scope);
