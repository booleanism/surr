PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS subscriptions (
  id BLOB PRIMARY KEY DEFAULT (randomblob(16)),
  user_id BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS subscribed_nodes (
  id BLOB PRIMARY KEY DEFAULT (randomblob(16)),
  subscription_id BLOB NOT NULL,
  node_id BLOB UNIQUE NOT NULL,

  CONSTRAINT fk_subscription FOREIGN KEY (subscription_id)
    REFERENCES subscriptions(id) ON DELETE CASCADE,
  
  CONSTRAINT unique_node_per_subscription UNIQUE (subscription_id, node_id)
);
