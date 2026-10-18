// Creates the two least-privilege Mongo users the stack runs on.
//
// The nodes write timeseries documents; the analytics service reads them back.
// Neither needs to do the other's job, and neither needs root: the root account
// exists for administration (and for the smoke check, which counts documents
// from outside the stack). Before this script both services connected as root,
// which meant a compromised node held read access to every node's data.
//
// Run by the `mongo-init` one-shot service on every `up`, not by mongo's own
// /docker-entrypoint-initdb.d hooks: those only fire when the data directory is
// empty, so an existing `mongo-data` volume would never get the users and the
// stack would come up half-broken after an upgrade.
//
// Idempotent on purpose. updateUser rather than createUser when the account is
// already there, so editing a password in .env takes effect on the next `up`
// instead of silently keeping the old one.

const dbName = process.env.MONGO_DB || "nodes";

const users = [
  {
    user: process.env.MONGO_NODES_USER || "surr-writer",
    pwd: process.env.MONGO_NODES_PASSWORD || "surr",
    // readWrite is what createCollection needs, which is how a node creates
    // its own timeseries collection on first boot.
    roles: [{ role: "readWrite", db: dbName }],
  },
  {
    user: process.env.MONGO_ANALYTICS_USER || "surr-reader",
    pwd: process.env.MONGO_ANALYTICS_PASSWORD || "surr",
    roles: [{ role: "read", db: dbName }],
  },
];

const target = db.getSiblingDB(dbName);

for (const u of users) {
  if (target.getUser(u.user) === null) {
    target.createUser(u);
    print(`created ${u.user} on ${dbName}`);
    continue;
  }

  target.updateUser(u.user, { pwd: u.pwd, roles: u.roles });
  print(`updated ${u.user} on ${dbName}`);
}
