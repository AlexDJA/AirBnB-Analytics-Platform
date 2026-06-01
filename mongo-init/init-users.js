// ============================================================
// MongoDB initialization script — runs ONCE on first container boot.
// (Files in /docker-entrypoint-initdb.d are executed only when the
//  data directory is empty, i.e. on first startup.)
//
// What this script does:
//   1. Creates an application user "spark_writer" on the airbnb DB
//      with readWrite — used by spark_pipeline.py and any future
//      ingestion job.
//   2. Creates a STRICTLY READ-ONLY user "agent_readonly" on the
//      airbnb DB with the built-in `read` role — used by the M3
//      agent. Any attempt to insert/update/delete from this user
//      will be rejected by MongoDB itself at the driver level,
//      with an `OperationFailure: not authorized ...` error.
//
// The root user (admin / lab1pass) is created automatically by the
// mongo:7 image from MONGO_INITDB_ROOT_USERNAME / _PASSWORD in .env.
// We don't recreate it here.
// ============================================================

print("[init] Setting up application users on the 'airbnb' database...");

// Switch to the target database
db = db.getSiblingDB("airbnb");

// ── Application user (read/write) — used by Spark ─────────────
db.createUser({
  user: "spark_writer",
  pwd: "sparkpass",
  roles: [{ role: "readWrite", db: "airbnb" }],
});
print("[init] Created user 'spark_writer' with readWrite on airbnb.");

// ── Agent user (read-only) — used by the M3 agent ─────────────
db.createUser({
  user: "agent_readonly",
  pwd: "agentpass",
  roles: [{ role: "read", db: "airbnb" }],
});
print("[init] Created user 'agent_readonly' with READ-ONLY on airbnb.");

print("[init] Done. Users summary:");
db.getUsers().users.forEach(u => print(`  - ${u.user} : ${JSON.stringify(u.roles)}`));