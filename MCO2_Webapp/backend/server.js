// server.js - CENTRAL NODE (Server0)
// Fully corrected version

const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({ origin: "*", methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"] }));
app.use(express.json());

const mysql = require("mysql2/promise");

// -----------------------------
// DATABASE POOLS (adjust IPs if needed)
// -----------------------------
const centralDB = mysql.createPool({
  host: "10.2.14.81",
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_basics",
  waitForConnections: true,
  connectionLimit: 10
});

const f1DB = mysql.createPool({
  host: "10.2.14.82",
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_f1",
  waitForConnections: true,
  connectionLimit: 5
});

const f2DB = mysql.createPool({
  host: "10.2.14.83",
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_f2",
  waitForConnections: true,
  connectionLimit: 5
});

const tableCentral = "dim_title";
const tableF1 = "dim_title_f1";
const tableF2 = "dim_title_f2";

// -----------------------------
// Helper: failover read (supports prepared params)
// -----------------------------
async function queryFailover(sql, params = []) {
  try {
    return await centralDB.query(sql, params);
  } catch (e1) {
    console.warn("CENTRAL DOWN → trying F1", e1.message);
    try {
      return await f1DB.query(sql, params);
    } catch (e2) {
      console.warn("F1 DOWN → trying F2", e2.message);
      return await f2DB.query(sql, params);
    }
  }
}

// -----------------------------
// Replication (simple version)
// - Central must succeed (otherwise throw).
// - Fragments are attempted, but failures on fragments are logged (do not block).
// - If central was down, we queue the (sql, params) and try to replay periodically.
// -----------------------------
const colList = `(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)`;
const centralReplaceSql = `REPLACE INTO ${tableCentral} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

// simple in-memory recovery queue for central writes that failed
let recoveryQueue = [];

/**
 * movie object keys expected:
 *  tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
 */
async function replicateInsertOrUpdate(movie) {
  const values = [
    movie.tconst,
    movie.titleType || "movie",
    movie.primaryTitle || "Untitled",
    movie.originalTitle || movie.primaryTitle || "Untitled",
    movie.isAdult ? 1 : 0,
    movie.startYear || new Date().getFullYear(),
    movie.endYear || null,
    movie.runtimeMinutes || 0,
    movie.genres || "Unknown"
  ];


  // 1) Try central first (required)
  try {
    await centralDB.query(centralReplaceSql, values);
  } catch (err) {
    console.error("❌ CENTRAL NODE FAILED — queueing for recovery", err.message);
    // queue the SQL and params for later replay
    recoveryQueue.push({ sql: centralReplaceSql, params: values });
    // throw to inform caller that central is down (so they can get an error)
    throw new Error("Central node unavailable - write queued for recovery.");
  }

  // 2) Write to fragment (best-effort)
  try {
    if (movie.startYear != null && movie.startYear <= 2010) {
      await f1DB.query(`REPLACE INTO ${tableF1} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
    } else {
      await f2DB.query(`REPLACE INTO ${tableF2} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
    }
  } catch (fragErr) {
    console.warn("⚠ Fragment write failed (but central succeeded):", fragErr.message);
    // do not rethrow; central succeeded so operation is considered successful
  }

  return { success: true };
}

// periodic attempt to flush recoveryQueue back to central
setInterval(async () => {
  if (recoveryQueue.length === 0) return;
  console.log("Attempting to replay recovery queue to CENTRAL (length = " + recoveryQueue.length + ")");

  const pending = [...recoveryQueue];
  let allDone = true;

  for (const task of pending) {
    try {
      await centralDB.query(task.sql, task.params);
      // remove this task from the queue
      recoveryQueue = recoveryQueue.filter(q => q !== task);
    } catch (err) {
      allDone = false;
      console.warn("CENTRAL still down or write failed:", err.message);
      break; // break early - central still likely down
    }
  }

  if (allDone) console.log("Recovery queue flushed successfully.");
}, 5000);

// -----------------------------
// API ROUTES (all use /api/...)
// -----------------------------
app.get("/api/health", (req, res) => res.json({ status: "OK" }));

// GET movies (failover read)
app.get("/api/movies", async (req, res) => {
  try {
    const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
  } catch (err) {
    console.error("GET /api/movies error:", err.message);
    res.status(500).json({ error: "Unable to fetch movies." });
  }
});

// SEARCH (prepared)
app.get("/api/movies/search", async (req, res) => {
  try {
    let q = req.query.q;

    let sql;
    let params;

    if (q.startsWith("tt")) {
        // Search by primary key ID
        sql = `SELECT * FROM ${tableCentral} WHERE tconst = ? LIMIT 1`;
        params = [q];
      } else {
        // Search by title
        sql = `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE ? LIMIT 200`;
        params = ["%" + q + "%"];
      }

const [rows] = await queryFailover(sql, params);
res.json(rows);

    res.json(rows);
  } catch (err) {
    console.error("GET /api/movies/search error:", err.message);
    res.status(500).json({ error: "Search failed." });
  }
});

// ADD
app.post("/api/movies", async (req, res) => {
  try {
    await replicateInsertOrUpdate(req.body);
    res.json({ message: "Movie added (replicated)" });
  } catch (err) {
    // central failed and write was queued
    res.status(503).json({ error: err.message });
  }
});

// UPDATE
app.put("/api/movies/:tconst", async (req, res) => {
  try {
    req.body.tconst = req.params.tconst;
    await replicateInsertOrUpdate(req.body);
    res.json({ message: "Movie updated (replicated)" });
  } catch (err) {
    res.status(503).json({ error: err.message });
  }
});

// DELETE (attempt to remove from all nodes; failures logged)
app.delete("/api/movies/:tconst", async (req, res) => {
  const id = req.params.tconst;
  try {
    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst = ?`, [id]);
  } catch (err) {
    console.warn("Delete central failed:", err.message);
    // we continue to attempt fragments but inform user if central failed
    try {
      await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst = ?`, [id]);
    } catch (e) { console.warn("Delete f1 failed:", e.message); }
    try {
      await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst = ?`, [id]);
    } catch (e) { console.warn("Delete f2 failed:", e.message); }
    return res.status(503).json({ error: "Central delete failed." });
  }

  // central succeeded, try fragments (best-effort)
  try {
    await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst = ?`, [id]);
  } catch (e) { console.warn("Delete f1 failed:", e.message); }
  try {
    await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst = ?`, [id]);
  } catch (e) { console.warn("Delete f2 failed:", e.message); }

  res.json({ message: "Movie deleted across nodes (best-effort)" });
});

// REPORTS
app.get("/api/reports/top-genres", async (req, res) => {
  try {
    const [rows] = await queryFailover(`
      SELECT genres, COUNT(*) AS count
      FROM ${tableCentral}
      WHERE genres IS NOT NULL
      GROUP BY genres
      ORDER BY count DESC
      LIMIT 5
    `);
    res.json(rows);
  } catch (err) {
    console.error("GET /api/reports/top-genres error:", err.message);
    res.status(500).json({ error: "Unable to fetch top genres." });
  }
});

app.get("/api/reports/most-titles-year", async (req, res) => {
  try {
    const [rows] = await centralDB.query(`
      SELECT startYear, COUNT(*) AS count
      FROM ${tableCentral}
      WHERE startYear IS NOT NULL
      GROUP BY startYear
      ORDER BY count DESC
      LIMIT 1
    `);
    res.json(rows[0] || {});
  } catch (err) {
    console.error("GET /api/reports/most-titles-year error:", err.message);
    res.status(500).json({ error: "Unable to fetch most titles year." });
  }
});

app.get("/api/reports/adult-count", async (req, res) => {
  try {
    const [rows] = await centralDB.query(`
      SELECT
        SUM(isAdult = 1) AS adultCount,
        SUM(isAdult = 0) AS nonAdultCount
      FROM ${tableCentral}
    `);
    res.json(rows[0] || { adultCount: 0, nonAdultCount: 0 });
  } catch (err) {
    console.error("GET /api/reports/adult-count error:", err.message);
    res.status(500).json({ error: "Unable to fetch adult counts." });
  }
});

// -----------------------------
// Start server
// -----------------------------
const PORT = 3000;
app.listen(PORT, () => console.log(`CENTRAL backend running on port ${PORT}`));

// handle uncaught rejections (logs)
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason && reason.stack ? reason.stack : reason);
});
