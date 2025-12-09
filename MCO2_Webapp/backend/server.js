// server2.js - Fragment Node (Server2)

const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({ origin: "*", methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"] }));
app.use(express.json());

const mysql = require("mysql2/promise");

// =============================
// DB CONNECTIONS
// =============================
const centralDB = mysql.createPool({
  host: "10.2.14.81",
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_basics"
});

const otherDB = mysql.createPool({
  host: "10.2.14.82",
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_f1"
});

const localDB = mysql.createPool({
  host: "10.2.14.83",
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_f2"
});

const tableCentral = "dim_title";
const tableLocal = "dim_title_f2";
const tableOther = "dim_title_f1";

// =============================
// FAILOVER READ
// =============================
async function queryFailover(sql, params = []) {
  try {
    return await centralDB.query(sql, params);
  } catch (e1) {
    console.warn("CENTRAL DOWN → trying OTHER", e1.message);
    try {
      return await otherDB.query(sql, params);
    } catch (e2) {
      console.warn("OTHER DOWN → trying LOCAL", e2.message);
      return await localDB.query(sql, params);
    }
  }
}

// =============================
// REPLICATION + RECOVERY
// =============================
let recoveryQueue = [];
const colList = `(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)`;

async function replicateInsertOrUpdate(movie) {
  const values = [
    movie.tconst,
    movie.titleType,
    movie.primaryTitle,
    movie.originalTitle,
    movie.isAdult,
    movie.startYear,
    movie.endYear,
    movie.runtimeMinutes,
    movie.genres
  ];

  // WRITE TO CENTRAL
  try {
    await centralDB.query(
      `REPLACE INTO ${tableCentral} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      values
    );
  } catch (err) {
    recoveryQueue.push({
      sql: `REPLACE INTO ${tableCentral} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      values
    });
  }

  // WRITE TO LOCAL (only > 2010)
  try {
    if (movie.startYear > 2010) {
      await localDB.query(
        `REPLACE INTO ${tableLocal} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        values
      );
    }
  } catch (err) {
    console.log("LOCAL write failed:", err.message);
  }
}

// RECOVERY
setInterval(async () => {
  if (recoveryQueue.length === 0) return;
  const pending = [...recoveryQueue];
  let success = [];

  for (const task of pending) {
    try {
      await centralDB.query(task.sql, task.values);
      success.push(task);
    } catch {
      break;
    }
  }

  recoveryQueue = recoveryQueue.filter(q => !success.includes(q));
}, 5000);

// =============================
// API ROUTES
// =============================
app.get("/api/health", (req, res) => res.json({ status: "OK" }));

app.get("/api/movies", async (req, res) => {
  const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
  res.json(rows);
});

app.get("/api/movies/search", async (req, res) => {
  const term = "%" + req.query.q + "%";
  const [rows] = await queryFailover(
    `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE ? LIMIT 200`,
    [term]
  );
  res.json(rows);
});

app.post("/api/movies", async (req, res) => {
  await replicateInsertOrUpdate(req.body);
  res.json({ message: "Movie added" });
});

app.put("/api/movies/:tconst", async (req, res) => {
  req.body.tconst = req.params.tconst;
  await replicateInsertOrUpdate(req.body);
  res.json({ message: "Movie updated" });
});

// =============================
// START SERVER
// =============================
app.listen(3000, () => console.log("FRAGMENT Server2 running on port 3000"));
