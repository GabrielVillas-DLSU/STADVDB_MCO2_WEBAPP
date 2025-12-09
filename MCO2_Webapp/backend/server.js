// server2.js - SERVER 2 (same logic as Server0)

const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({ origin: "*", methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"] }));
app.use(express.json());

const mysql = require("mysql2/promise");

// -----------------------------
// DATABASE POOLS
// -----------------------------
const centralDB = mysql.createPool({
  host: "10.2.14.81",   // CENTRAL NODE
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_basics",
  waitForConnections: true,
  connectionLimit: 10
});

const f1DB = mysql.createPool({
  host: "10.2.14.82",   // FRAGMENT 1 (Server1)
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_f1",
  waitForConnections: true,
  connectionLimit: 5
});

const f2DB = mysql.createPool({
  host: "10.2.14.83",   // FRAGMENT 2 (THIS SERVER)
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
// FAILOVER READ
// -----------------------------
async function queryFailover(sql, params = []) {
  try {
    return await centralDB.query(sql, params);
  } catch (e1) {
    console.warn("CENTRAL DOWN → trying F1");
    try {
      return await f1DB.query(sql, params);
    } catch (e2) {
      console.warn("F1 DOWN → trying F2");
      return await f2DB.query(sql, params);
    }
  }
}

// -----------------------------
// REPLICATION + RECOVERY
// -----------------------------
const colList = `(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)`;
const centralReplaceSql = `REPLACE INTO ${tableCentral} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

let recoveryQueue = [];

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
    await centralDB.query(centralReplaceSql, values);
    console.log("Central write OK");
  } catch (err) {
    console.log("CENTRAL DOWN – queued:", movie.tconst);
    recoveryQueue.push({
      sql: centralReplaceSql,
      values
    });
  }

  // WRITE TO FRAGMENT
  try {
    if (movie.startYear <= 2010) {
      await f1DB.query(
        `REPLACE INTO ${tableF1} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        values
      );
      console.log("Fragment F1 write OK");
    } else {
      await f2DB.query(
        `REPLACE INTO ${tableF2} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        values
      );
      console.log("Fragment F2 write OK");
    }
  } catch (err) {
    console.log("❌ Fragment write failed:", err.message);
  }
}

// -----------------------------
// API ROUTES
// -----------------------------
app.get("/api/health", (req, res) => res.json({ status: "OK" }));

app.get("/api/movies", async (req, res) => {
  try {
    const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: "Unable to fetch movies." });
  }
});

app.get("/api/movies/search", async (req, res) => {
  const term = "%" + req.query.q + "%";

  try {
    const [rows] = await queryFailover(
      `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE ? LIMIT 200`,
      [term]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: "Search failed" });
  }
});

app.post("/api/movies", async (req, res) => {
  try {
    const { tconst, type, title, year, isAdult, genre } = req.body;

    const movie = {
      tconst,
      titleType: type,
      primaryTitle: title,
      originalTitle: title,
      isAdult: isAdult === "yes" ? 1 : 0,
      startYear: parseInt(year),
      endYear: null,
      runtimeMinutes: 0,
      genres: genre
    };

    await replicateInsertOrUpdate(movie);
    res.json({ message: "Movie added (replicated)", movie });
  } catch (err) {
    res.status(500).json({ error: "Add failed", detail: err.message });
  }
});

app.put("/api/movies/:tconst", async (req, res) => {
  try {
    const tconst = req.params.tconst;
    const { type, title, year, isAdult, genre } = req.body;

    const movie = {
      tconst,
      titleType: type,
      primaryTitle: title,
      originalTitle: title,
      isAdult: isAdult === "yes" ? 1 : 0,
      startYear: parseInt(year),
      endYear: null,
      runtimeMinutes: 0,
      genres: genre
    };

    await replicateInsertOrUpdate(movie);
    res.json({ message: "Movie updated (replicated)", movie });
  } catch (err) {
    res.status(500).json({ error: "Update failed", detail: err.message });
  }
});

app.delete("/api/movies/:tconst", async (req, res) => {
  const id = req.params.tconst;

  try {
    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst = ?`, [id]);
  } catch (err) {
    console.warn("Central delete failed:", err.message);
  }

  try {
    await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst = ?`, [id]);
  } catch (err) {
    console.warn("F1 delete failed:", err.message);
  }

  try {
    await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst = ?`, [id]);
  } catch (err) {
    console.warn("F2 delete failed:", err.message);
  }

  res.json({ message: "Movie delete attempt finished" });
});

// -----------------------------
// RECOVERY QUEUE
// -----------------------------
setInterval(async () => {
  if (recoveryQueue.length === 0) return;

  console.log("Attempting CENTRAL recovery… pending:", recoveryQueue.length);

  const pending = [...recoveryQueue];
  let recovered = [];

  for (const task of pending) {
    try {
      await centralDB.query(task.sql, task.values);
      console.log("Recovered:", task.values[0]);
      recovered.push(task);
    } catch (err) {
      console.log("Still down:", err.message);
      break;
    }
  }

  recoveryQueue = recoveryQueue.filter(q => !recovered.includes(q));

}, 5000);

// -----------------------------
const PORT = 3000;
app.listen(PORT, () => console.log(`SERVER 2 backend running on port ${PORT}`));

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});
