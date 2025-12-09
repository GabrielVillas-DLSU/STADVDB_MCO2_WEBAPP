// server.js - SERVER 1

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
  host: "10.2.14.81", // SAME CENTRAL
  port: 3306,
  user: "root",
  password: "",
  database: "imdb_title_basics",
  waitForConnections: true,
  connectionLimit: 10
});

const f1DB = mysql.createPool({
  host: "10.2.14.82", // THIS IS SERVER1
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
// Helper: failover read
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
// Replication (same as server0)
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

  // CENTRAL write
  try {
    await centralDB.query(centralReplaceSql, values);
    console.log("Central write OK");
  } catch (err) {
    console.log("CENTRAL DOWN – queued:", movie.tconst);
    recoveryQueue.push({ sql: centralReplaceSql, values });
  }

  // FRAGMENT write
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
// Routes (copied exactly from server0)
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
  const [rows] = await queryFailover(
    `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE ? LIMIT 200`,
    [term]
  );
  res.json(rows);
});

app.post("/api/movies", async (req, res) => {
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
});

app.put("/api/movies/:tconst", async (req, res) => {
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
});

app.delete("/api/movies/:tconst", async (req, res) => {
  const id = req.params.tconst;
  await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst = ?`, [id]);
  await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst = ?`, [id]);
  await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst = ?`, [id]);
  res.json({ message: "Movie deleted across nodes" });
});

// -----------------------------
// Recovery timer
// -----------------------------
setInterval(async () => {
  if (recoveryQueue.length === 0) return;
  console.log("Recovery attempt…");
  const pending = [...recoveryQueue];
  for (const task of pending) {
    try {
      await centralDB.query(task.sql, task.values);
      recoveryQueue = recoveryQueue.filter(q => q !== task);
      console.log("Recovered:", task.values[0]);
    } catch (err) {
      break;
    }
  }
}, 5000);

// -----------------------------
app.listen(3000, () =>
  console.log("SERVER1 backend running on port 3000")
);
