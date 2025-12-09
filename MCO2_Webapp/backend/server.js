// =====================
// SERVER 1 - FRAGMENT NODE (≤ 2010)
// =====================

const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({ origin: "*", methods: ["GET", "POST", "PUT", "DELETE"] }));
app.use(express.json());

const mysql = require("mysql2/promise");

// -----------------------------
// DATABASE POOLS
// -----------------------------
const localDB = mysql.createPool({
  host: "10.2.14.82",
  user: "root",
  password: "",
  database: "imdb_title_f1"
});

const centralDB = mysql.createPool({
  host: "10.2.14.81",
  user: "root",
  password: "",
  database: "imdb_title_basics"
});

const fragment2DB = mysql.createPool({
  host: "10.2.14.83",
  user: "root",
  password: "",
  database: "imdb_title_f2"
});

// -----------------------------
// Failover read: local → central → fragment2
// -----------------------------
async function queryFailover(sql, params = []) {
  try {
    return await localDB.query(sql, params);
  } catch (err1) {
    try {
      return await centralDB.query(sql, params);
    } catch (err2) {
      return await fragment2DB.query(sql, params);
    }
  }
}

// -----------------------------
// Replication + recovery queue
// -----------------------------
const colList = `(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)`;

let recoveryQueue = [];

async function replicate(movie) {
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

  // 1) Write to LOCAL fragment always
  await localDB.query(`REPLACE INTO dim_title_f1 ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
  console.log("LOCAL write OK (F1)");

  // 2) Try central, queue if failed
  try {
    await centralDB.query(`REPLACE INTO dim_title ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
    console.log("Central write OK");
  } catch (err) {
    console.log("Central DOWN → queueing", movie.tconst);
    recoveryQueue.push(values);
  }
}

// Retry central every 5 seconds
setInterval(async () => {
  if (recoveryQueue.length === 0) return;
  console.log("Retrying CENTRAL…");

  const pending = [...recoveryQueue];
  recoveryQueue = [];

  for (const values of pending) {
    try {
      await centralDB.query(`REPLACE INTO dim_title ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
      console.log("Recovered write OK");
    } catch (err) {
      console.log("Still down:", err.message);
      recoveryQueue.push(values);
      break;
    }
  }
}, 5000);

// -----------------------------
// ROUTES (same API as server0)
// -----------------------------
app.get("/api/health", (req, res) => res.json({ status: "OK" }));

app.get("/api/movies", async (req, res) => {
  const [rows] = await queryFailover(`SELECT * FROM dim_title LIMIT 200`);
  res.json(rows);
});

app.get("/api/movies/search", async (req, res) => {
  const term = "%" + req.query.q + "%";
  const [rows] = await queryFailover(`SELECT * FROM dim_title WHERE primaryTitle LIKE ? LIMIT 200`, [term]);
  res.json(rows);
});

app.post("/api/movies", async (req, res) => {
  await replicate(req.body);
  res.json({ message: "Movie added + replicated (F1)" });
});

app.put("/api/movies/:tconst", async (req, res) => {
  req.body.tconst = req.params.tconst;
  await replicate(req.body);
  res.json({ message: "Movie updated + replicated (F1)" });
});

app.delete("/api/movies/:tconst", async (req, res) => {
  const id = req.params.tconst;

  await localDB.query(`DELETE FROM dim_title_f1 WHERE tconst=?`, [id]);

  try { await centralDB.query(`DELETE FROM dim_title WHERE tconst=?`, [id]); } catch {}
  res.json({ message: "Movie deleted (best effort)" });
});

// Reports always from central when possible
app.get("/api/reports/top-genres", async (req, res) => {
  const [rows] = await centralDB.query(`
      SELECT genres, COUNT(*) AS count
      FROM dim_title
      GROUP BY genres
      ORDER BY count DESC LIMIT 5`);
  res.json(rows);
});

app.get("/api/reports/most-titles-year", async (req, res) => {
  const [rows] = await centralDB.query(`
      SELECT startYear, COUNT(*) AS count
      FROM dim_title
      GROUP BY startYear
      ORDER BY count DESC LIMIT 1`);
  res.json(rows[0] || {});
});

app.get("/api/reports/adult-count", async (req, res) => {
  const [rows] = await centralDB.query(`
      SELECT SUM(isAdult=1) AS adultCount, SUM(isAdult=0) AS nonAdultCount
      FROM dim_title`);
  res.json(rows[0]);
});

// Start server
app.listen(3000, () => console.log("Server1 (Fragment ≤ 2010) running on port 3000"));
