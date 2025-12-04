// =====================
//  SERVER 0 - CENTRAL NODE
// =====================
const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({ origin: "*", methods: ["GET", "POST", "PUT", "DELETE"] }));
app.use(express.json());

const mysql = require("mysql2/promise");

// =============================
// DATABASE CONNECTIONS
// =============================
const centralDB = mysql.createPool({
    host: "10.2.14.81",
    port: 3306,
    user: "root",
    password: "",
    database: "imdb_title_basics",
});

const f1DB = mysql.createPool({
    host: "10.2.14.82",
    port: 3306,
    user: "root",
    password: "",
    database: "imdb_title_f1",
});

const f2DB = mysql.createPool({
    host: "10.2.14.83",
    port: 3306,
    user: "root",
    password: "",
    database: "imdb_title_f2",
});

const tableCentral = "dim_title";
const tableF1 = "dim_title_f1";
const tableF2 = "dim_title_f2";

// =====================================================
// FAILOVER QUERY FOR READ OPERATIONS
// =====================================================
async function queryFailover(sql) {
    try {
        return await centralDB.query(sql);
    } catch (e1) {
        console.log("CENTRAL DOWN → switching to F1");
        try {
            return await f1DB.query(sql);
        } catch (e2) {
            console.log("F1 DOWN → switching to F2");
            return await f2DB.query(sql);
        }
    }
}

// =====================================================
// WRITE REPLICATION + RECOVERY QUEUE
// =====================================================
let recoveryQueue = [];

async function replicate(movie) {
    const values = [
        movie.tconst,
        movie.titleType,
        movie.primaryTitle,
        movie.originalTitle || null,
        movie.isAdult,
        movie.startYear,
        movie.endYear || null,
        movie.runtimeMinutes || null,
        movie.genres,
    ];

    const colList = `(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)`;

    const sql = `REPLACE INTO ${tableCentral} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    // Try to write to CENTRAL
    try {
        await centralDB.query(sql, values);
    } catch (err) {
        console.log("CENTRAL DOWN → Queueing for recovery");
        recoveryQueue.push({ sql, values });
    }

    // Insert into fragment
    const fragmentTable = movie.startYear <= 2010 ? tableF1 : tableF2;
    const fragDB = movie.startYear <= 2010 ? f1DB : f2DB;

    await fragDB.query(
        `REPLACE INTO ${fragmentTable} ${colList} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        values
    );
}

// Automatic CENTRAL recovery
setInterval(async () => {
    if (recoveryQueue.length === 0) return;

    console.log("Attempting CENTRAL recovery…");

    try {
        for (const task of recoveryQueue) {
            await centralDB.query(task.sql, task.values);
        }
        console.log("CENTRAL RECOVERY SUCCESS");
        recoveryQueue = [];
    } catch {
        console.log("CENTRAL still down… retrying later");
    }
}, 5000);

// =============================
// API ROUTES
// =============================

app.get("/api/health", (req, res) => res.json({ status: "OK" }));

// GET ALL MOVIES
app.get("/api/movies", async (req, res) => {
    const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
});

// SEARCH
app.get("/api/movies/search", async (req, res) => {
    const term = "%" + req.query.q + "%";
    const [rows] = await queryFailover(
        `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE ? LIMIT 200`,
        [term]
    );
    res.json(rows);
});

// ADD MOVIE
app.post("/api/movies", async (req, res) => {
    await replicate(req.body);
    res.json({ message: "Movie added and replicated" });
});

// UPDATE MOVIE
app.put("/api/movies/:tconst", async (req, res) => {
    req.body.tconst = req.params.tconst;
    await replicate(req.body);
    res.json({ message: "Movie updated and replicated" });
});

// DELETE MOVIE
app.delete("/api/movies/:tconst", async (req, res) => {
    const id = req.params.tconst;

    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst=?`, [id]);
    await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst=?`, [id]);
    await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst=?`, [id]);

    res.json({ message: "Movie deleted across all nodes" });
});

// =============================
// FIXED REPORT ROUTES (/api/...)
// =============================

app.get("/api/reports/top-genres", async (req, res) => {
    const [rows] = await queryFailover(`
        SELECT genres, COUNT(*) AS count
        FROM ${tableCentral}
        WHERE genres IS NOT NULL
        GROUP BY genres
        ORDER BY count DESC
        LIMIT 5
    `);
    res.json(rows);
});

app.get("/api/reports/most-titles-year", async (req, res) => {
    const [rows] = await centralDB.query(`
        SELECT startYear, COUNT(*) AS count
        FROM ${tableCentral}
        GROUP BY startYear
        ORDER BY count DESC
        LIMIT 1
    `);
    res.json(rows[0]);
});

app.get("/api/reports/adult-count", async (req, res) => {
    const [rows] = await centralDB.query(`
        SELECT 
            SUM(isAdult = 1) AS adultCount,
            SUM(isAdult = 0) AS nonAdultCount
        FROM ${tableCentral}
    `);
    res.json(rows[0]);
});

// =============================
// START SERVER
// =============================
app.listen(3000, () => console.log("CENTRAL backend running on port 3000"));
