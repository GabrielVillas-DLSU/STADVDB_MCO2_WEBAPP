// =====================
//  SERVER 1 - FRAGMENT NODE (≤ 2010)
// =====================
const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({
    origin: "*",   // allow ALL servers to call each other
    methods: ["GET", "POST", "PUT", "DELETE"]
}));
app.use(express.json());

const mysql = require("mysql2/promise");

// ===============
// DATABASE POOLS
// ===============
const centralDB = mysql.createPool({
    host: "10.2.14.81",
    port: 3306,
    user: "root",
    password: "",
    database: "imdb_title_basics"
});

const localDB = mysql.createPool({
    host: "10.2.14.82",
    port: 3306,
    user: "root",
    password: "",
    database: "imdb_title_f1"
});

const f2DB = mysql.createPool({
    host: "10.2.14.83",
    port: 3306,
    user: "root",
    password: "",
    database: "imdb_title_f2"
});

const tableCentral = "dim_title";
const tableLocal = "dim_title_f1";
const tableF2 = "dim_title_f2";

// =========================
// FAILOVER QUERY FOR READS
// =========================
async function queryFailover(sql) {
    try {
        return await centralDB.query(sql);
    } catch {
        console.log("CENTRAL DOWN → using LOCAL F1");
        try {
            return await localDB.query(sql);
        } catch {
            console.log("F1 DOWN → switching to F2");
            return await f2DB.query(sql);
        }
    }
}

// =========================
// REPLICATION + RECOVERY
// =========================
let recoveryQueue = [];

async function replicate(movie) {
    const values = [
        movie.tconst, movie.titleType, movie.primaryTitle,
        movie.isAdult, movie.startYear, movie.genres
    ];

    const sqlCentral = `
        REPLACE INTO ${tableCentral}
        (tconst, titleType, primaryTitle, isAdult, startYear, genres)
        VALUES (?, ?, ?, ?, ?, ?)
    `;

    // Attempt central write
    try {
        await centralDB.query(sqlCentral, values);
    } catch {
        console.log("CENTRAL DOWN → queueing update");
        recoveryQueue.push(movie);
    }

    // Always write to its own local fragment
    await localDB.query(`
        REPLACE INTO ${tableLocal}
        (tconst, titleType, primaryTitle, isAdult, startYear, genres)
        VALUES (?, ?, ?, ?, ?, ?)
    `, values);

    // If movie belongs to f2
    if (movie.startYear > 2010) {
        await f2DB.query(`
            REPLACE INTO ${tableF2}
            (tconst, titleType, primaryTitle, isAdult, startYear, genres)
            VALUES (?, ?, ?, ?, ?, ?)
        `, values);
    }
}

// Recovery loop
setInterval(async () => {
    if (recoveryQueue.length === 0) return;

    console.log("Attempting CENTRAL recovery...");
    try {
        for (const movie of recoveryQueue) {
            await centralDB.query(
                `REPLACE INTO ${tableCentral}
                (tconst, titleType, primaryTitle, isAdult, startYear, genres)
                VALUES (?, ?, ?, ?, ?, ?)`,
                [
                    movie.tconst, movie.titleType, movie.primaryTitle,
                    movie.isAdult, movie.startYear, movie.genres
                ]
            );
        }
        console.log("Recovery success!");
        recoveryQueue = [];
    } catch {
        console.log("Central still down... retry later.");
    }

}, 5000);

// =========================
// API ROUTES
// =========================

app.get("/api/health", (req, res) => {
    res.json({ status: "OK" });
});

// GET ALL MOVIES
app.get("/api/movies", async (req, res) => {
    const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
});

// SEARCH
app.get("/api/movies/search", async (req, res) => {
    const term = "%" + req.query.q + "%";
    const [rows] = await queryFailover(
        `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE '${term}' LIMIT 200`
    );
    res.json(rows);
});

// ADD MOVIE
app.post("/api/movies", async (req, res) => {
    await replicate(req.body);
    res.json({ message: "Movie added (replicated)" });
});

// UPDATE
app.put("/api/movies/:tconst", async (req, res) => {
    req.body.tconst = req.params.tconst;
    await replicate(req.body);
    res.json({ message: "Movie updated (replicated)" });
});

// DELETE
app.delete("/api/movies/:tconst", async (req, res) => {
    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst=?`, [req.params.tconst]);
    await localDB.query(`DELETE FROM ${tableLocal} WHERE tconst=?`, [req.params.tconst]);
    await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst=?`, [req.params.tconst]);
    res.json({ message: "Movie deleted across all nodes" });
});

// REPORTS
app.get("/api/reports/top-genres", async (req, res) => {
    const [rows] = await queryFailover(`
        SELECT genres, COUNT(*) AS cnt
        FROM ${tableCentral}
        GROUP BY genres
        ORDER BY cnt DESC
        LIMIT 5
    `);
    res.json(rows);
});

app.get("/reports/most-titles-year", async (req, res) => {
    const [rows] = await centralDB.query(`
        SELECT startYear, COUNT(*) AS count
        FROM ${tableCentral}
        WHERE startYear IS NOT NULL
        GROUP BY startYear
        ORDER BY count DESC
        LIMIT 1;
    `);
    res.json(rows[0]);
});


app.get("/reports/adult-count", async (req, res) => {
    const [rows] = await centralDB.query(`
        SELECT 
            SUM(isAdult = 1) AS adultCount,
            SUM(isAdult = 0) AS nonAdultCount
        FROM ${tableCentral};
    `);
    res.json(rows[0]);
});

// START
app.listen(3000, () => console.log("NODE 1 backend running on port 3000"));
