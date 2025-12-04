// =====================
//  SERVER 2 - FRAGMENT NODE (> 2010)
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

const f1DB = mysql.createPool({
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
const tableF1 = "dim_title_f1";
const tableLocal = "dim_title_f2";

// =========================
// FAILOVER READ QUERY
// =========================
async function queryFailover(sql) {
    try {
        return await centralDB.query(sql);
    } catch {
        console.log("CENTRAL DOWN → switching to F1");
        try {
            return await f1DB.query(sql);
        } catch {
            console.log("F1 DOWN → using LOCAL F2");
            return await localDB.query(sql);
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

    // Insert to central
    try {
        await centralDB.query(`
            REPLACE INTO ${tableCentral}
            (tconst, titleType, primaryTitle, isAdult, startYear, genres)
            VALUES (?, ?, ?, ?, ?, ?)`, values);
    } catch {
        console.log("CENTRAL DOWN → queueing update for later");
        recoveryQueue.push(movie);
    }

    // Insert to fragment 2 (local)
    await localDB.query(`
        REPLACE INTO ${tableLocal}
        (tconst, titleType, primaryTitle, isAdult, startYear, genres)
        VALUES (?, ?, ?, ?, ?, ?)`, values);

    // If the movie should also exist on fragment 1
    if (movie.startYear <= 2010) {
        await f1DB.query(`
            REPLACE INTO ${tableF1}
            (tconst, titleType, primaryTitle, isAdult, startYear, genres)
            VALUES (?, ?, ?, ?, ?, ?)`, values);
    }
}

// Recovery loop
setInterval(async () => {
    if (recoveryQueue.length === 0) return;
    console.log("Attempting CENTRAL recovery…");

    try {
        for (const movie of recoveryQueue) {
            await centralDB.query(`
                REPLACE INTO ${tableCentral}
                (tconst, titleType, primaryTitle, isAdult, startYear, genres)
                VALUES (?, ?, ?, ?, ?, ?)`, [
                movie.tconst, movie.titleType, movie.primaryTitle,
                movie.isAdult, movie.startYear, movie.genres
            ]);
        }
        console.log("Recovery successful!");
        recoveryQueue = [];
    } catch {
        console.log("Central still down… retrying later.");
    }
}, 5000);

// =========================
// API ROUTES
// =========================

app.get("/api/health", (req, res) => {
    res.json({ status: "OK" });
});

app.get("/api/movies", async (req, res) => {
    const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
});

app.get("/api/movies/search", async (req, res) => {
    const term = "%" + req.query.q + "%";
    const [rows] = await queryFailover(`
        SELECT * FROM ${tableCentral} 
        WHERE primaryTitle LIKE '${term}' LIMIT 200`
    );
    res.json(rows);
});

app.post("/api/movies", async (req, res) => {
    await replicate(req.body);
    res.json({ message: "Movie added (replicated)" });
});

app.put("/api/movies/:tconst", async (req, res) => {
    req.body.tconst = req.params.tconst;
    await replicate(req.body);
    res.json({ message: "Movie updated (replicated)" });
});

app.delete("/api/movies/:tconst", async (req, res) => {
    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst=?`, [req.params.tconst]);
    await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst=?`, [req.params.tconst]);
    await localDB.query(`DELETE FROM ${tableLocal} WHERE tconst=?`, [req.params.tconst]);
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

// START SERVER
app.listen(3000, () => console.log("NODE 2 backend running on port 3000"));
