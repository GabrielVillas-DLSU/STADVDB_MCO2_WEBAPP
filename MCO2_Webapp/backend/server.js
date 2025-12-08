// =====================
// SERVER 2 - FRAGMENT NODE (> 2010)
// =====================
const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({ origin: "*", methods: ["GET","POST","PUT","DELETE"] }));
app.use(express.json());

const mysql = require("mysql2/promise");

// =====================
// DATABASE POOLS
// =====================
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

// =====================
// NORMALIZE DATA
// =====================
function normalize(movie) {
    return [
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
}

// =====================
// FAILOVER READ QUERY
// =====================
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

// =====================
// REPLICATION + RECOVERY
// =====================
let recoveryQueue = [];

async function replicate(movie) {
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


  // Central node (always try first)
    try {
        await centralDB.query(`
            REPLACE INTO dim_title
            (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, values);
        console.log("Central write OK");
    } catch (err) {
        console.log("CENTRAL DOWN – queued:", movie.tconst);
        recoveryQueue.push(movie);
    }

    // Write to correct fragment
    try {
        if (movie.startYear <= 2010) {
            await f1DB.query(`
                REPLACE INTO dim_title_f1
                (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, values);
            console.log("Fragment F1 write OK");
        } else {
            await localDB.query(`
                REPLACE INTO dim_title_f2
                (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, values);
            console.log("Fragment F2 write OK");
        }
    } catch (err) {
        console.log("❌ Fragment write failed:", err.message);
    }
}
// =====================
// RECOVERY LOOP
// =====================
setInterval(async () => {
    if (!recoveryQueue.length) return;

    console.log("Attempting CENTRAL recovery…");

    try {
        for (const values of recoveryQueue) {
            await centralDB.query(
                `REPLACE INTO ${tableCentral}
                (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
        }
        recoveryQueue = [];
        console.log("CENTRAL RECOVERY SUCCESS!");
    } catch {
        console.log("CENTRAL still down… retrying later.");
    }

}, 5000);

// =====================
// API ROUTES
// =====================

app.get("/api/health", (req, res) => {
    res.json({ status: "OK" });
});

app.get("/api/movies", async (req, res) => {
    const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
});

app.get("/api/movies/search", async (req, res) => {
    const term = "%" + req.query.q + "%";

    try {
        const [rows] = await queryFailover(
            `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE ? LIMIT 200`,
            [term]
        );

        return res.json(rows);

    } catch (err) {
        console.log("SEARCH ERROR:", err.message);
        // Return only once
        return res.status(500).json({ error: "Search failed", detail: err.message });
    }
});


app.post("/api/movies", async (req, res) => {
    await replicate(req.body);
    res.json({ message: "Movie added (replicated)" });
});

app.put("/api/movies/:tconst", async (req, res) => {
    try {
        req.body.tconst = req.params.tconst;
        await replicate(req.body);
        res.json({ message: "Movie updated (replicated)" });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});


app.delete("/api/movies/:tconst", async (req, res) => {
    const t = req.params.tconst;

    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst=?`, [t]);
    await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst=?`, [t]);
    await localDB.query(`DELETE FROM ${tableLocal} WHERE tconst=?`, [t]);

    res.json({ message: "Movie deleted across nodes" });
});

// =====================
// REPORTS
// =====================

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

app.get("/api/reports/most-titles-year", async (req, res) => {
    const [rows] = await queryFailover(`
        SELECT startYear, COUNT(*) AS count
        FROM ${tableCentral}
        WHERE startYear IS NOT NULL
        GROUP BY startYear
        ORDER BY count DESC
        LIMIT 1
    `);
    res.json(rows[0]);
});

app.get("/api/reports/adult-count", async (req, res) => {
    const [rows] = await queryFailover(`
        SELECT SUM(isAdult=1) AS adultCount,
               SUM(isAdult=0) AS nonAdultCount
        FROM ${tableCentral}
    `);
    res.json(rows[0]);
});

// =====================
// START SERVER
// =====================
app.listen(3000, () =>
    console.log("NODE 2 (>2010) backend running on port 3000")
);
