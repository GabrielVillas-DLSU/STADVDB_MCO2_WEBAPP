// =====================
// SERVER 1 - FRAGMENT NODE (≤ 2010)
// =====================
const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors({ origin: "*", methods: ["GET","POST","PUT","DELETE"] }));
app.use(express.json());

const mysql = require("mysql2/promise");

// DB POOLS
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
const tableLocal   = "dim_title_f1";
const tableF2      = "dim_title_f2";

// ---------------------
// Failover Queries
// ---------------------
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

// ---------------------
// Replication + Recovery
// ---------------------
let recoveryQueue = [];

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

async function replicate(movie) {
    const values = normalize(movie);

    try {
        await centralDB.query(`REPLACE INTO ${tableCentral}
        (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
    } catch {
        console.log("CENTRAL DOWN → queueing for later");
        recoveryQueue.push(values);
    }

    // write local
    await localDB.query(`REPLACE INTO ${tableLocal}
    (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);

    // if > 2010 → also write to F2
    if (values[5] > 2010) {
        await f2DB.query(`REPLACE INTO ${tableF2}
        (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
    }
}

// Recovery loop
setInterval(async () => {
    if (!recoveryQueue.length) return;

    console.log("Attempting CENTRAL recovery…");

    try {
        for (const values of recoveryQueue) {
            await centralDB.query(`REPLACE INTO ${tableCentral}
            (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values);
        }
        recoveryQueue = [];
        console.log("CENTRAL RECOVERY SUCCESS!");
    } catch {
        console.log("CENTRAL still down… retrying later");
    }
}, 5000);

// ---------------------
// API ROUTES
// ---------------------

app.get("/api/health", (req,res)=>res.json({status:"OK"}));

app.get("/api/movies", async (req,res)=>{
    const [rows] = await queryFailover(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
});

app.get("/api/movies/search", async (req,res)=>{
    const term = "%" + req.query.q + "%";
    const [rows] = await queryFailover(
        `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE '${term}' LIMIT 200`);
    res.json(rows);
});

app.post("/api/movies", async (req,res)=>{
    await replicate(req.body);
    res.json({message:"Movie added (replicated)"});
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


app.delete("/api/movies/:tconst", async (req,res)=>{
    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst=?`, [req.params.tconst]);
    await localDB.query(`DELETE FROM ${tableLocal} WHERE tconst=?`, [req.params.tconst]);
    await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst=?`, [req.params.tconst]);
    res.json({message:"Movie deleted across nodes"});
});

// REPORTS
app.get("/api/reports/top-genres", async (req,res)=>{
    const [rows] = await queryFailover(`
        SELECT genres, COUNT(*) AS cnt
        FROM ${tableCentral}
        GROUP BY genres
        ORDER BY cnt DESC
        LIMIT 5`);
    res.json(rows);
});

app.get("/api/reports/most-titles-year", async (req,res)=>{
    const [rows] = await queryFailover(`
        SELECT startYear, COUNT(*) AS count
        FROM ${tableCentral}
        GROUP BY startYear
        ORDER BY count DESC
        LIMIT 1`);
    res.json(rows[0]);
});

app.get("/api/reports/adult-count", async (req,res)=>{
    const [rows] = await queryFailover(`
        SELECT SUM(isAdult=1) AS adultCount, SUM(isAdult=0) AS nonAdultCount
        FROM ${tableCentral}`);
    res.json(rows[0]);
});

// START
app.listen(3000,()=>console.log("NODE 1 (≤2010) running on port 3000"));
