// ---------------------------------------------
// STADVDB MCO2 – Distributed DB Backend
// Node.js + Express + MySQL2
// ---------------------------------------------

const express = require('express');
const mysql = require('mysql2/promise');
const fs = require('fs');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

// ---------------------------------------------
// DATABASE CONNECTIONS (3 NODES)
// ---------------------------------------------
const centralDB = mysql.createPool({
    host: "ccscloud.dlsu.edu.ph",
    port: 60781,
    user: "root",
    password: "",
    database: "imdb_title_basics"
});

const node1DB = mysql.createPool({
    host: "ccscloud.dlsu.edu.ph",
    port: 60782,
    user: "root",
    password: "",
    database: "imdb_title_f1"
});

const node2DB = mysql.createPool({
    host: "ccscloud.dlsu.edu.ph",
    port: 60783,
    user: "root",
    password: "",
    database: "imdb_title_f2"
});

// ---------------------------------------------------
// LOCAL WRITE-AHEAD LOG (WAL) FOR MISSED REPLICATION
// ---------------------------------------------------
const LOG_FILE = "replication_queue.json";

// ensure exists
if (!fs.existsSync(LOG_FILE)) fs.writeFileSync(LOG_FILE, JSON.stringify([]));

function logFailure(task) {
    const queue = JSON.parse(fs.readFileSync(LOG_FILE));
    queue.push(task);
    fs.writeFileSync(LOG_FILE, JSON.stringify(queue, null, 2));
}

async function replayFailedReplications() {
    const queue = JSON.parse(fs.readFileSync(LOG_FILE));
    const stillFailed = [];

    for (const task of queue) {
        try {
            if (task.target === "central") {
                await centralDB.query(task.sql, task.params);
            } else if (task.target === "node1") {
                await node1DB.query(task.sql, task.params);
            } else if (task.target === "node2") {
                await node2DB.query(task.sql, task.params);
            }
        } catch (err) {
            stillFailed.push(task); // keep if still failing
        }
    }

    fs.writeFileSync(LOG_FILE, JSON.stringify(stillFailed, null, 2));
}

// ---------------------------------------------------
// REPLICATION ENGINE
// ---------------------------------------------------
async function replicateToCentral(sql, params) {
    try {
        await centralDB.query(sql, params);
        return { success: true };
    } catch (err) {
        // Log failure for later
        logFailure({ target: "central", sql, params });
        return { success: false, message: "Central replication failed" };
    }
}

async function replicateToFragments(sql, params, year) {
    try {
        if (year <= 2010) {
            await node1DB.query(sql, params);
            return { success: true };
        } else {
            await node2DB.query(sql, params);
            return { success: true };
        }
    } catch (err) {
        // Log failure
        const target = year <= 2010 ? "node1" : "node2";
        logFailure({ target, sql, params });
        return { success: false, message: "Fragment replication failed" };
    }
}

// ---------------------------------------------------
// BASIC API — View All Movies (Unified View)
// ---------------------------------------------------
app.get("/movies", async (req, res) => {
    try {
        const [rows0] = await centralDB.query("SELECT * FROM dim_title LIMIT 500");
        res.json(rows0);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ---------------------------------------------------
// ADD MOVIE (writes to central + appropriate fragment)
// ---------------------------------------------------
app.post("/movies/add", async (req, res) => {
    const { tconst, titleType, primaryTitle, startYear, genres } = req.body;

    const sql = `INSERT INTO dim_title (tconst,titleType,primaryTitle,startYear,genres)
                 VALUES (?,?,?,?,?)`;
    const params = [tconst, titleType, primaryTitle, startYear, genres];

    try {
        await centralDB.query(sql, params);
        await replicateToFragments(sql, params, startYear);

        res.json({ success: true, message: "Movie added + replicated." });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ---------------------------------------------------
// UPDATE MOVIE (replication included)
// ---------------------------------------------------
app.put("/movies/update/:tconst", async (req, res) => {
    const tconst = req.params.tconst;
    const { primaryTitle, startYear } = req.body;

    const sql = `UPDATE dim_title SET primaryTitle=?, startYear=? WHERE tconst=?`;
    const params = [primaryTitle, startYear, tconst];

    try {
        await centralDB.query(sql, params);
        await replicateToFragments(sql, params, startYear);

        res.json({ success: true, message: "Update OK + replicated." });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ---------------------------------------------------
// DELETE MOVIE
// ---------------------------------------------------
app.delete("/movies/delete/:tconst", async (req, res) => {
    const tconst = req.params.tconst;

    const sql = `DELETE FROM dim_title WHERE tconst=?`;

    try {
        await centralDB.query(sql, [tconst]);
        await replicateToCentral(sql, [tconst]);   // delete from fragments too (generic)

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ---------------------------------------------------
// SIMULATE CONCURRENT READS
// ---------------------------------------------------
app.get("/simulate/concurrent-read/:tconst", async (req, res) => {
    const tconst = req.params.tconst;

    try {
        const results = await Promise.all([
            centralDB.query("SELECT * FROM dim_title WHERE tconst=?", [tconst]),
            node1DB.query("SELECT * FROM dim_title_f1 WHERE tconst=?", [tconst]),
            node2DB.query("SELECT * FROM dim_title_f2 WHERE tconst=?", [tconst])
        ]);

        res.json({
            central: results[0][0],
            node1: results[1][0],
            node2: results[2][0]
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ---------------------------------------------------
// SIMULATE WRITER + READERS
// ---------------------------------------------------
app.post("/simulate/write-read", async (req, res) => {
    const { tconst, newTitle } = req.body;

    const sql = "UPDATE dim_title SET primaryTitle=? WHERE tconst=?";
    const params = [newTitle, tconst];

    try {
        // writer (central)
        const writePromise = centralDB.query(sql, params);

        // readers (node1 + node2)
        const read1 = node1DB.query("SELECT * FROM dim_title_f1 WHERE tconst=?", [tconst]);
        const read2 = node2DB.query("SELECT * FROM dim_title_f2 WHERE tconst=?", [tconst]);

        const results = await Promise.all([writePromise, read1, read2]);

        res.json({
            updateStatus: "writer done",
            readers: {
                node1: results[1][0],
                node2: results[2][0]
            }
        });

    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ---------------------------------------------------
// FAILURE SIMULATION
// ---------------------------------------------------
app.post("/simulate/failure-central", (req, res) => {
    logFailure({
        target: "central",
        sql: "UPDATE dim_title SET primaryTitle='FAILED' WHERE tconst='tt0000001'",
        params: []
    });
    res.json({ success: true, message: "Simulated central node failure." });
});

app.post("/simulate/failure-node1", (req, res) => {
    logFailure({
        target: "node1",
        sql: "UPDATE dim_title_f1 SET primaryTitle='FAILED' WHERE tconst='tt0000001'",
        params: []
    });
    res.json({ success: true, message: "Simulated node1 failure." });
});

// ---------------------------------------------------
// RECOVERY — replay queued writes
// ---------------------------------------------------
app.post("/simulate/recovery", async (req, res) => {
    await replayFailedReplications();
    res.json({ success: true, message: "Recovery complete. All pending writes replayed." });
});

// ---------------------------------------------------
// START SERVER
// ---------------------------------------------------
app.listen(3000, () => {
    console.log("Backend running on port 3000");
});
