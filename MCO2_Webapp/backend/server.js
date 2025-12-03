const express = require("express");
const app = express();
const cors = require("cors");
app.use(cors());
app.use(express.json());

// =========================
// DATABASE CONNECTIONS
// =========================
const mysql = require("mysql2/promise");

const centralDB = mysql.createPool({
    host: "ccscloud.dlsu.edu.ph",
    port: 60781,
    user: "root",
    password: "Cr6Sq5RPcvZLubhjEAnF8tYX",
    database: "imdb_title_basics"
});

const f1DB = mysql.createPool({
    host: "ccscloud.dlsu.edu.ph",
    port: 60782,
    user: "root",
    password: "Cr6Sq5RPcvZLubhjEAnF8tYX",
    database: "imdb_title_f1"
});

const f2DB = mysql.createPool({
    host: "ccscloud.dlsu.edu.ph",
    port: 60783,
    user: "root",
    password: "Cr6Sq5RPcvZLubhjEAnF8tYX",
    database: "imdb_title_f2"
});

const tableCentral = "dim_title";
const tableF1 = "dim_title_f1";
const tableF2 = "dim_title_f2";

// =========================
// HELPER: REPLICATION LOGIC
// =========================
async function replicateInsertOrUpdate(movie) {
    const { tconst, titleType, primaryTitle, isAdult, startYear, genres } = movie;

    // Always write to Central
    await centralDB.query(
        `REPLACE INTO ${tableCentral} (tconst, titleType, primaryTitle, isAdult, startYear, genres)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [tconst, titleType, primaryTitle, isAdult, startYear, genres]
    );

    // Write to fragment based on year:
    if (startYear <= 2010) {
        await f1DB.query(
            `REPLACE INTO ${tableF1} (tconst, titleType, primaryTitle, isAdult, startYear, genres)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [tconst, titleType, primaryTitle, isAdult, startYear, genres]
        );
    } else {
        await f2DB.query(
            `REPLACE INTO ${tableF2} (tconst, titleType, primaryTitle, isAdult, startYear, genres)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [tconst, titleType, primaryTitle, isAdult, startYear, genres]
        );
    }
}

// =========================
// ROUTE: GET ALL MOVIES
// =========================
app.get("/movies", async (req, res) => {
    const [rows] = await centralDB.query(`SELECT * FROM ${tableCentral} LIMIT 200`);
    res.json(rows);
});

// =========================
// ROUTE: SEARCH MOVIES
// =========================
app.get("/movies/search", async (req, res) => {
    const q = "%" + req.query.q + "%";
    const [rows] = await centralDB.query(
        `SELECT * FROM ${tableCentral} WHERE primaryTitle LIKE ? LIMIT 200`,
        [q]
    );
    res.json(rows);
});

// =========================
// ROUTE: ADD MOVIE
// =========================
app.post("/movies", async (req, res) => {
    const { tconst, type, title, year, isAdult, genre } = req.body;

    const movie = {
        tconst,
        titleType: type,
        primaryTitle: title,
        isAdult: isAdult === "yes" ? 1 : 0,
        startYear: year,
        genres: genre
    };

    await replicateInsertOrUpdate(movie);
    res.json({ message: "Movie added + replicated", movie });
});

// =========================
// ROUTE: UPDATE MOVIE
// =========================
app.put("/movies/:tconst", async (req, res) => {
    const tconst = req.params.tconst;
    const { type, title, year, isAdult, genre } = req.body;

    const movie = {
        tconst,
        titleType: type,
        primaryTitle: title,
        isAdult: isAdult === "yes" ? 1 : 0,
        startYear: year,
        genres: genre
    };

    await replicateInsertOrUpdate(movie);
    res.json({ message: "Movie updated + replicated", movie });
});

// =========================
// ROUTE: DELETE MOVIE
// =========================
app.delete("/movies/:tconst", async (req, res) => {
    const tconst = req.params.tconst;

    await centralDB.query(`DELETE FROM ${tableCentral} WHERE tconst=?`, [tconst]);
    await f1DB.query(`DELETE FROM ${tableF1} WHERE tconst=?`, [tconst]);
    await f2DB.query(`DELETE FROM ${tableF2} WHERE tconst=?`, [tconst]);

    res.json({ message: "Movie deleted from all nodes" });
});

app.get("/reports/top-genres", async (req, res) => {
    const [rows] = await centralDB.query(`
        SELECT genres, COUNT(*) AS count
        FROM ${tableCentral}
        WHERE genres IS NOT NULL
        GROUP BY genres
        ORDER BY count DESC
        LIMIT 5;
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


// =========================
// START SERVER
// =========================
app.listen(3000, () => {
    console.log("Backend running on port 3000");
});
