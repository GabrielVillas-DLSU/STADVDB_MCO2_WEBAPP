//--------------------------------------------------
// FAILOVER API ENDPOINTS
//--------------------------------------------------
const API_NODES = [
    "http://ccscloud.dlsu.edu.ph:60181/api",   // Server0
    "http://ccscloud.dlsu.edu.ph:60182/api",   // Server1
    "http://ccscloud.dlsu.edu.ph:60183/api"    // Server2
];

let ACTIVE_API = null;

//--------------------------------------------------
// AUTO-DETECT WORKING NODE
//--------------------------------------------------
async function findWorkingNode() {
    for (let api of API_NODES) {
        try {
            const res = await fetch(`${api}/health`);
            if (res.ok) {
                ACTIVE_API = api;
                console.log("✅ Connected to node:", api);
                return;
            }
        } catch (err) {
            console.log("❌ Node offline:", api);
        }
    }

    alert("❌ No database nodes are reachable!");
}

findWorkingNode();

// Helper
async function waitForAPI() {
    while (!ACTIVE_API) {
        await new Promise(r => setTimeout(r, 200));
    }
}

//--------------------------------------------------
// GET ALL MOVIES
//--------------------------------------------------
async function loadMovies() {
    await waitForAPI();
    const res = await fetch(`${ACTIVE_API}/movies`);
    const data = await res.json();

    let table = document.getElementById("movieTableBody");
    if (!table) return;

    table.innerHTML = "";

    data.forEach((m, i) => {
        table.innerHTML += `
            <tr>
                <td>${i+1}</td>
                <td>${m.titleType}</td>
                <td>${m.primaryTitle}</td>
                <td>${m.startYear}</td>
                <td>${m.director || "-"}</td>
                <td>${m.isAdult ? "Yes" : "No"}</td>
                <td>${m.genres}</td>
                <td><button onclick="goUpdate('${m.tconst}')">Update</button></td>
                <td><button onclick="deleteMovie('${m.tconst}')">Delete</button></td>
            </tr>
        `;
    });
}

//--------------------------------------------------
// ADD MOVIE
//--------------------------------------------------
async function addMovie(event) {
    event.preventDefault();
    await waitForAPI();

    const typeEl   = document.getElementById("type");
    const titleEl  = document.getElementById("title");
    const yearEl   = document.getElementById("year");
    const genreEl  = document.getElementById("Genre");
    const adultEl  = document.querySelector("input[name=isAdult]:checked");

    // Basic safety check so we don't crash on null
    if (!typeEl || !titleEl || !yearEl || !genreEl || !adultEl) {
        alert("Add Movie form is not correctly loaded on this page.");
        return;
    }

    const isAdult = adultEl.value === "yes" ? 1 : 0;

    const body = {
        tconst: "tt" + Math.floor(Math.random() * 99999999),

        // match backend expectations
        titleType: typeEl.value,
        primaryTitle: titleEl.value,
        originalTitle: titleEl.value,   // you don't collect this separately
        startYear: parseInt(yearEl.value),
        endYear: null,
        runtimeMinutes: 0,
        isAdult: isAdult,
        genres: genreEl.value
    };

    await fetch(`${ACTIVE_API}/movies`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    });

    alert("Movie added!");
    window.location.href = "ViewMovies.html";
}

//--------------------------------------------------
// DELETE MOVIE
//--------------------------------------------------
async function deleteMovie(tconst) {
    await waitForAPI();
    if (!confirm("Delete movie?")) return;

    await fetch(`${ACTIVE_API}/movies/${tconst}`, {
        method: "DELETE"
    });

    alert("Deleted!");
    loadMovies();
}

//--------------------------------------------------
// GO TO UPDATE PAGE
//--------------------------------------------------
function goUpdate(tconst) {
    localStorage.setItem("updateTconst", tconst);
    window.location.href = "UpdatePage.html";
}

//--------------------------------------------------
// LOAD UPDATE FORM
//--------------------------------------------------
async function loadUpdateForm() {
    const tconst = localStorage.getItem("updateTconst");
    if (!tconst) return;

    const res = await fetch(`${ACTIVE_API}/movies/search?q=${tconst}`);
    const data = await res.json();

    if (!data || data.length === 0) {
        alert("Movie not found!");
        return;
    }

    const m = data[0];  // now safe

    document.getElementById("Updatetype").value = m.titleType;
    document.getElementById("Updatetitle").value = m.primaryTitle;
    document.getElementById("Updateyear").value = m.startYear;
    document.getElementById("Updatedirector").value = m.originalTitle || "";
    document.getElementById("Genre").value = m.genres;

    document.querySelector(
        `input[name=UpdateisAdult][value=${m.isAdult ? "yes" : "no"}]`
    ).checked = true;
}

//--------------------------------------------------
// SUBMIT UPDATE
//--------------------------------------------------
async function updateMovie(event) {
    event.preventDefault();
    await waitForAPI();

    const tconst = localStorage.getItem("updateTconst");

    const body = {
        type: document.getElementById("Updatetype").value,
        title: document.getElementById("Updatetitle").value,
        year: parseInt(document.getElementById("Updateyear").value),
        director: document.getElementById("Updatedirector").value,
        isAdult: document.querySelector("input[name=UpdateisAdult]:checked").value,
        genre: document.getElementById("Genre").value
    };

    await fetch(`${ACTIVE_API}/movies/${tconst}`, {
        method: "PUT",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify(body)
    });

    alert("Movie updated!");
    window.location.href = "ViewMovies.html";
}

//--------------------------------------------------
// SEARCH
//--------------------------------------------------
async function searchMovies(event) {
    event.preventDefault();
    await waitForAPI();

    const q = document.getElementById("searchBox").value;
    const res = await fetch(`${ACTIVE_API}/movies/search?q=${q}`);
    const data = await res.json();

    let results = document.getElementById("searchResults");
    results.innerHTML = "";

    data.forEach(m => {
        results.innerHTML += `
            <div>
                <b>${m.primaryTitle}</b> (${m.startYear}) — ${m.genres}
            </div>
        `;
    });
}

//--------------------------------------------------
// REPORT: TOP GENRES
//--------------------------------------------------
async function loadTopGenres() {
    await waitForAPI();

    const res = await fetch(`${ACTIVE_API}/reports/top-genres`);
    const data = await res.json();

    const list = document.getElementById("topGenres");
    list.innerHTML = "";

    data.forEach(g => list.innerHTML += `<li>${g.genres}: ${g.cnt}</li>`);
}

//--------------------------------------------------
// REPORT: MOST TITLE YEAR
//--------------------------------------------------
async function loadMostTitlesYear() {
    await waitForAPI();

    const res = await fetch(`${ACTIVE_API}/reports/most-titles-year`);
    const data = await res.json();

    document.getElementById("mostTitlesYear").innerText =
        `${data.startYear} (${data.count} titles)`;
}

//--------------------------------------------------
// REPORT: ADULT STATS
//--------------------------------------------------
async function loadAdultStats() {
    await waitForAPI();

    const res = await fetch(`${ACTIVE_API}/reports/adult-count`);
    const data = await res.json();

    document.getElementById("adultCount").innerText =
        `Adult: ${data.adultCount} | Non-Adult: ${data.nonAdultCount}`;
}
