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

    const body = {
        tconst: "tt" + Math.floor(Math.random()*99999999),
        type: document.getElementById("type").value,
        title: document.getElementById("title").value,
        year: parseInt(document.getElementById("year").value),
        director: document.getElementById("director").value,
        isAdult: document.querySelector("input[name=isAdult]:checked").value,
        genre: document.getElementById("Genre").value
    };

    await fetch(`${ACTIVE_API}/movies`, {
        method: "POST",
        headers: { "Content-Type":"application/json" },
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
    await waitForAPI();

    const tconst = localStorage.getItem("updateTconst");
    if (!tconst) return;

    const res = await fetch(`${ACTIVE_API}/movies/search?q=${tconst}`);
    const data = await res.json();
    const m = data[0];

    document.getElementById("Updatetype").value = m.titleType;
    document.getElementById("Updatetitle").value = m.primaryTitle;
    document.getElementById("Updateyear").value = m.startYear;
    document.getElementById("Updatedirector").value = m.director || "";
    document.getElementById("Genre").value = m.genres;

    document.querySelector(`input[name=UpdateisAdult][value=${m.isAdult ? "yes" : "no"}]`).checked = true;
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

    const res = await fetch(`${ACTIVE_API}/api/reports/top-genres`);
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

    const res = await fetch(`${ACTIVE_API}/api/reports/most-titles-year`);
    const data = await res.json();

    document.getElementById("mostTitlesYear").innerText =
        `${data.startYear} (${data.count} titles)`;
}

//--------------------------------------------------
// REPORT: ADULT STATS
//--------------------------------------------------
async function loadAdultStats() {
    await waitForAPI();

    const res = await fetch(`${ACTIVE_API}/api/reports/adult-count`);
    const data = await res.json();

    document.getElementById("adultCount").innerText =
        `Adult: ${data.adultCount} | Non-Adult: ${data.nonAdultCount}`;
}
