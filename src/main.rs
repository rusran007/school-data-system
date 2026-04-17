use axum::{
    routing::{get, post},
    Router,
    Json,
    extract::Query,
    extract::Path as AxumPath,
    response::IntoResponse,
};
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tower_http::services::ServeDir;
use walkdir::WalkDir;
use std::sync::atomic::{AtomicBool, Ordering};
static IS_INDEXING: AtomicBool = AtomicBool::new(false);
use rusqlite::{Connection, params};
const DATA_PATH: &str = "D:/DATA_Q-Junier";

#[derive(serde::Serialize)]
struct SchoolFiles {
    images: Vec<FileItem>,
    documents: Vec<FileItem>,
}

#[derive(Serialize)]
struct SearchItem {
    name: String,
    path: String,
    is_dir: bool,
}

#[derive(serde::Serialize)]
struct FileItem {
    name: String,
    is_dir: bool,
    path: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Teacher {
    id: Option<i64>,
    school: String,
    name: String,
    level: String,
    course: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Ticket {
    id:          Option<i64>,
    school:      String,
    coordinator: String,
    fee:         Option<f64>,
    expense:     Option<f64>,
    date:        String,
    addr:        String,
    done:        Option<i32>,
    teachers:    Vec<TicketTeacher>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct TicketTeacher {
    id:        Option<i64>,
    ticket_id: Option<i64>,
    name:      String,
    level:     String,
    course:    String,  
}

#[tokio::main]
async fn main() {

    init_db();
    index_files();

    let app = Router::new()

        .route("/api/list", get(list_files))
        .route("/api/search", get(search_files))
        .route("/api/reindex", get(reindex))
        .route("/api/teachers", get(get_teachers).post(add_teacher))
        .route("/api/tickets", get(get_tickets).post(add_ticket))
        .route("/api/tickets/done", get(get_done_tickets))        // ← ต้องอยู่ก่อน :id
        .route("/api/tickets/:id/done", post(ticket_done))
        .route("/api/tickets/:id", post(update_ticket).delete(delete_ticket))  // ← รวมเป็นตัวเดียว
        .route("/api/ticket-teachers", get(get_ticket_teachers))
        .route("/api/schools", get(get_schools))
        .route("/api/school-files", get(get_school_files))
        .route("/api/school-tickets", get(get_school_tickets))
        .route("/api/other", get(get_other))
        // สำคัญมาก
        .nest_service("/files", ServeDir::new(DATA_PATH))

        .nest_service("/", ServeDir::new("static"));

    println!("Server running: http://localhost:3000");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn get_done_tickets() -> Json<Vec<Ticket>> {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, school, coordinator, fee, expense, date, addr, done
             FROM tickets WHERE done = 1 ORDER BY id DESC"
        ).unwrap();
        let rows = stmt.query_map([], |row| Ok(Ticket {
            id:          row.get(0)?,
            school:      row.get(1)?,
            coordinator: row.get(2)?,
            fee:         row.get(3)?,
            expense:     row.get(4)?,
            date:        row.get(5)?,
            addr:        row.get(6)?,
            done:        row.get(7)?,
            teachers:    vec![],
        })).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    }).await.unwrap_or_default().into()
}

async fn update_ticket(AxumPath(id): AxumPath<i64>, Json(body): Json<Ticket>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        conn.execute(
            "UPDATE tickets SET school=?1, coordinator=?2, fee=?3, expense=?4, date=?5, addr=?6 WHERE id=?7",
            params![body.school, body.coordinator, body.fee, body.expense, body.date, body.addr, id],
        ).unwrap();

        // ลบครูเก่าแล้ว insert ใหม่
        conn.execute("DELETE FROM ticket_teachers WHERE ticket_id=?1", [id]).unwrap();
        for t in &body.teachers {
            conn.execute(
                "INSERT INTO ticket_teachers (ticket_id, name, level, course) VALUES (?1,?2,?3,?4)",
                params![id, t.name, t.level, t.course],
            ).unwrap();
        }
    }).await.unwrap();
    "ok"
}

async fn get_school_tickets(Query(params): Query<HashMap<String,String>>) -> Json<Vec<Ticket>> {
    let school = params.get("school").cloned().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, school, coordinator, fee, expense, date, addr, done
             FROM tickets WHERE school = ?1 AND done = 0 ORDER BY id DESC"
        ).unwrap();
        let rows = stmt.query_map([&school], |row| Ok(Ticket {
            id:          row.get(0)?,
            school:      row.get(1)?,
            coordinator: row.get(2)?,
            fee:         row.get(3)?,
            expense:     row.get(4)?,
            date:        row.get(5)?,
            addr:        row.get(6)?,
            done:        row.get(7)?,
            teachers:    vec![],
        })).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    }).await.unwrap_or_default().into()
}


async fn get_ticket_teachers(Query(params): Query<HashMap<String,String>>) -> Json<Vec<TicketTeacher>> {
    let ticket_id: i64 = params.get("ticket_id").and_then(|v| v.parse().ok()).unwrap_or(0);
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, ticket_id, name, level, course FROM ticket_teachers WHERE ticket_id = ?1"
        ).unwrap();
        let rows = stmt.query_map([ticket_id], |row| Ok(TicketTeacher {
            id:        row.get(0)?,
            ticket_id: row.get(1)?,
            name:      row.get(2)?,
            level:     row.get(3)?,
            course:    row.get(4)?,
        })).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    }).await.unwrap_or_default().into()
}

async fn delete_ticket(AxumPath(id): AxumPath<i64>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        conn.execute("DELETE FROM ticket_teachers WHERE ticket_id = ?1", [id]).unwrap();
        conn.execute("DELETE FROM tickets WHERE id = ?1", [id]).unwrap();
    }).await.unwrap();
    "ok"
}

async fn reindex() -> impl IntoResponse {
    if IS_INDEXING.swap(true, Ordering::SeqCst) {
        return "Already indexing...";
    }
    tokio::task::spawn_blocking(|| {
        index_files();
        IS_INDEXING.store(false, Ordering::SeqCst);
    });
    "Reindex started"
}

async fn list_files(Query(params): Query<HashMap<String,String>>) -> Json<Vec<FileItem>> {

    let path = params.get("path").cloned().unwrap_or_default();

    let base = Path::new(DATA_PATH)
    .canonicalize()
    .unwrap_or_else(|_| Path::new(DATA_PATH).to_path_buf());
    let full_path = if path.is_empty() {
    base.to_path_buf()
} else {
    base.join(&path)
};
    println!("REQUEST PATH: {}", path);
    println!("BASE PATH: {:?}", base);
    println!("FULL PATH (before canon): {:?}", full_path);

    // ป้องกัน ../
    let full_path = match full_path.canonicalize() {
        Ok(p) => {
            println!("FULL PATH (after canon): {:?}", p);

            if p.starts_with(&base) {
                p
            } else {
                println!("❌ Path not inside base!");
                return Json(vec![]);
            }
        }
        Err(e) => {
            println!("❌ Canonicalize error: {:?}", e);
            return Json(vec![]);
        }
    };

    let items = tokio::task::spawn_blocking(move || {
    let mut items = Vec::new();

    if let Ok(entries) = fs::read_dir(full_path) {
        for entry in entries.flatten() {

            if items.len() > 1000 { break; }
            let path_obj = entry.path();

            let file_name = entry.file_name().to_string_lossy().to_string();
            let is_dir = entry.file_type().map(|f| f.is_dir()).unwrap_or(false);

            let relative = path_obj
                .strip_prefix(&base)
                .unwrap_or(&path_obj)
                .to_string_lossy()
                .replace("\\", "/");

            items.push(FileItem {
                name: file_name,
                is_dir,
                path: relative,
            });
        }
    }

    items
})
.await
.unwrap_or_else(|_| Vec::new());

Json(items)
}

async fn search_files(Query(params): Query<HashMap<String,String>>) -> Json<Vec<FileItem>> {
    let q = params.get("q").cloned().unwrap_or_default();

    let results = tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT name, path, is_dir FROM files WHERE name LIKE ?1 ORDER BY is_dir DESC, name ASC LIMIT 100"
        ).unwrap();

        let rows = stmt.query_map(
            [format!("%{}%", q)],
            |row| Ok(FileItem {
                name: row.get(0)?,
                path: row.get(1)?,
                is_dir: row.get::<_, i32>(2)? == 1,
            })
        ).unwrap();

        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();

    Json(results)
}


fn init_db() {
    let conn = Connection::open("files.db").unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            id      INTEGER PRIMARY KEY,
            name    TEXT,
            path    TEXT,
            is_dir  INTEGER
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS teachers (
            id      INTEGER PRIMARY KEY,
            school  TEXT,
            name    TEXT,
            level   TEXT,
            course  TEXT
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS tickets (
            id          INTEGER PRIMARY KEY,
            school      TEXT,
            coordinator TEXT,
            fee         REAL,
            expense     REAL,
            date        TEXT,
            addr        TEXT,
            done        INTEGER DEFAULT 0
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS ticket_teachers (
            id        INTEGER PRIMARY KEY,
            ticket_id INTEGER,
            name      TEXT,
            level     TEXT,
            course    TEXT,
            FOREIGN KEY(ticket_id) REFERENCES tickets(id)
        )",
        [],
    ).unwrap();
}

fn index_files() {

    let conn = Connection::open("files.db").unwrap();

    let base = Path::new(DATA_PATH)
        .canonicalize()
        .unwrap_or_else(|_| Path::new(DATA_PATH).to_path_buf());

    conn.execute("BEGIN", []).unwrap();
    conn.execute("DELETE FROM files", []).unwrap();

    for entry in WalkDir::new(DATA_PATH).into_iter().filter_map(|e| e.ok()) {

        let path = entry.path();
        let Some(file_name) = path.file_name() else { continue; };
        let name = file_name.to_string_lossy().to_string();

        let relative = path.strip_prefix(&base)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace("\\","/");

        let is_dir = path.is_dir() as i32;

        conn.execute(
            "INSERT INTO files (name, path, is_dir) VALUES (?1, ?2, ?3)",
            params![name, relative, is_dir],
        ).unwrap();

    }
    conn.execute("COMMIT", []).unwrap();
    println!("Index complete!");
}

async fn get_teachers(Query(params): Query<HashMap<String,String>>) -> Json<Vec<Teacher>> {
    let school = params.get("school").cloned().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, school, name, level, course FROM teachers WHERE school = ?1"
        ).unwrap();
        let rows = stmt.query_map([&school], |row| Ok(Teacher {
            id: row.get(0)?,
            school: row.get(1)?,
            name: row.get(2)?,
            level: row.get(3)?,
            course: row.get(4)?,
        })).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    }).await.unwrap_or_default().into()
}

async fn add_teacher(Json(body): Json<Teacher>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        conn.execute(
            "INSERT INTO teachers (school, name, level, course) VALUES (?1, ?2, ?3, ?4)",
            params![body.school, body.name, body.level, body.course],
        ).unwrap();
    }).await.unwrap();
    "ok"
}

async fn get_tickets() -> Json<Vec<Ticket>> {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, school, coordinator, fee, expense, date, addr, done
             FROM tickets WHERE done = 0 ORDER BY id DESC"
        ).unwrap();
        let rows = stmt.query_map([], |row| Ok(Ticket {
            id:          row.get(0)?,
            school:      row.get(1)?,
            coordinator: row.get(2)?,
            fee:         row.get(3)?,
            expense:     row.get(4)?,
            date:        row.get(5)?,
            addr:        row.get(6)?,
            done:        row.get(7)?,
            teachers:    vec![],
        })).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    }).await.unwrap_or_default().into()
}

async fn add_ticket(Json(body): Json<Ticket>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();

        conn.execute(
            "INSERT INTO tickets (school, coordinator, fee, expense, date, addr)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![body.school, body.coordinator,
                    body.fee, body.expense, body.date, body.addr],
        ).unwrap();

        let ticket_id = conn.last_insert_rowid();

        for t in &body.teachers {
            conn.execute(
                "INSERT INTO ticket_teachers (ticket_id, name, level, course)
                 VALUES (?1, ?2, ?3, ?4)",
                params![ticket_id, t.name, t.level, t.course],
            ).unwrap();
        }
    }).await.unwrap();
    "ok"
}
async fn ticket_done(AxumPath(id): AxumPath<i64>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();

        let result = conn.query_row(
            "SELECT school FROM tickets WHERE id = ?1",
            [id],
            |row| row.get::<_, String>(0)
        );

        if let Ok(school) = result {
            let mut stmt = conn.prepare(
                "SELECT name, level, course FROM ticket_teachers WHERE ticket_id = ?1"
            ).unwrap();

            let teachers: Vec<(String, String, String)> = stmt.query_map(
                [id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            ).unwrap().filter_map(|r| r.ok()).collect();

            for (name, level, course) in teachers {
                conn.execute(
                    "INSERT INTO teachers (school, name, level, course)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![school, name, level, course],
                ).unwrap();
            }

            conn.execute(
                "UPDATE tickets SET done = 1 WHERE id = ?1",
                [id],
            ).unwrap();
        }
    }).await.unwrap();
    "ok"
}

async fn get_schools() -> Json<Vec<String>> {
    tokio::task::spawn_blocking(|| {
        let base = Path::new(DATA_PATH).join("2026");
        let mut schools = Vec::new();

        if let Ok(entries) = fs::read_dir(&base) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().map(|f| f.is_dir()).unwrap_or(false);
                if is_dir && name.starts_with("School_") {
                    schools.push(name.replacen("School_", "", 1));
                }
            }
        }

        schools.sort();
        schools
    }).await.unwrap_or_default().into()
}

async fn get_school_files(Query(params): Query<HashMap<String,String>>) -> Json<SchoolFiles> {
    let school = params.get("school").cloned().unwrap_or_default();

    tokio::task::spawn_blocking(move || {
        let school_path = Path::new(DATA_PATH)
            .join("2026")
            .join(format!("School_{}", school));

        let image_exts = ["jpg","jpeg","png","gif","webp"];
        let mut images = Vec::new();
        let mut documents = Vec::new();

        // โหลด images
        let img_path = school_path.join("images");
        if let Ok(entries) = fs::read_dir(&img_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let ext = name.split('.').last().unwrap_or("").to_lowercase();
                let relative = entry.path()
                    .strip_prefix(DATA_PATH)
                    .unwrap_or(&entry.path())
                    .to_string_lossy()
                    .replace("\\", "/");

                if image_exts.contains(&ext.as_str()) {
                    images.push(FileItem { name, is_dir: false, path: relative });
                }
            }
        }

        // โหลด documents
        let doc_path = school_path.join("documents");
        if let Ok(entries) = fs::read_dir(&doc_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let relative = entry.path()
                    .strip_prefix(DATA_PATH)
                    .unwrap_or(&entry.path())
                    .to_string_lossy()
                    .replace("\\", "/");
                documents.push(FileItem { name, is_dir: false, path: relative });
            }
        }

        SchoolFiles { images, documents }
    }).await.unwrap_or(SchoolFiles { images: vec![], documents: vec![] }).into()
}

async fn get_other() -> Json<Vec<FileItem>> {
    tokio::task::spawn_blocking(|| {
        let other_path = Path::new(DATA_PATH).join("2026").join("other");
        let mut items = Vec::new();

        if let Ok(entries) = fs::read_dir(&other_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().map(|f| f.is_dir()).unwrap_or(false);
                let relative = entry.path()
                    .strip_prefix(DATA_PATH)
                    .unwrap_or(&entry.path())
                    .to_string_lossy()
                    .replace("\\", "/");
                items.push(FileItem { name, is_dir, path: relative });
            }
        }

        items
    }).await.unwrap_or_default().into()
}