use axum::{
    Json, Router,
    extract::Path as AxumPath,
    extract::Query,
    response::IntoResponse,
    routing::{get, post, put},
};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use tower_http::services::ServeDir;
use walkdir::WalkDir;
static IS_INDEXING: AtomicBool = AtomicBool::new(false);
use rusqlite::{Connection, params};
const DATA_PATH: &str = "D:/DATA_Q-Junier";

#[derive(serde::Serialize)]
struct SchoolFiles {
    images: Vec<FileItem>,
    documents: Vec<FileItem>,
}

#[derive(serde::Serialize)]
struct Summary {
    school_count: i64,
    teacher_count: i64,
    ticket_total: i64,
    ticket_pending: i64,
    ticket_done: i64,
    total_fee: f64,
    total_expense: f64,
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
    school: Option<String>,
    name: String,
    level: String,
    course: String,
    ticket_id: Option<i64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Ticket {
    id: Option<i64>,
    ticket_no: String,
    coordinator: String,
    fee: Option<f64>,
    start_date: String,
    end_date: String,
    addr: String,
    note: String,
    done: Option<i32>,
    schools: Vec<TicketSchool>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TicketTeacher {
    id: Option<i64>,
    ticket_id: Option<i64>,
    school: String,
    name: String,
    level: String,
    course: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TicketSchool {
    id: Option<i64>,
    ticket_id: Option<i64>,
    school: String,
    coordinator: String,
    expense: Option<f64>,
    teachers: Vec<TicketTeacher>,
}

#[tokio::main]
async fn main() {
    init_db();
    tokio::task::spawn_blocking(|| {
        index_files();
    });

    let app = Router::new()
        .route("/api/list", get(list_files))
        .route("/api/search", get(search_files))
        .route("/api/reindex", get(reindex))
        .route("/api/teachers", get(get_teachers).post(add_teacher))
        .route("/api/tickets", get(get_tickets).post(add_ticket))
        .route("/api/tickets/done", get(get_done_tickets))
        .route("/api/tickets/:id/done", post(ticket_done))
        .route("/api/tickets/:id", put(update_ticket).delete(delete_ticket))
        .route("/api/ticket-schools", get(get_ticket_schools))
        .route("/api/schools", get(get_schools))
        .route("/api/school-files", get(get_school_files))
        .route("/api/school-tickets", get(get_school_tickets))
        .route("/api/teacher-history", get(get_teacher_history))
        .route("/api/search-teacher", get(search_teacher))
        .route("/api/summary", get(get_summary))
        .nest_service("/files", ServeDir::new(DATA_PATH))
        .nest_service("/", ServeDir::new("static").fallback(get(|| async {
        axum::response::Html(fs::read_to_string("static/app.html").unwrap())
        })));

    println!("Server running: http://localhost:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_summary() -> Json<Summary> {
    tokio::task::spawn_blocking(|| {
        let conn = Connection::open("files.db").unwrap();

       let base = std::path::Path::new(DATA_PATH).join("2026");
       let school_count = std::fs::read_dir(&base)
       .map(|entries| {
        entries.flatten()
            .filter(|e| {
                e.file_type().map(|f| f.is_dir()).unwrap_or(false)
                && e.file_name().to_string_lossy().starts_with("School_")
            })
            .count() as i64
      })
       .unwrap_or(0);

        let teacher_count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT name || '|' || school) FROM teachers", [], |r| r.get(0)
        ).unwrap_or(0);

        let ticket_total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tickets", [], |r| r.get(0)
        ).unwrap_or(0);

        let ticket_pending: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tickets WHERE done = 0", [], |r| r.get(0)
        ).unwrap_or(0);

        let ticket_done: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tickets WHERE done = 1", [], |r| r.get(0)
        ).unwrap_or(0);

        let total_fee: f64 = conn.query_row(
            "SELECT COALESCE(SUM(fee), 0) FROM tickets", [], |r| r.get(0)
        ).unwrap_or(0.0);

        let total_expense: f64 = conn.query_row(
            "SELECT COALESCE(SUM(expense), 0) FROM ticket_schools", [], |r| r.get(0)
        ).unwrap_or(0.0);

        Summary {
            school_count,
            teacher_count,
            ticket_total,
            ticket_pending,
            ticket_done,
            total_fee,
            total_expense,
        }
    }).await.unwrap_or(Summary {
        school_count: 0, teacher_count: 0,
        ticket_total: 0, ticket_pending: 0, ticket_done: 0,
        total_fee: 0.0, total_expense: 0.0,
    }).into()
}

async fn get_done_tickets() -> Json<Vec<Ticket>> {
    tokio::task::spawn_blocking(|| {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, ticket_no, coordinator, fee, start_date, end_date, addr, note, done
                 FROM tickets WHERE done = 1 ORDER BY id DESC",
            )
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok(Ticket {
                    id: row.get(0)?,
                    ticket_no: row.get(1)?,
                    coordinator: row.get(2)?,
                    fee: row.get(3)?,
                    start_date: row.get(4)?,
                    end_date: row.get(5)?,
                    addr: row.get(6)?,
                    note: row.get(7)?,
                    done: row.get(8)?,
                    schools: vec![],
                })
            })
            .unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default()
    .into()
}

async fn update_ticket(
    AxumPath(id): AxumPath<i64>,
    Json(body): Json<Ticket>,
) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let mut conn = Connection::open("files.db").unwrap();
        let tx = conn.transaction().unwrap();

        // อัพเดต ticket หลัก
        tx.execute(
            "UPDATE tickets SET ticket_no=?1, coordinator=?2, fee=?3, start_date=?4, end_date=?5, addr=?6, note=?7 WHERE id=?8",
            params![body.ticket_no, body.coordinator, body.fee, body.start_date, body.end_date, body.addr, body.note, id],
        ).unwrap();

        // ลบของเก่าออกทั้งหมดก่อน (ทำแค่ครั้งเดียว ไม่ใช่ในลูป)
        tx.execute("DELETE FROM ticket_schools WHERE ticket_id=?1", [id]).unwrap();
        tx.execute("DELETE FROM ticket_teachers WHERE ticket_id=?1", [id]).unwrap();

        // insert ใหม่
        for s in &body.schools {
            tx.execute(
                "INSERT INTO ticket_schools (ticket_id, school, coordinator, expense) VALUES (?1, ?2, ?3, ?4)",
                params![id, s.school, s.coordinator, s.expense],
            ).unwrap();
            for t in &s.teachers {
                tx.execute(
                    "INSERT INTO ticket_teachers (ticket_id, school, name, level, course) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![id, s.school, t.name, t.level, t.course],
                ).unwrap();
            }
        }

        // ถ้า ticket done แล้ว sync teachers table ด้วย
        let is_done: i32 = tx.query_row(
            "SELECT done FROM tickets WHERE id = ?1", [id], |r| r.get(0)
        ).unwrap_or(0);

        if is_done == 1 {
            tx.execute("DELETE FROM teachers WHERE ticket_id = ?1", [id]).unwrap();
            let mut stmt = tx.prepare(
                "SELECT school, name, level, course FROM ticket_teachers WHERE ticket_id = ?1"
            ).unwrap();
            let list: Vec<(String,String,String,String)> = stmt
                .query_map([id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
            drop(stmt); // ต้อง drop ก่อน execute ต่อ
            for (school, name, level, course) in list {
                tx.execute(
                    "INSERT INTO teachers (school, name, level, course, ticket_id) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![school, name, level, course, id],
                ).unwrap();
            }
        }

        tx.commit().unwrap();
    })
    .await
    .unwrap();
    "ok"
}

async fn get_ticket_schools(
    Query(params): Query<HashMap<String, String>>,
) -> Json<Vec<TicketSchool>> {
    let ticket_id: i64 = params
        .get("ticket_id")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, ticket_id, school, coordinator, expense FROM ticket_schools WHERE ticket_id = ?1",
            )
            .unwrap();
        let schools_iter = stmt
            .query_map([ticket_id], |row| {
                Ok(TicketSchool {
                    id: row.get(0)?,
                    ticket_id: row.get(1)?,
                    school: row.get(2)?,
                    coordinator: row.get(3)?,
                    expense: row.get(4)?,
                    teachers: vec![],
                })
            })
            .unwrap();

        let mut schools = Vec::new();
        for school_res in schools_iter {
            if let Ok(mut s) = school_res {
                let mut stmt2 = conn.prepare(
    "SELECT MAX(id), school, name, MAX(level), MAX(course), MAX(ticket_id)
     FROM ticket_teachers 
     WHERE ticket_id = ?1 AND school = ?2
     GROUP BY name", // เพิ่มบรรทัดนี้เพื่อยุบชื่อครู
).unwrap();
                let teachers: Vec<TicketTeacher> = stmt2
                    .query_map(params![ticket_id, &s.school], |row| {
                        Ok(TicketTeacher {
                            id: row.get(0)?,
                            school: row.get(1)?,
                            name: row.get(2)?,
                            level: row.get(3)?,
                            course: row.get(4)?,
                            ticket_id: row.get(5)?,
                        })
                    })
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                s.teachers = teachers;
                schools.push(s);
            }
        }
        schools
    })
    .await
    .unwrap_or_default()
    .into()
}

async fn get_school_tickets(Query(params): Query<HashMap<String,String>>) -> Json<Vec<Ticket>> {
    let school = params.get("school").cloned().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        // 1. เปลี่ยนจาก JOIN เป็น LEFT JOIN
        // 2. ใช้ IFNULL เพื่อป้องกัน Error กรณีข้อมูลใน ticket_schools ยังไม่มี
        let mut stmt = conn.prepare(
            "SELECT t.id, t.ticket_no, t.coordinator, t.fee, t.start_date, t.end_date, t.addr, t.note, t.done, 
                    IFNULL(ts.coordinator, ''), IFNULL(ts.expense, 0.0)
             FROM tickets t
             LEFT JOIN ticket_schools ts ON ts.ticket_id = t.id
             WHERE ts.school = ?1
             ORDER BY t.done ASC, t.id DESC"
        ).unwrap();

        let rows = stmt.query_map([&school], |row| {
            let t_id: i64 = row.get(0)?;
            
            // ใช้ความพยายามดึงข้อมูล ถ้าไม่มีให้เป็นค่าว่าง
            let school_coord: String = row.get(9).unwrap_or_default();
            let school_exp: f64 = row.get(10).unwrap_or(0.0);

            Ok(Ticket {
                id:          Some(t_id),
                ticket_no:   row.get(1).unwrap_or_default(),
                coordinator: row.get(2).unwrap_or_default(),
                fee:         Some(row.get(3).unwrap_or(0.0)),
                start_date:  row.get(4).unwrap_or_default(),
                end_date:    row.get(5).unwrap_or_default(),
                addr:        row.get(6).unwrap_or_default(),
                note:        row.get(7).unwrap_or_default(),
                done:        Some(row.get(8).unwrap_or(0)),
                schools:     vec![TicketSchool {
                    id: None,
                    ticket_id: Some(t_id),
                    school: school.clone(),
                    coordinator: school_coord,
                    expense: Some(school_exp),
                    teachers: vec![],
                }],
            })
        }).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    }).await.unwrap_or_default().into()
}

async fn delete_ticket(AxumPath(id): AxumPath<i64>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        conn.execute("DELETE FROM teachers WHERE ticket_id = ?1", [id]).unwrap();
        conn.execute("DELETE FROM ticket_schools WHERE ticket_id = ?1", [id]).unwrap();
        conn.execute("DELETE FROM ticket_teachers WHERE ticket_id = ?1", [id]).unwrap();
        conn.execute("DELETE FROM tickets WHERE id = ?1", [id]).unwrap();
    })
    .await
    .unwrap();
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

async fn list_files(Query(params): Query<HashMap<String, String>>) -> Json<Vec<FileItem>> {
    let path = params.get("path").cloned().unwrap_or_default();
    let base = Path::new(DATA_PATH)
        .canonicalize()
        .unwrap_or_else(|_| Path::new(DATA_PATH).to_path_buf());
    let full_path = if path.is_empty() { base.to_path_buf() } else { base.join(&path) };
    let full_path = match full_path.canonicalize() {
        Ok(p) if p.starts_with(&base) => p,
        _ => return Json(vec![]),
    };
    let items = tokio::task::spawn_blocking(move || {
        let mut items = Vec::new();
        if let Ok(entries) = fs::read_dir(full_path) {
            for entry in entries.flatten() {
                if items.len() > 1000 { break; }
                let path_obj = entry.path();
                let file_name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().map(|f| f.is_dir()).unwrap_or(false);
                let relative = path_obj.strip_prefix(&base).unwrap_or(&path_obj)
                    .to_string_lossy().replace("\\", "/");
                items.push(FileItem { name: file_name, is_dir, path: relative });
            }
        }
        items
    })
    .await
    .unwrap_or_default();
    Json(items)
}

async fn search_files(Query(params): Query<HashMap<String, String>>) -> Json<Vec<FileItem>> {
    let q = params.get("q").cloned().unwrap_or_default();
    let results = tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT name, path, is_dir FROM files WHERE name LIKE ?1
             ORDER BY is_dir DESC, name ASC LIMIT 100",
        ).unwrap();
        let rows = stmt.query_map([format!("%{}%", q)], |row| {
            Ok(FileItem { name: row.get(0)?, path: row.get(1)?, is_dir: row.get::<_, i32>(2)? == 1 })
        }).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();
    Json(results)
}

fn init_db() {
    let conn = Connection::open("files.db").unwrap();

    // 1. ตารางไฟล์
    conn.execute("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, name TEXT, path TEXT, is_dir INTEGER)", []).unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files (path)", []).unwrap();
    // เพิ่มบรรทัดนี้เพื่อสร้าง index ใหม่ที่รวมชื่อไฟล์ด้วย เพื่อให้การค้นหาด้วยชื่อเร็วขึ้น
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_name ON files (name)", []).unwrap();

    // 2. ตารางคณะครู (ข้อมูลหลัก)
    conn.execute("CREATE TABLE IF NOT EXISTS teachers (id INTEGER PRIMARY KEY, school TEXT, name TEXT, level TEXT, course TEXT, ticket_id INTEGER)", []).unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_teachers_name_school ON teachers (name, school)", []).unwrap();

    // 3. ตาราง Tickets
    conn.execute("CREATE TABLE IF NOT EXISTS tickets (
        id INTEGER PRIMARY KEY, 
        ticket_no TEXT, 
        coordinator TEXT, 
        fee REAL, 
        start_date TEXT, 
        end_date TEXT, 
        addr TEXT, 
        note TEXT, 
        done INTEGER DEFAULT 0
    )", []).unwrap();
    
    // ตรวจสอบและเพิ่มคอลัมน์ใหม่ๆ สำหรับกรณีอัปเกรด
    let _ = conn.execute("ALTER TABLE tickets ADD COLUMN note TEXT", []);

    // 4. ตารางความสัมพันธ์ Ticket - โรงเรียน (เก็บค่าใช้จ่ายแยกรายที่)
    conn.execute("CREATE TABLE IF NOT EXISTS ticket_schools (
    id INTEGER PRIMARY KEY,
    ticket_id INTEGER,
    school TEXT,
    coordinator TEXT DEFAULT '',
    expense REAL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
    )", []).unwrap();
    // migration สำหรับ DB เก่าที่มีอยู่แล้ว
    let _ = conn.execute("ALTER TABLE ticket_schools ADD COLUMN coordinator TEXT DEFAULT ''", []);
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ticket_schools_school ON ticket_schools (school)", []).unwrap();

    // 5. ตารางรายชื่อครูที่มากับ Ticket นั้นๆ
    conn.execute("CREATE TABLE IF NOT EXISTS ticket_teachers (id INTEGER PRIMARY KEY, ticket_id INTEGER, school TEXT, name TEXT, level TEXT, course TEXT, FOREIGN KEY(ticket_id) REFERENCES tickets(id))", []).unwrap();
    // เพิ่มบรรทัดนี้เพื่อลบรายชื่อครูที่ "ไม่มีประวัติผูกอยู่" (กำจัดชื่อเก่าที่แก้ไขไปแล้ว)
}

fn index_files() {
    let mut conn = Connection::open("files.db").unwrap();
    let base = Path::new(DATA_PATH)
        .canonicalize()
        .unwrap_or_else(|_| Path::new(DATA_PATH).to_path_buf());

    println!("Starting Incremental Indexing...");

    // --- แก้ไขตรงนี้ ---
    // ใช้สโคป { } เพื่อให้ stmt ถูก Drop (ปล่อยวาง) ทันทีที่จบสโคป
    let mut existing_paths: std::collections::HashSet<String> = {
        let mut stmt = conn.prepare("SELECT path FROM files").unwrap();
        let paths = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        paths // ส่งค่ากลับไปให้ existing_paths แล้ว stmt จะตายลงตรงนี้
    }; 

    // ตอนนี้ conn เป็นอิสระแล้ว สามารถเปิด transaction ได้
    let tx = conn.transaction().unwrap();
    
    for entry in WalkDir::new(DATA_PATH).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        
        // แก้ไข warning: ใส่ _ หน้า metadata เพราะเรายังไม่ได้ใช้ประโยชน์จากมัน
        let _metadata = match path.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };

        let Some(file_name) = path.file_name() else { continue; };
        let name = file_name.to_string_lossy().to_string();
        let relative = path.strip_prefix(&base).unwrap_or(path)
            .to_string_lossy().replace("\\", "/");

        existing_paths.remove(&relative);

        let is_dir = path.is_dir() as i32;
        
        tx.execute(
            "INSERT OR IGNORE INTO files (name, path, is_dir) VALUES (?1, ?2, ?3)",
            params![name, relative, is_dir],
        ).unwrap();
    }

    for missing_path in existing_paths {
        tx.execute("DELETE FROM files WHERE path = ?1", params![missing_path]).unwrap();
    }

    tx.commit().unwrap();
    println!("Index complete! (Incremental)");
}

async fn get_teachers(Query(params): Query<HashMap<String, String>>) -> Json<Vec<Teacher>> {
    let school = params.get("school").cloned().unwrap_or_default();
    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT name, school, MAX(level), MAX(course), MAX(ticket_id)
             FROM (
                 SELECT tt.name, tt.school, tt.level, tt.course, tt.ticket_id
                 FROM ticket_teachers tt
                 JOIN tickets t ON tt.ticket_id = t.id
                 WHERE tt.school = ?1

                 UNION

                 SELECT tr.name, tr.school, tr.level, tr.course, tr.ticket_id
                 FROM teachers tr
                 JOIN tickets t ON tr.ticket_id = t.id
                 WHERE tr.school = ?1
             )
             GROUP BY name
             ORDER BY name ASC"
        ).unwrap();

        let rows = stmt.query_map([&school], |row| {
            Ok(Teacher {
                id:        None,
                name:      row.get(0)?,
                school:    Some(row.get(1)?),
                level:     row.get(2)?,
                course:    row.get(3)?,
                ticket_id: row.get(4)?,
            })
        }).unwrap();

        rows.filter_map(|r| r.ok()).collect::<Vec<Teacher>>()
    }).await;

    match result {
        Ok(teachers) => Json(teachers),
        Err(_) => Json(vec![]),
    }
}

async fn add_teacher(Json(body): Json<Teacher>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        conn.execute(
            "INSERT INTO teachers (school, name, level, course) VALUES (?1, ?2, ?3, ?4)",
            params![body.school, body.name, body.level, body.course],
        ).unwrap();
    })
    .await
    .unwrap();
    "ok"
}

async fn get_tickets() -> Json<Vec<Ticket>> {
    tokio::task::spawn_blocking(|| {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, ticket_no, coordinator, fee, start_date, end_date, addr, note, done
             FROM tickets WHERE done = 0 ORDER BY id DESC",
        ).unwrap();
        let rows = stmt.query_map([], |row| {
            Ok(Ticket {
                id: row.get(0)?, ticket_no: row.get(1)?, coordinator: row.get(2)?,
                fee: row.get(3)?, start_date: row.get(4)?, end_date: row.get(5)?, addr: row.get(6)?,
                note: row.get(7)?, done: row.get(8)?, schools: vec![],
            })
        }).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default()
    .into()
}

async fn add_ticket(Json(body): Json<Ticket>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let mut conn = Connection::open("files.db").unwrap();
        let tx = conn.transaction().unwrap();

        let last_id: i64 = tx
        .query_row("SELECT MAX(id) FROM tickets", [], |r| r.get(0))
        .unwrap_or(0); // ถ้าไม่มีข้อมูลเลยจะได้ 0
        let ticket_no = format!("SMB-Q{:04}", last_id + 1);

        tx.execute(
            "INSERT INTO tickets (ticket_no, coordinator, start_date, end_date, addr, note, fee) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![ticket_no, body.coordinator, body.start_date, body.end_date, body.addr, body.note, body.fee],
        ).unwrap();

        let ticket_id = tx.last_insert_rowid();

        for s in &body.schools {
            tx.execute(
                "INSERT INTO ticket_schools (ticket_id, school, coordinator, expense) VALUES (?1, ?2, ?3, ?4)",
                params![ticket_id, s.school, s.coordinator, s.expense],
            ).unwrap();
            for t in &s.teachers {
                tx.execute(
                    "INSERT INTO ticket_teachers (ticket_id, school, name, level, course) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![ticket_id, s.school, t.name, t.level, t.course],
                ).unwrap();
            }
        }

        tx.commit().unwrap();
    })
    .await
    .unwrap();
    "ok"
}

async fn ticket_done(AxumPath(id): AxumPath<i64>) -> impl IntoResponse {
    tokio::task::spawn_blocking(move || {
        let mut conn = Connection::open("files.db").unwrap();
        let tx = conn.transaction().unwrap();

        // 1. แก้ SQL: เอา coordinator ออกจาก SELECT (เพราะตาราง ticket_teachers ไม่มีคอลัมน์นี้)
        let mut stmt = tx
            .prepare("SELECT school, name, level, course FROM ticket_teachers WHERE ticket_id = ?1")
            .unwrap();
            
        let list: Vec<(String, String, String, String)> = stmt
            .query_map([id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt); 

        // 2. แก้ตอน INSERT: เอา coordinator ออก (เพราะตาราง teachers เก็บแค่ข้อมูลครู)
        for (school, name, level, course) in list {
            tx.execute(
                "INSERT INTO teachers (school, name, level, course, ticket_id) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![school, name, level, course, id],
            ).unwrap();
        }

        // 3. อัปเดตสถานะ Ticket เป็นเสร็จสิ้น
        tx.execute("UPDATE tickets SET done = 1 WHERE id = ?1", [id]).unwrap();
        
        tx.commit().unwrap();
    })
    .await
    .unwrap();
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
    })
    .await
    .unwrap_or_default()
    .into()
}

async fn get_school_files(Query(params): Query<HashMap<String, String>>) -> Json<SchoolFiles> {
    let school = params.get("school").cloned().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let school_path = Path::new(DATA_PATH).join("2026").join(format!("School_{}", school));
        let image_exts = ["jpg", "jpeg", "png", "gif", "webp"];
        let mut images = Vec::new();
        let mut documents = Vec::new();
        let img_path = school_path.join("images");
        if let Ok(entries) = fs::read_dir(&img_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let ext = name.split('.').last().unwrap_or("").to_lowercase();
                let relative = entry.path().strip_prefix(DATA_PATH).unwrap_or(&entry.path())
                    .to_string_lossy().replace("\\", "/");
                if image_exts.contains(&ext.as_str()) {
                    images.push(FileItem { name, is_dir: false, path: relative });
                }
            }
        }
        let doc_path = school_path.join("documents");
        if let Ok(entries) = fs::read_dir(&doc_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let relative = entry.path().strip_prefix(DATA_PATH).unwrap_or(&entry.path())
                    .to_string_lossy().replace("\\", "/");
                documents.push(FileItem { name, is_dir: false, path: relative });
            }
        }
        SchoolFiles { images, documents }
    })
    .await
    .unwrap_or(SchoolFiles { images: vec![], documents: vec![] })
    .into()
}

async fn get_teacher_history(
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let name = params.get("name").cloned().unwrap_or_default();
    let school = params.get("school").cloned().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT tk.id, t.course, t.level, tk.ticket_no, tk.start_date, tk.end_date, tk.note, tk.coordinator
 FROM teachers t
 JOIN tickets tk ON tk.id = t.ticket_id
 WHERE t.name = ?1 AND t.school = ?2
 ORDER BY tk.id DESC"
        ).unwrap();
        let rows = stmt.query_map([&name, &school], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_,i64>(0)?, "course": row.get::<_,String>(1)?,
                "level": row.get::<_,String>(2)?, "ticket_no": row.get::<_,String>(3)?,
                "start_date": row.get::<_,String>(4)?, "end_date": row.get::<_,String>(5)?, "note": row.get::<_,String>(6)?, "coordinator": row.get::<_,String>(7)?,
            }))
        }).unwrap();
        let history: Vec<_> = rows.filter_map(|r| r.ok()).collect();
        serde_json::json!({ "name": name, "school": school, "history": history })
    })
    .await
    .unwrap_or(serde_json::json!({}))
    .into()
}

async fn search_teacher(Query(params): Query<HashMap<String,String>>) -> Json<Vec<Teacher>> {
    let q = params.get("q").cloned().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open("files.db").unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, school, name, level, course, ticket_id 
             FROM teachers 
             WHERE name LIKE ?1 
             GROUP BY name, school
             ORDER BY name ASC LIMIT 50"
        ).unwrap();
        let rows = stmt.query_map([format!("%{}%", q)], |row| Ok(Teacher {
            id:        row.get(0)?,
            school:    row.get(1)?,
            name:      row.get(2)?,
            level:     row.get(3)?,
            course:    row.get(4)?,
            ticket_id: row.get(5)?,
        })).unwrap();
        rows.filter_map(|r| r.ok()).collect::<Vec<_>>()
    }).await.unwrap_or_default().into()
}