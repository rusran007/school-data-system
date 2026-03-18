use axum::{
    routing::get,
    Router,
    Json,
    extract::Query,
    response::IntoResponse,
};
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tower_http::services::ServeDir;
use walkdir::WalkDir;

const DATA_PATH: &str = "D:/DATA_Q-Junier";


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

async fn reindex() -> impl IntoResponse {
    index_files();
    "Re-index complete"
}

async fn list_files(Query(params): Query<HashMap<String,String>>) -> Json<Vec<FileItem>> {

    let path = params.get("path").cloned().unwrap_or_default();

    let full_path = if path.is_empty() {
        DATA_PATH.to_string()
    } else {
        format!("{}/{}", DATA_PATH, path)
    };

    let mut items = Vec::new();

    if let Ok(entries) = fs::read_dir(full_path) {

        for entry in entries {

            if let Ok(entry) = entry {

                let file_name = entry.file_name().to_string_lossy().to_string();

                let is_dir = entry.path().is_dir();

                let path_obj = entry.path();

                items.push(FileItem{
                    name: file_name,
                    is_dir: is_dir,
                    path: path_obj
                        .strip_prefix(DATA_PATH)
                        .unwrap()
                        .to_string_lossy()
                        .replace("\\","/")
                });

            }

        }

    }

    Json(items)
}

#[tokio::main]
async fn main() {

    init_db();
    index_files();

    let app = Router::new()

        .route("/api/list", get(list_files))
        .route("/api/search", get(search_files))
        .route("/api/reindex", get(reindex))

        // สำคัญมาก
        .nest_service("/files", ServeDir::new(DATA_PATH))

        .nest_service("/", ServeDir::new("static"));

    println!("Server running: http://localhost:3000");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn search_files(Query(params): Query<HashMap<String,String>>) -> Json<Vec<FileItem>> {

    let q = params.get("q").cloned().unwrap_or_default();

    let conn = Connection::open("files.db").unwrap();

    let mut stmt = conn.prepare(
        "SELECT name, path, is_dir FROM files WHERE name LIKE ?1 ORDER BY is_dir DESC, name ASC LIMIT 100"
    ).unwrap();

    let rows = stmt.query_map(
        [format!("%{}%", q)],
        |row| {
            Ok(FileItem {
                name: row.get(0)?,
                path: row.get(1)?,
                is_dir: row.get::<_, i32>(2)? == 1,
            })
        }
    ).unwrap();

    let mut results = Vec::new();

    for item in rows {
        results.push(item.unwrap());
    }

    Json(results)
}

use rusqlite::{Connection, params};

fn init_db() {

    let conn = Connection::open("files.db").unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            name TEXT,
            path TEXT,
            is_dir INTEGER
        )",
        [],
    ).unwrap();

}

fn index_files() {

    let conn = Connection::open("files.db").unwrap();

    conn.execute("DELETE FROM files", []).unwrap();

    for entry in WalkDir::new(DATA_PATH).into_iter().filter_map(|e| e.ok()) {

        let path = entry.path();

        let name = path.file_name().unwrap().to_string_lossy().to_string();

        let rel_path = path.strip_prefix(DATA_PATH)
            .unwrap()
            .to_string_lossy()
            .replace("\\","/");

        let is_dir = path.is_dir() as i32;

        conn.execute(
            "INSERT INTO files (name, path, is_dir) VALUES (?1, ?2, ?3)",
            params![name, rel_path, is_dir],
        ).unwrap();

    }

    println!("Index complete!");
}