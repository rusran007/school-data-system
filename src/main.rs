use axum::{
    routing::get,
    Router,
    Json,
    extract::Query,
};
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tower_http::services::ServeDir;

const DATA_PATH: &str = "D:/DATA_Q-Junier";

#[derive(Serialize)]
struct FileItem {
    name: String,
    is_dir: bool,
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

                items.push(FileItem{
                    name:file_name,
                    is_dir
                });

            }

        }

    }

    Json(items)
}

#[tokio::main]
async fn main() {

    let app = Router::new()

        .route("/api/list", get(list_files))

        // สำคัญมาก
        .nest_service("/files", ServeDir::new(DATA_PATH))

        .nest_service("/", ServeDir::new("static"));

    println!("Server running: http://localhost:3000");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}