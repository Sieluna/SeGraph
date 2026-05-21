use std::{env, error, path::PathBuf};

use waw_server::http::serve_sqlite;

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let db_path = db_path_from_args();
    let warm_cache_path = warm_cache_path_from_args();
    let addr = listen_addr_from_env();
    serve_sqlite(&db_path, warm_cache_path.as_deref(), &addr).await?;
    Ok(())
}

fn db_path_from_args() -> PathBuf {
    env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/graph.db"))
}

fn warm_cache_path_from_args() -> Option<PathBuf> {
    env::args_os().nth(2).map(PathBuf::from)
}

fn listen_addr_from_env() -> String {
    let port = env::var("PORT").unwrap_or_else(|_| "5177".to_string());
    env::var("GRAPH_LISTEN_ADDR").unwrap_or_else(|_| format!("127.0.0.1:{port}"))
}
