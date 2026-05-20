use std::{env, error, path::PathBuf};

use waw_server::http::serve_sqlite;

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let db_path = db_path_from_args();
    let addr = listen_addr_from_env();
    serve_sqlite(&db_path, &addr).await?;
    Ok(())
}

fn db_path_from_args() -> PathBuf {
    env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/graph.db"))
}

fn listen_addr_from_env() -> String {
    let port = env::var("PORT").unwrap_or_else(|_| "5177".to_string());
    env::var("GRAPH_LISTEN_ADDR").unwrap_or_else(|_| format!("127.0.0.1:{port}"))
}
