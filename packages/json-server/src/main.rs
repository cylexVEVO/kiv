// basic implementation of a JSON server for kiv

use axum::{extract::State, http::StatusCode, routing::post, Router};
use clap::Parser;
use core::{GetResult, Kiv, KivError, OperationResult, OperationResultResult};
use kivql::parser::ParserError;
use kivql::tokenizer::TokenizerError;
use serde::Serialize;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    db_path: PathBuf,
    #[arg(short, long, value_name = "PORT", default_value_t = 7312)]
    port: u16,
}

struct AppState {
    kiv: Kiv,
}

#[derive(Serialize)]
#[serde(remote = "KivError")]
enum KivErrorP {
    #[serde(rename = "tokenizerError")]
    TokenizerError(#[serde(with = "TokenizerErrorP")] TokenizerError),
    #[serde(rename = "parserError")]
    ParserError(#[serde(with = "ParserErrorP")] ParserError),
}

#[derive(Serialize)]
struct KivErrorPW(#[serde(with = "KivErrorP")] KivError);

#[derive(Serialize)]
#[serde(remote = "TokenizerError")]
enum TokenizerErrorP {
    #[serde(rename = "unknownKeyword")]
    UnknownKeyword(String),
}

#[derive(Serialize)]
#[serde(remote = "ParserError")]
enum ParserErrorP {
    #[serde(rename = "setNoKey")]
    SetNoKey,
    #[serde(rename = "setNoValue")]
    SetNoValue,
    #[serde(rename = "setNoTo")]
    SetNoTo,
    #[serde(rename = "operationFirst")]
    OperationFirst,
    #[serde(rename = "unexpectedOperation")]
    UnexpectedOperation,
    #[serde(rename = "emptyStatement")]
    EmptyStatement,
    #[serde(rename = "deleteNoKey")]
    DeleteNoKey,
    #[serde(rename = "getNoKey")]
    GetNoKey,
}

#[derive(Serialize)]
#[serde(remote = "OperationResult")]
struct OperationResultP {
    time: Duration,
    #[serde(with = "OperationResultResultP")]
    result: OperationResultResult,
}

#[derive(Serialize)]
struct OperationResultPW(#[serde(with = "OperationResultP")] OperationResult);

#[derive(Serialize)]
#[serde(remote = "OperationResultResult")]
pub enum OperationResultResultP {
    #[serde(rename = "set")]
    Set,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "get")]
    Get(#[serde(with = "GetResultP")] GetResult),
}

#[derive(Serialize)]
#[serde(remote = "GetResult")]
pub struct GetResultP {
    pub value: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let kiv = match Kiv::open(args.db_path) {
        Ok(kiv) => kiv,
        Err(err) => {
            eprintln!("Error opening database:");
            eprintln!("{:?}", err);
            std::process::exit(1);
        }
    };

    let shared_state = Arc::new(Mutex::new(AppState { kiv }));

    let app = Router::new()
        .route("/exec", post(exec))
        .with_state(shared_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn exec(
    State(state): State<Arc<Mutex<AppState>>>,
    body: String,
) -> axum::http::Response<String> {
    match state.lock().unwrap().kiv.exec(body) {
        Ok(res) => {
            return axum::http::Response::builder()
                .header("content-type", "application/json")
                .status(StatusCode::OK)
                .body(serde_json::to_string(&OperationResultPW(res)).unwrap())
                .unwrap();
        }
        Err(err) => {
            return axum::http::Response::builder()
                .header("content-type", "application/json")
                .status(StatusCode::BAD_REQUEST)
                .body(serde_json::to_string(&KivErrorPW(err)).unwrap())
                .unwrap();
        }
    }
}
