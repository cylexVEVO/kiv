// core kiv implementation

use kivql::{
    parser::{Operation, Parser, ParserError},
    tokenizer::{Tokenizer, TokenizerError},
};
use std::{
    io,
    path::PathBuf,
    time::{Duration, Instant},
};
use storage::Storage;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KivError {
    #[error("tokenizer error")]
    TokenizerError(#[from] TokenizerError),
    #[error("parser error")]
    ParserError(#[from] ParserError),
}

#[derive(Error, Debug)]
pub enum KivOpenError {
    #[error("io error")]
    IoError(#[from] io::Error),
}

#[derive(Debug)]
pub struct OperationResult {
    pub time: Duration,
    pub result: OperationResultResult,
}

#[derive(Debug)]
pub enum OperationResultResult {
    Set,
    Delete,
    Get(GetResult),
}

#[derive(Debug)]
pub struct GetResult {
    pub value: Option<String>,
}

pub struct Kiv {
    tokenizer: Tokenizer,
    storage: Storage,
}
impl Kiv {
    pub fn open(path: PathBuf) -> Result<Self, KivOpenError> {
        Ok(Self {
            tokenizer: Tokenizer::new(),
            storage: Storage::open(path.to_string_lossy().to_string())
                .expect("Failed to initialize storage"),
        })
    }

    pub fn exec(&mut self, statement: String) -> Result<OperationResult, KivError> {
        let tokens = self.tokenizer.tokenize(statement)?;
        let operation = Parser::parse(tokens)?;

        let start = Instant::now();
        #[allow(unused_assignments)]
        let mut result: OperationResultResult = OperationResultResult::Set;

        match &operation {
            Operation::SET(set) => {
                // see if we need to write or update entry
                let entry = self
                    .storage
                    .get_data_entry(&set.key)
                    .expect("unknown error");
                if entry.is_some() {
                    // update
                    self.storage
                        .update_data_entry(&set.key, &set.value)
                        .expect("unknown error");
                } else {
                    // write
                    self.storage
                        .write_data_entry(&set.key, &set.value)
                        .expect("unknown error");
                }
                result = OperationResultResult::Set;
            }
            Operation::DELETE(delete) => {
                self.storage
                    .delete_data_entry(&delete.key)
                    .expect("unknown error");
                result = OperationResultResult::Delete;
            }
            Operation::GET(get) => {
                let value = self
                    .storage
                    .get_data_entry(&get.key)
                    .expect("unknown error");
                result = OperationResultResult::Get(GetResult { value });
            }
        }

        let elapsed = start.elapsed();

        return Ok(OperationResult {
            time: elapsed,
            result,
        });
    }
}
