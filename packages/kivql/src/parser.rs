use crate::tokenizer::{Keyword, Token};
use thiserror::Error;

#[derive(Debug)]
pub enum Operation {
    SET(Set),
    DELETE(Delete),
    GET(Get),
}

#[derive(Debug)]
pub struct Set {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct Delete {
    pub key: String,
}

#[derive(Debug)]
pub struct Get {
    pub key: String,
}

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("no key provided for SET operation")]
    SetNoKey,
    #[error("no value provided for SET operation")]
    SetNoValue,
    #[error("no TO after SET operation key")]
    SetNoTo,
    #[error("expected operation before anything else")]
    OperationFirst,
    #[error("unexpected operation")]
    UnexpectedOperation,
    #[error("empty statement")]
    EmptyStatement,
    #[error("no key provided for DELETE operation")]
    DeleteNoKey,
    #[error("no key provided for GET operation")]
    GetNoKey,
}

pub struct Parser {}

impl Parser {
    pub fn parse(tokens: Vec<Token>) -> Result<Operation, ParserError> {
        let operation = if let Some(op) = tokens.get(0) {
            op
        } else {
            return Err(ParserError::EmptyStatement);
        };

        match operation {
            Token::Keyword(keyword) => match keyword {
                Keyword::SET => {
                    let tokens: Vec<&Token> = tokens
                        .iter()
                        .filter(|token| token != &&Token::Whitespace)
                        .collect();

                    let key = match tokens.get(1) {
                        Some(Token::String(k)) => k,
                        _ => return Err(ParserError::SetNoKey),
                    };

                    match tokens.get(2) {
                        Some(Token::Keyword(Keyword::TO)) => {}
                        _ => return Err(ParserError::SetNoTo),
                    }

                    let value = match tokens.get(3) {
                        Some(Token::String(v)) => v,
                        _ => return Err(ParserError::SetNoValue),
                    };

                    return Ok(Operation::SET(Set {
                        key: key.to_owned(),
                        value: value.to_owned(),
                    }));
                }
                Keyword::DELETE => {
                    let tokens: Vec<&Token> = tokens
                        .iter()
                        .filter(|token| token != &&Token::Whitespace)
                        .collect();

                    let key = match tokens.get(1) {
                        Some(Token::String(k)) => k,
                        _ => return Err(ParserError::DeleteNoKey),
                    };

                    return Ok(Operation::DELETE(Delete {
                        key: key.to_owned(),
                    }));
                }
                Keyword::GET => {
                    let tokens: Vec<&Token> = tokens
                        .iter()
                        .filter(|token| token != &&Token::Whitespace)
                        .collect();

                    let key = match tokens.get(1) {
                        Some(Token::String(k)) => k,
                        _ => return Err(ParserError::GetNoKey),
                    };

                    return Ok(Operation::GET(Get {
                        key: key.to_owned(),
                    }));
                }
                _ => return Err(ParserError::UnexpectedOperation),
            },
            _ => return Err(ParserError::OperationFirst),
        }
    }
}
