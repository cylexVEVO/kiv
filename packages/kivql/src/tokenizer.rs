use thiserror::Error;

#[derive(Error, Debug)]
pub enum TokenizerError {
    #[error("unknown keyword")]
    UnknownKeyword(String),
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Keyword(Keyword),
    String(String),
    Whitespace,
}

#[derive(Debug, PartialEq)]
pub enum Keyword {
    SET,
    TO,
    DELETE,
    GET,
}

pub struct Tokenizer {
    input: String,
    position: usize,
}

impl Tokenizer {
    pub fn new() -> Self {
        Self {
            input: String::new(),
            position: 0,
        }
    }

    pub fn tokenize(&mut self, statement: String) -> Result<Vec<Token>, TokenizerError> {
        self.input = statement;
        self.position = 0;

        let mut tokens: Vec<Token> = vec![];

        while !self.input_finished() {
            let current_char = self.current_char().unwrap();

            if Tokenizer::is_whitespace(current_char) {
                self.read_until(|char| !Tokenizer::is_whitespace(char));
                tokens.push(Token::Whitespace);
            }

            if Tokenizer::is_alphanumeric(current_char) {
                let keyword = self.read_until(|char| !Tokenizer::is_alphanumeric(char));
                let keyword = match &*keyword.to_uppercase() {
                    "SET" => Token::Keyword(Keyword::SET),
                    "TO" => Token::Keyword(Keyword::TO),
                    "DELETE" => Token::Keyword(Keyword::DELETE),
                    "GET" => Token::Keyword(Keyword::GET),
                    _ => return Err(TokenizerError::UnknownKeyword(keyword)),
                };
                tokens.push(keyword);
            }

            if Tokenizer::is_quote(current_char) {
                self.advance();
                tokens.push(Token::String(self.read_until(Tokenizer::is_quote)));
                self.advance();
            }

            self.advance();
        }

        Ok(tokens)
    }

    fn current_char(&self) -> Option<char> {
        self.input.chars().nth(self.position)
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn read_until<P>(&mut self, predicate: P) -> String
    where
        P: Fn(char) -> bool,
    {
        let mut read = String::new();

        loop {
            let current_char = if let Some(char) = self.current_char() {
                char
            } else {
                break;
            };

            read.push(current_char);

            let next_char = if let Some(char) = self.peek_next() {
                char
            } else {
                break;
            };

            if predicate(next_char) {
                break;
            }

            self.advance();
        }

        read
    }

    fn peek_next(&self) -> Option<char> {
        self.input.chars().nth(self.position + 1)
    }

    fn input_finished(&self) -> bool {
        self.position >= self.input.len()
    }

    fn is_whitespace(char: char) -> bool {
        char == ' ' || char == '\n' || char == '\r' || char == '\t'
    }

    fn is_quote(char: char) -> bool {
        char == '\'' || char == '"'
    }

    fn is_alphanumeric(char: char) -> bool {
        // i like letters :D
        let chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        chars.contains(char)
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::{Keyword, Token, Tokenizer};

    #[test]
    fn strings_are_detected() {
        let expected = vec![
            Token::String(String::from("hello")),
            Token::Whitespace,
            Token::String(String::from("world")),
        ];

        let statement = String::from("\"hello\" \"world\"");

        let tokens = Tokenizer::new().tokenize(statement).unwrap();

        assert_eq!(expected, tokens);
    }

    #[test]
    fn keywords_are_detected() {
        let expected = vec![
            Token::Keyword(Keyword::SET),
            Token::Whitespace,
            Token::Keyword(Keyword::TO),
        ];

        let statement = String::from("SET TO");

        let tokens = Tokenizer::new().tokenize(statement).unwrap();

        assert_eq!(expected, tokens);
    }
}
