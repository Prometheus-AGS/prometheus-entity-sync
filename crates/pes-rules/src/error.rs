//! Errors produced while parsing and validating `sync-rules.toml`.

use std::path::PathBuf;

/// An error while parsing or validating a `sync-rules.toml` document.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParseError {
    /// The file at the given path could not be read.
    #[error("failed to read {path}: {source}")]
    Io {
        /// The path that could not be read.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The TOML document was syntactically invalid.
    #[error("TOML syntax error{}: {message}", line_col_suffix(*line, *column))]
    Syntax {
        /// The error message from the TOML parser.
        message: String,
        /// 1-based line number, if the parser reported one.
        line: Option<usize>,
        /// 1-based column number, if the parser reported one.
        column: Option<usize>,
    },
    /// The document parsed syntactically but failed semantic validation.
    #[error("validation error in bucket '{bucket_id}': {message}")]
    Validation {
        /// The bucket id the error was found in.
        bucket_id: String,
        /// A human-readable description of the validation failure.
        message: String,
    },
}

fn line_col_suffix(line: Option<usize>, column: Option<usize>) -> String {
    match (line, column) {
        (Some(l), Some(c)) => format!(" at line {l}, column {c}"),
        (Some(l), None) => format!(" at line {l}"),
        _ => String::new(),
    }
}

impl ParseError {
    /// Build a [`ParseError::Syntax`] with precise line/column computed
    /// against the original `source` text.
    ///
    /// `toml::de::Error` only exposes a byte span, not line/column, so the
    /// original source text is required to resolve a human-readable
    /// position.
    pub fn from_toml_error(err: toml::de::Error, source: &str) -> Self {
        let (line, column) = err
            .span()
            .map(|span| offset_to_line_col(source, span.start))
            .unwrap_or((None, None));
        ParseError::Syntax {
            message: err.message().to_string(),
            line,
            column,
        }
    }
}

fn offset_to_line_col(source: &str, offset: usize) -> (Option<usize>, Option<usize>) {
    if offset > source.len() {
        return (None, None);
    }
    let mut line = 1usize;
    let mut col = 1usize;
    for ch in source[..offset].chars() {
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (Some(line), Some(col))
}
