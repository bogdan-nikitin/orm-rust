use crate::{data::DataType, object::Schema, ObjectId};

use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    NotFound(Box<NotFoundError>),
    #[error(transparent)]
    UnexpectedType(Box<UnexpectedTypeError>),
    #[error(transparent)]
    MissingColumn(Box<MissingColumnError>),
    #[error("database is locked")]
    LockConflict,
    #[error("storage error: {0}")]
    Storage(#[source] Box<dyn std::error::Error>),
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::DatabaseBusy,
                    extended_code: _,
                },
                _,
            ) => Error::LockConflict,
            e => Error::Storage(Box::new(e)),
        }
    }
}

fn find_column_name(msg: &str) -> Option<&str> {
    if let Some(column_name) = msg.strip_prefix("no such column: ") {
        Some(column_name)
    } else if msg.contains("has no column named ") {
        Some(&msg[msg.rfind(' ').unwrap() + 1..])
    } else {
        None
    }
}

pub fn map_rusqlite_error(err: rusqlite::Error, schema: &Schema) -> Error {
    match err {
        rusqlite::Error::InvalidColumnType(column_index, _, got_type) => {
            let field = &schema.fields[column_index];
            Error::UnexpectedType(Box::new(UnexpectedTypeError {
                type_name: schema.type_name,
                attr_name: field.attr_name,
                table_name: schema.table_name,
                column_name: field.column_name,
                expected_type: field.data_type,
                got_type: got_type.to_string(),
            }))
        }
        rusqlite::Error::SqliteFailure(_, Some(msg)) if { find_column_name(&msg).is_some() } => {
            let column_name = find_column_name(&msg).unwrap();
            let field = schema
                .fields
                .iter()
                .find(|f| f.column_name == column_name)
                .unwrap();
            Error::MissingColumn(Box::new(MissingColumnError {
                type_name: schema.type_name,
                attr_name: field.attr_name,
                table_name: schema.table_name,
                column_name: field.column_name,
            }))
        }
        e => e.into(),
    }
}

pub fn map_rusqlite_error_with_id(err: rusqlite::Error, schema: &Schema, id: ObjectId) -> Error {
    match err {
        rusqlite::Error::QueryReturnedNoRows => Error::NotFound(Box::new(NotFoundError {
            object_id: id,
            type_name: schema.type_name,
        })),
        e => map_rusqlite_error(e, schema),
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("object is not found: type '{type_name}', id {object_id:?}")]
pub struct NotFoundError {
    pub object_id: ObjectId,
    pub type_name: &'static str,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "invalid type for {type_name}::{attr_name}: expected equivalent of {expected_type:?}, \
    got {got_type} (table: {table_name}, column: {column_name})"
)]
pub struct UnexpectedTypeError {
    pub type_name: &'static str,
    pub attr_name: &'static str,
    pub table_name: &'static str,
    pub column_name: &'static str,
    pub expected_type: DataType,
    pub got_type: String,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "missing a column for {type_name}::{attr_name} \
    (table: {table_name}, column: {column_name})"
)]
pub struct MissingColumnError {
    pub type_name: &'static str,
    pub attr_name: &'static str,
    pub table_name: &'static str,
    pub column_name: &'static str,
}

////////////////////////////////////////////////////////////////////////////////

pub type Result<T> = std::result::Result<T, Error>;
