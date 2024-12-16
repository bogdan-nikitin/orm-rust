use crate::{data::DataType, storage::Row};

use std::any::Any;

////////////////////////////////////////////////////////////////////////////////

pub trait Object: Any {
    fn from_row(row: Row) -> Self;
    fn to_row(&self) -> Row;
    const SCHEMA: Schema;
}

////////////////////////////////////////////////////////////////////////////////

pub struct Field {
    pub column_name: &'static str,
    pub data_type: DataType,
    pub attr_name: &'static str,
}

impl Field {
    pub fn get_create_sql(&self) -> String {
        format!(
            "{} {}",
            self.column_name,
            match self.data_type {
                DataType::String => "TEXT",
                DataType::Bytes => "BLOB",
                DataType::Int64 => "BIGINT",
                DataType::Float64 => "REAL",
                DataType::Bool => "TINYINT",
            }
        )
    }
}

pub struct Schema {
    pub table_name: &'static str,
    pub fields: &'static [Field],
    pub type_name: &'static str,
}

pub trait Store: Any {
    fn to_row(&self) -> Row;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn get_schema(&self) -> &'static Schema;
}

impl<T: Object> Store for T {
    fn to_row(&self) -> Row {
        T::to_row(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_schema(&self) -> &'static Schema {
        &T::SCHEMA
    }
}
