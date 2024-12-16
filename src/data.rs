use std::borrow::Cow;

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct ObjectId(i64);

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    String,
    Bytes,
    Int64,
    Float64,
    Bool,
}

pub trait ToDataType {
    const DATA_TYPE: DataType;
}

impl ToDataType for String {
    const DATA_TYPE: DataType = DataType::String;
}

impl ToDataType for Vec<u8> {
    const DATA_TYPE: DataType = DataType::Bytes;
}

impl ToDataType for i64 {
    const DATA_TYPE: DataType = DataType::Int64;
}

impl ToDataType for f64 {
    const DATA_TYPE: DataType = DataType::Float64;
}

impl ToDataType for bool {
    const DATA_TYPE: DataType = DataType::Bool;
}

////////////////////////////////////////////////////////////////////////////////

pub enum Value<'a> {
    String(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

impl ObjectId {
    pub fn into_i64(&self) -> i64 {
        self.0
    }
}

impl From<i64> for ObjectId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl<'a> From<&'a String> for Value<'a> {
    fn from(value: &'a String) -> Self {
        Value::String(value.into())
    }
}

impl<'a> From<&'a Vec<u8>> for Value<'a> {
    fn from(value: &'a Vec<u8>) -> Self {
        Value::Bytes(value.into())
    }
}

impl<'a> From<&'a i64> for Value<'a> {
    fn from(value: &'a i64) -> Self {
        Value::Int64(*value)
    }
}

impl<'a> From<&'a f64> for Value<'a> {
    fn from(value: &'a f64) -> Self {
        Value::Float64(*value)
    }
}

impl<'a> From<&'a bool> for Value<'a> {
    fn from(value: &'a bool) -> Self {
        Value::Bool(*value)
    }
}

impl<'a> From<Value<'a>> for String {
    fn from(value: Value<'a>) -> Self {
        match value {
            Value::String(x) => x.into_owned(),
            _ => panic!("Expected orm::data::Value::String"),
        }
    }
}

impl<'a> From<Value<'a>> for Vec<u8> {
    fn from(value: Value<'a>) -> Self {
        match value {
            Value::Bytes(x) => x.into_owned(),
            _ => panic!("Expected orm::data::Value::Bytes"),
        }
    }
}

impl<'a> From<Value<'a>> for i64 {
    fn from(value: Value<'a>) -> Self {
        match value {
            Value::Int64(x) => x,
            _ => panic!("Expected orm::data::Value::Int64"),
        }
    }
}

impl<'a> From<Value<'a>> for f64 {
    fn from(value: Value<'a>) -> Self {
        match value {
            Value::Float64(x) => x,
            _ => panic!("Expected orm::data::Value::Float64"),
        }
    }
}

impl<'a> From<Value<'a>> for bool {
    fn from(value: Value<'a>) -> Self {
        match value {
            Value::Bool(x) => x,
            _ => panic!("Expected orm::data::Value::Bool"),
        }
    }
}
