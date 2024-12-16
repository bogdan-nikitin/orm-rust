use crate::{
    data::{DataType, Value},
    error::{map_rusqlite_error, map_rusqlite_error_with_id, Result},
    object::Schema,
    ObjectId,
};

use rusqlite::ToSql;

////////////////////////////////////////////////////////////////////////////////

pub type Row<'a> = Vec<Value<'a>>;
pub type RowSlice<'a> = [Value<'a>];

////////////////////////////////////////////////////////////////////////////////

fn list_fields(schema: &Schema) -> String {
    schema
        .fields
        .iter()
        .map(|f| f.column_name)
        .collect::<Vec<_>>()
        .join(",")
}

fn row_to_parameters<'a>(row: &'a RowSlice) -> Vec<&'a dyn ToSql> {
    row.iter()
        .map(|v| match &v {
            Value::String(x) => x as &dyn ToSql,
            Value::Bytes(x) => x as &dyn ToSql,
            Value::Int64(x) => x as &dyn ToSql,
            Value::Float64(x) => x as &dyn ToSql,
            Value::Bool(x) => x as &dyn ToSql,
        })
        .collect::<Vec<_>>()
}

pub(crate) trait StorageTransaction {
    fn table_exists(&self, table: &str) -> Result<bool>;
    fn create_table(&self, schema: &Schema) -> Result<()>;

    fn insert_row(&self, schema: &Schema, row: &RowSlice) -> Result<ObjectId>;
    fn update_row(&self, id: ObjectId, schema: &Schema, row: &RowSlice) -> Result<()>;
    fn select_row(&self, id: ObjectId, schema: &Schema) -> Result<Row<'static>>;
    fn delete_row(&self, id: ObjectId, schema: &Schema) -> Result<()>;

    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
}

impl<'a> StorageTransaction for rusqlite::Transaction<'a> {
    fn table_exists(&self, table: &str) -> Result<bool> {
        let mut stmt =
            self.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1")?;
        Ok(stmt.exists([table])?)
    }

    fn create_table(&self, schema: &Schema) -> Result<()> {
        let fields = ["id INTEGER PRIMARY KEY AUTOINCREMENT".to_string()]
            .into_iter()
            .chain(schema.fields.iter().map(|f| f.get_create_sql()))
            .collect::<Vec<_>>()
            .join(",");
        self.execute(
            format!("CREATE TABLE {}({})", schema.table_name, fields).as_str(),
            [],
        )?;
        Ok(())
    }

    fn insert_row(&self, schema: &Schema, row: &RowSlice) -> Result<ObjectId> {
        let placeholders = (1..=row.len())
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(",");
        let sql = if schema.fields.is_empty() {
            format!("INSERT INTO {} DEFAULT VALUES", schema.table_name)
        } else {
            format!(
                "INSERT INTO {}({}) VALUES({})",
                schema.table_name,
                list_fields(schema),
                placeholders
            )
        };
        self.execute(sql.as_str(), row_to_parameters(row).as_slice())
            .map_err(|e| map_rusqlite_error(e, schema))?;
        Ok(self.last_insert_rowid().into())
    }

    fn update_row(&self, id: ObjectId, schema: &Schema, row: &RowSlice) -> Result<()> {
        if schema.fields.is_empty() {
            return Ok(());
        }
        let mut parameters = row_to_parameters(row);
        let id_parameter = &id.into_i64();
        parameters.push(id_parameter);
        let set_sql = (1..=row.len())
            .zip(schema.fields)
            .map(|(i, f)| format!("{} = ?{}", f.column_name, i))
            .chain(["id = id".to_string()])
            .collect::<Vec<_>>()
            .join(",");
        self.execute(
            format!(
                "UPDATE {} SET {} WHERE id = ?{}",
                schema.table_name,
                set_sql,
                parameters.len()
            )
            .as_str(),
            parameters.as_slice(),
        )?;
        Ok(())
    }

    fn select_row(&self, id: ObjectId, schema: &Schema) -> Result<Row<'static>> {
        let map_err = |e| map_rusqlite_error_with_id(e, schema, id);
        let mut stmt = self
            .prepare(
                format!(
                    "SELECT {} FROM {} WHERE id = ?1",
                    if schema.fields.is_empty() {
                        "1".to_string()
                    } else {
                        list_fields(schema)
                    },
                    schema.table_name
                )
                .as_str(),
            )
            .map_err(map_err)?;
        stmt.query_row([id.into_i64()], |row| {
            schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    Ok(match f.data_type {
                        DataType::String => Value::String(row.get::<_, String>(i)?.into()),
                        DataType::Bytes => Value::Bytes(row.get::<_, Vec<u8>>(i)?.into()),
                        DataType::Int64 => Value::Int64(row.get::<_, i64>(i)?),
                        DataType::Float64 => Value::Float64(row.get::<_, f64>(i)?),
                        DataType::Bool => Value::Bool(row.get::<_, bool>(i)?),
                    })
                })
                .collect()
        })
        .map_err(map_err)
    }

    fn delete_row(&self, id: ObjectId, schema: &Schema) -> Result<()> {
        self.execute(
            format!("DELETE FROM {} WHERE id = ?1", schema.table_name).as_str(),
            [id.into_i64()],
        )?;
        Ok(())
    }

    fn commit(&self) -> Result<()> {
        self.execute("COMMIT", [])?;
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        self.execute("ROLLBACK", [])?;
        Ok(())
    }
}
