use polars_core::prelude::AnyValue;
use sqlx::{Column, Row, TypeInfo};
use sqlx::sqlite::{SqliteColumn, SqliteRow};

pub(crate) struct SqlRowProcessor {
    columns: Option<Vec<SqliteColumn>>,
}

///
///
///
impl SqlRowProcessor {

    pub fn new() -> Self {
        SqlRowProcessor {
            columns:None
        }
    }

    ///
    ///
    ///
    fn caching(&mut self,sql_row:&SqliteRow){
        if self.columns.is_none(){
            self.columns = Some(sql_row.columns().iter().map(|column|column.clone()).collect());
        }
    }

    ///
    ///
    ///
    pub fn get_columns(&mut self) -> Option<&mut Vec<SqliteColumn>> {
        self.columns.as_mut()
    }

    ///
    ///
    ///
    pub fn process(&mut self, row:&SqliteRow) -> Vec<AnyValue<'static>>{
        //
        self.caching(row);

        let len = row.len();
        let mut values:Vec<AnyValue> = Vec::with_capacity(len);

        for i in 0..len {
            let column = row.column(i);
            let data_type = column.type_info().name();

            match data_type {
                "REAL"=>{
                    let float_value = row.try_get::<f64, &str>(column.name());
                    values.push(AnyValue::Float64(float_value.unwrap()));
                }
                "INTEGER"=>{
                    let value = row.try_get::<i32, &str>(column.name());
                    values.push(AnyValue::Int32(value.unwrap()));
                }
                "INT8"|"BIGINT"=>{
                    let i8_value = row.try_get::<i64, &str>(column.name());
                    values.push(AnyValue::Int64(i8_value.unwrap()));
                }
                "BOOLEAN"=>{
                    let bool_value = row.try_get::<bool, &str>(column.name());
                    values.push(AnyValue::Boolean(bool_value.unwrap()));
                }
                "TEXT"|"VARCHAR"|"CHAR(N)" =>{
                    let text_value = row.try_get::<&str, &str>(column.name()).unwrap();
                    values.push(AnyValue::Utf8Owned(String::from(text_value)));
                }
                _=>{
                    values.push(AnyValue::Null);
                }
            }
        }

        values
    }
}