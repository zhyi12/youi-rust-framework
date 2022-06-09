use polars_core::prelude::{AnyValue, DataFrame, Series, NamedFromOwned};
use sqlx::{Column, Error, Pool, Sqlite, TypeInfo};
use sqlx::sqlite::{SqliteColumn, SqliteRow};
use crate::sqlite_dataframe::processor::SqlRowProcessor;

///
/// 从sql中读取dataframe
///
pub async fn read_sql(pool:& Pool<Sqlite>, sql:&str) -> Result<DataFrame,Error> {
    let mut conn = pool.acquire().await?;

    let mut row_processor = SqlRowProcessor::new();

    //行数据
    let row_data_arr:Vec<Vec<AnyValue>> = sqlx::query(sql)
        .map(|row:SqliteRow|row_processor.process(&row))
        .fetch_all(&mut conn).await?;

    let columns = row_processor.get_columns().unwrap();

    let mut columns_series:Vec<Series> = Vec::with_capacity(columns.len());
    for(idx,column) in columns.into_iter().enumerate(){
        columns_series.push(build_columns_series(column,&row_data_arr,idx));
    }

    Ok(DataFrame::new(columns_series).unwrap())
}
///
///
///
fn build_columns_series(column:&SqliteColumn,row_data_arr:&Vec<Vec<AnyValue>>,idx:usize)->Series{
    let data_type = column.type_info().name();
    println!("data_type {}",data_type);
    let column_name = column.name();
    match data_type {
        "REAL"=>{
            let data_list:Vec<f64> = row_data_arr.iter().map(|row_data|
                row_data.get(idx).unwrap().extract().unwrap()).collect();
            return Series::from_vec(column_name,data_list);
        }
        "INTEGER"|"INT8"|"BIGINT"=>{
            let data_list:Vec<i64> = row_data_arr.iter().map(|row_data|
                row_data.get(idx).unwrap().extract().unwrap()).collect();
            return Series::from_vec(column_name,data_list);
        }
        "TEXT"|"VARCHAR"|"CHAR(N)"=>{
            let data_list:Vec<String> = row_data_arr.iter().map(|row_data|{
                let value = row_data.get(idx).unwrap();
                match value {
                    AnyValue::Utf8Owned(v)=>{
                        return String::from(v);
                    }
                    _ => {
                        String::new()
                    }
                }
            }).collect();
            let mut s: Series =Series::from_iter(data_list);
            s.rename(column_name);
            return s;
        }
        _ => {
            let data_list:Vec<String> = row_data_arr.iter().map(|_|String::new()).collect();
            let mut s: Series =Series::from_iter(data_list);
            s.rename(column_name);
            s
        }
    }
}