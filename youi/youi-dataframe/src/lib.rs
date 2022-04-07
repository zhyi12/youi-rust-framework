pub mod script;
pub mod lazy;
use polars_core::frame::DataFrame;
use polars_io::prelude::*;
use crate::lazy::eval_lazy_script;

///
///
///
pub fn df_to_json(mut df:DataFrame)->String{
    let mut json_buf = Vec::new();
    //将dataFrame写入Vec
    JsonWriter::new(&mut json_buf).with_json_format(JsonFormat::Json)
        .finish(&mut df).expect("json write error");
    //转换为String对象
    let json_str = String::from_utf8(json_buf).unwrap();
    json_str
}
///
///
///
pub fn df_script_executor(script:&str)->String{
    let js_df = eval_lazy_script(script).unwrap();
    let df = js_df.df.collect().unwrap();
    df_to_json(df)
}