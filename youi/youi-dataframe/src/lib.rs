pub mod script;
pub mod lazy;
pub mod transform;

#[cfg(feature = "sqlite_dataframe")]
pub mod sqlite_dataframe;

pub use polars_core::prelude::*;
pub use polars_io::prelude::*;

use crate::lazy::{eval_lazy_script};

extern crate serde_json;
extern crate rhai;

extern crate polars_core;
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
    //脚本转换：四则运算等
    let exec_script = transform::transform(script);
    let js_df = eval_lazy_script(&exec_script);

    match js_df {
        Ok(x) => {
            let result = x.df.collect();
            match result {
                Ok(df) => {
                    return df_to_json(df);
                }
                Err(e) => {
                    println!("df execute error:{:?},\n{}",e,script);
                }
            }
        }
        Err(_) => {
            println!("script parse error:{},\n{}",js_df.err().unwrap(),script);
        }
    }

    String::from("[]")
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
