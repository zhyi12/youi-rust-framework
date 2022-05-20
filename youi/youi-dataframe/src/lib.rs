pub mod script;
pub mod lazy;
pub mod transform;

use polars_core::frame::DataFrame;
use polars_io::prelude::*;
use crate::lazy::eval_lazy_script;

extern crate serde_json;

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
    if js_df.is_err(){
        println!("script parse error:{},\n{}",js_df.err().unwrap(),script);
        String::from("[]")
    }else{
        let err_msg = String::from("error script:")+script;
        let df = js_df.unwrap().df.collect().expect(&err_msg);
        df_to_json(df)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
