use polars_core::prelude::Result as PolarsResult;
use polars_core::prelude::DataFrame;
use polars_core::prelude::groupby::GroupBy;
use polars_io::csv::CsvReader;
use polars_io::SerReader;

use rhai::{Engine, EvalAltResult, ImmutableString, NativeCallContext};

#[derive(Debug, Clone)]
pub struct JsDataFrame {
    pub df: DataFrame,
}

impl JsDataFrame {
    ///
    ///
    ///
    fn read_csv(path:String) -> Self {
        println!("{}",path);
        Self { df: CsvReader::from_path(&path).unwrap().has_header(true).finish().unwrap() }
    }

    ///
    ///
    ///
    fn group_by(self,aggType:String,by:String,selection:String)->Self{
        let group_by = self.df.groupby([by]).unwrap().select([selection]);
        Self{df:do_aggregate(Some(&aggType),group_by)}
    }
}
///
///
///
fn do_aggregate(agg_type:Option<&str>, group_by: GroupBy) -> DataFrame {
    match agg_type{
        Some("count") => group_by.count(),
        Some("sum") => group_by.sum(),
        Some("first") => group_by.first(),
        Some("last") => group_by.last(),
        Some("max") => group_by.max(),
        Some("min") => group_by.min(),
        Some("std") => group_by.std(),
        _ => group_by.count()
    }.unwrap()
}

///
///
///
pub fn eval_df_script(script:&str)->Result<JsDataFrame, Box<EvalAltResult>>{
    let mut engine = Engine::new();
    engine.register_fn("readCsv", JsDataFrame::read_csv)
        .register_fn("groupBy",JsDataFrame::group_by);

    let result = engine.eval(script);

    result
}
