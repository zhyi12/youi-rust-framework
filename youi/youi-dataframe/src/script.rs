
use polars_core::prelude::{PolarsError, Result as PolarsResult};
use polars_core::prelude::DataFrame;
use polars_core::prelude::groupby::GroupBy;
use polars_io::csv::CsvReader;
use polars_io::SerReader;

use rhai::{Engine, EvalAltResult, Dynamic};
use rhai::serde::from_dynamic;

#[derive(Debug, Clone)]
pub struct JsDataFrame {
    pub df: DataFrame,
}

#[derive(Debug,Clone, serde::Deserialize)]
struct Cube{
    group_names:Vec<String>,
    measure_items:Vec<MeasureItem>
}

#[derive(Debug,Clone, serde::Deserialize)]
struct MeasureItem{
    name:String,
    aggregates:Vec<String>
}

#[derive(Debug,Clone, serde::Deserialize)]
struct Sort{
    name:String,
    reverse:bool
}

#[derive(Debug,Clone, serde::Deserialize)]
struct Sorts{
    sorts:Vec<Sort>
}

impl JsDataFrame {
    ///
    /// read csv from path
    ///
    fn read_csv(path:String) -> Self {
        println!("{}",path);
        Self { df: CsvReader::from_path(&path).unwrap().has_header(true).finish().unwrap() }
    }

    ///
    /// df group by
    ///
    fn group_by(self,agg_type:String,by:String,selection:String)->Self{
        let group_names:Vec<_> = by.split(",").collect();
        let measure_names:Vec<_> = selection.split(",").collect();
        let group_by = self.df.groupby(group_names).unwrap().select(measure_names);

        Self{df:finish_group_by(group_by,&agg_type,).unwrap()}
    }

    ///
    /// cube query
    ///
    fn cube_query(self,param:Dynamic) -> Self{
        let cube:Cube = from_dynamic(&param).unwrap();

        let agg_arr:Vec<_> =
            cube.measure_items.iter().map(|measure_item:&MeasureItem|{
                let clone_item:MeasureItem = measure_item.clone();
                (clone_item.name,clone_item.aggregates)
            }).collect();

        let df = self.df.groupby(cube.group_names).unwrap().agg(&agg_arr).unwrap();

        Self{df}
    }

    ///
    /// fliter
    ///
    fn filter(self) ->Self{
        Self{df:self.df}
    }

    ///
    /// 排序
    ///
    fn sort(self,sorts:Dynamic) ->Self{
        let sorts:Sorts = from_dynamic(&sorts).unwrap();

        let by_column:Vec<_> = sorts.sorts.iter().map(|sort|sort.clone().name).collect();
        let reverse:Vec<_> = sorts.sorts.iter().map(|sort|sort.clone().reverse).collect();

        Self {df:self.df.sort(by_column,reverse).unwrap()}
    }
}

///
///
///
fn finish_group_by(gb: GroupBy, agg: &str) -> PolarsResult<DataFrame> {
    match agg {
        "min" => gb.min(),
        "max" => gb.max(),
        "mean" => gb.mean(),
        "first" => gb.first(),
        "last" => gb.last(),
        "sum" => gb.sum(),
        "count" => gb.count(),
        "n_unique" => gb.n_unique(),
        "median" => gb.median(),
        "agg_list" => gb.agg_list(),
        "groups" => gb.groups(),
        "std" => gb.std(),
        "var" => gb.var(),
        a => Err(PolarsError::ComputeError(
            format!("agg fn {} does not exists", a).into(),
        )),
    }
}

///
/// eval df script by rhai engine
///
pub fn eval_df_script(script:&str)->Result<JsDataFrame, Box<EvalAltResult>>{
    let mut engine = Engine::new();
    engine.register_fn("readCsv", JsDataFrame::read_csv)
        .register_fn("cubeQuery",JsDataFrame::cube_query)
        .register_fn("sort",JsDataFrame::sort)
        .register_fn("filter",JsDataFrame::filter)
        .register_fn("groupBy",JsDataFrame::group_by);

    let result = engine.eval(script);

    result
}
