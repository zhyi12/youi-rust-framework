
use polars_core::frame::DataFrame;
use polars_core::prelude::PolarsError;
use polars_core::prelude::Result as PolarsResult;
use polars_lazy::dsl::col;
use rhai::plugin::*;
use polars_lazy::prelude::{AggExpr, DynamicGroupOptions, Expr, LazyCsvReader, LazyFrame};
use rhai::serde::from_dynamic;

///
///
///
#[derive(Clone)]
pub struct JsLazyFrame{
    pub df:LazyFrame
}

#[derive(Clone)]
pub struct JsExpr{
    pub expr:Expr,
}

#[derive(Debug,Clone, serde::Deserialize)]
struct AggParam{
    group_names:Vec<String>,
    col_exprs:Vec<ColExpr>
}

#[derive(Debug,Clone, serde::Deserialize)]
struct ColExpr{
    name:String,
    aggregate:String,
}

impl JsLazyFrame {
    ///
    ///
    ///
    fn read_csv(path:String) -> Self {
        let df = LazyCsvReader::new(path).finish().unwrap();
        Self{df}
    }
    ///
    ///
    ///
    fn agg(self,param:Dynamic)->Self{
        let agg_param:AggParam = from_dynamic(&param).unwrap();

        let by:Vec<Expr> = agg_param.group_names.iter().map(|name|col(name)).collect();
        let aggs:Vec<Expr> = agg_param.col_exprs.iter().map(|expr|build_col_expr(expr)).collect();

        let df = self.df.groupby(by).agg(aggs);
        Self{df}
    }
}

fn build_col_expr(expr:&ColExpr)-> Expr{
    let clone:ColExpr  = expr.clone();
    let name = clone.name;
    let aggregate = clone.aggregate;

    let mut alias = String::from(&name);
    alias.push_str("_");
    alias.push_str(&aggregate);

    finish_agg_expr(col(&name),&aggregate).alias(&alias)
}

fn finish_agg_expr(expr:Expr ,aggregate: &str) -> Expr {
    match aggregate {
        "min" => expr.min(),
        "max" => expr.max(),
        "mean" => expr.mean(),
        "first" => expr.first(),
        "last" => expr.last(),
        "sum" => expr.sum(),
        "count" => expr.count(),
        _ => expr
    }
}

///
///
///
pub fn eval_lazy_script(script:&str) ->Result<JsLazyFrame, Box<EvalAltResult>>{
    let mut engine = Engine::new();

    engine.register_fn("readCsv",JsLazyFrame::read_csv)
        .register_fn("agg",JsLazyFrame::agg);

    let result = engine.eval(script);

    result
}