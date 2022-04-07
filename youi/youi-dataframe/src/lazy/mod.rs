use polars_core::prelude::SortOptions;
use polars_lazy::dsl::col;
use rhai::plugin::*;
use polars_lazy::prelude::{Expr, LazyCsvReader, LazyFrame};
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
    ///
    /// 排序
    ///
    fn sort(self,name:String,descending:bool)->Self{
        let sort_opt = SortOptions{
            descending,
            nulls_last: true
        };
        let df = self.df.sort(&name, sort_opt);
        Self{df}
    }
}

fn build_col_expr(expr:&ColExpr)-> Expr{
    let clone:ColExpr  = expr.clone();
    let name = clone.name;
    let aggregate = clone.aggregate;

    let alias = format!("{}_{}",name,aggregate);

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
        .register_fn("agg",JsLazyFrame::agg)
        .register_fn("sort",JsLazyFrame::sort);

    let result = engine.eval(script);

    result
}