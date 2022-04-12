use polars_core::prelude::{JoinType, SortOptions};
use polars_lazy::dsl::{col, cols};
use polars_lazy::dsl::Expr::Literal;
use polars_lazy::logical_plan::LiteralValue;
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
    /// 读取csv文件
    ///
    fn read_csv(path:String) -> Self {
        let df = LazyCsvReader::new(path).finish().unwrap();
        Self{df}
    }

    ///
    /// 连接数据集
    ///
    fn join(self,other:JsLazyFrame,how:&str,left_on:String,right_on:String) -> Self{
        //
        let how = match how {
            "left" => JoinType::Left,
            "inner" => JoinType::Inner,
            "outer" => JoinType::Outer,
            "cross" => JoinType::Cross,
            _ => panic!("not supported"),
        };

        let left_on:Vec<_> = left_on.split(",").map(|name|col(name)).collect();
        let right_on:Vec<_> = right_on.split(",").map(|name|col(name)).collect();

        let df = self.df.join_builder()
            .with(other.df)
            .left_on(left_on)
            .right_on(right_on)
            // .allow_parallel(allow_parallel)
            // .force_parallel(force_parallel)
            .how(how)
            // .suffix(suffix)
            .finish()
            .into();

        Self{df}
    }
    ///
    /// 左联接
    ///
    fn left_join(self,other:JsLazyFrame,left_on:String,right_on:String)->Self{
        let left_on:Vec<_> = left_on.split(",").map(|name|col(name)).collect();
        let right_on:Vec<_> = right_on.split(",").map(|name|col(name)).collect();

        let df = self.df.join_builder()
            .with(other.df)
            .left_on(left_on)
            .right_on(right_on)
            .how(JoinType::Left)
            .finish()
            .into();

        Self{df}
    }
    ///
    /// 过滤
    ///
    fn filter(self,expr:JsExpr) -> Self{
        Self{df:self.df.filter(expr.expr)}
    }
    ///
    /// 汇总
    ///
    fn agg(self,param:Dynamic)->Self{
        let agg_param:AggParam = from_dynamic(&param).unwrap();

        let by:Vec<Expr> = agg_param.group_names.iter().map(|name|col(name)).collect();
        let agg_list:Vec<Expr> = agg_param.col_exprs.iter().map(|expr|build_col_expr(expr)).collect();

        let df = self.df.groupby(by).agg(agg_list);
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

impl JsExpr {
    fn col(name:String)->Self{
        Self{expr:col(&name)}
    }

    fn cols(names:&Vec<Dynamic>)->Self{
        let names:Vec<String> = names.to_vec().iter()
            .map(|name|from_dynamic(name).unwrap()).collect();
        println!("{:?}",names);
        Self{expr:cols(names)}
    }

    fn is_null(self)->Self{
        println!("{}","is null expr");
        Self{expr:self.expr.is_null()}
    }

    fn eq(self,other:JsExpr)->Self{
        Self{expr:self.expr.eq(other.expr)}
    }

    fn gt(self,other:JsExpr)->Self{
        Self{expr:self.expr.gt(other.expr)}
    }

    fn gt_eq(self,other:JsExpr)->Self{
        Self{expr:self.expr.gt_eq(other.expr)}
    }

    fn lt(self,other:JsExpr)->Self{
        Self{expr:self.expr.lt(other.expr)}
    }

    fn lt_eq(self,other:JsExpr)->Self{
        Self{expr:self.expr.lt_eq(other.expr)}
    }

    fn or(self,other:JsExpr)->Self{
        Self{expr:self.expr.or(other.expr)}
    }

    fn and(self,other:JsExpr)->Self{
        Self{expr:self.expr.and(other.expr)}
    }

    fn value_expr(value:String)->Self{
        Self{expr:Literal(LiteralValue::Utf8(value))}
    }

    fn value_expr_i64(value:i64)->Self{
        Self{expr:Literal(LiteralValue::Int64(value))}
    }

    fn value_expr_bool(value:bool)->Self{
        Self{expr:Literal(LiteralValue::Boolean(value))}
    }

    fn value_expr_f64(value:f64)->Self{
        Self{expr:Literal(LiteralValue::Float64(value))}
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

    engine
        .register_type::<JsLazyFrame>()
        .register_fn("readCsv",JsLazyFrame::read_csv)
        .register_fn("join",JsLazyFrame::join)
        .register_fn("leftJoin",JsLazyFrame::left_join)
        .register_fn("agg",JsLazyFrame::agg)
        .register_fn("sort",JsLazyFrame::sort)
        .register_fn("filter",JsLazyFrame::filter)
        .register_type::<JsExpr>()
        .register_fn("col",JsExpr::col)
        .register_fn("cols",JsExpr::cols)
        .register_fn("or",JsExpr::or)
        .register_fn("and",JsExpr::and)
        .register_fn("expr",JsExpr::value_expr)
        .register_fn("expr",JsExpr::value_expr_i64)
        .register_fn("expr",JsExpr::value_expr_bool)
        .register_fn("expr",JsExpr::value_expr_f64)
        .register_fn("eq",JsExpr::eq)
        .register_fn("gt",JsExpr::gt)
        .register_fn("gte",JsExpr::gt_eq)
        .register_fn("lt",JsExpr::lt)
        .register_fn("lte",JsExpr::lt_eq)
        .register_fn("isNull",JsExpr::is_null);

    let result = engine.eval(script);

    result
}