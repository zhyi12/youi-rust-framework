
use std::ops::{Add, Div, Mul, Sub};
use polars_core::frame::{DataFrame, DistinctKeepStrategy};
use polars_core::prelude::{DataType, IntoSeries, JoinType, NamedFrom, Series, SortOptions};
use polars_lazy::dsl::{col, cols};
use polars_lazy::dsl::Expr::{Literal};
use polars_lazy::logical_plan::LiteralValue;
use rhai::plugin::*;
use polars_lazy::prelude::*;
use rhai::Array;
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

struct ColumnItem{
    name:String,
    data_type:String
}

impl JsLazyFrame {
    ///
    /// 读取csv文件
    ///
    fn read_csv(path:String) -> Self {
        let mut errors :Vec<String> = Vec::new();

        let result_df = LazyCsvReader::new(path).finish();

        match result_df {
            Ok(x) => {
                return Self{df:x};
            }
            Err(_) => {
                errors.push(String::from("数据文件异常"));
            }
        }

        Self{df:DataFrame::new(vec![Series::new("error",&errors)]).unwrap().lazy()}
    }

    ///
    /// 读取csv头信息
    ///
    fn read_csv_header(path:String) -> Self{
        let result_lf = LazyCsvReader::new(path)
            .with_skip_rows(0)
            .with_n_rows(Option::Some(1))
            .finish();

        let mut errors :Vec<String> = Vec::new();

        match result_lf {
            Ok(x) => {
                let result_df = x.collect();

                match result_df {
                    Ok(x) => {
                        let items:Vec<ColumnItem> = x.get_columns().iter().map(|s|(ColumnItem{
                            name:String::from(s.name()),
                            data_type:String::from(s.dtype().to_string())
                        })).collect();

                        let v1:Vec<String> = items.iter().map(|item|String::from(item.name.as_str())).collect();
                        let v2:Vec<String> = items.iter().map(|item|String::from(item.data_type.as_str())).collect();

                        let df = DataFrame::new(vec![Series::new("name",&v1), Series::new("dataType",&v2)]);

                        return Self{df:df.unwrap().lazy()};
                    }
                    Err(_) => {
                        errors.push(String::from("数据文件异常"));
                    }
                }
            }
            Err(_) => {
                errors.push(String::from("csv 读取异常"));
            }
        }

        Self{df:DataFrame::new(vec![Series::new("error",&errors)]).unwrap().lazy()}

    }

    ///
    ///
    ///
    fn pager_read_csv(path:String,page_index:i64,page_size:i64) -> Self {
        let start_row:usize = ((page_index-1)*page_size) as usize;

        let mut errors :Vec<String> = Vec::new();
        let result_df = LazyCsvReader::new(path)
            .with_skip_rows(start_row)
            .with_n_rows(Option::Some(page_size as usize))
            .finish();
        match result_df {
            Ok(x) => {
                return Self{df:x};
            }
            Err(_) => {
                errors.push(String::from("数据文件异常"));
            }
        }

        Self{df:DataFrame::new(vec![Series::new("error",&errors)]).unwrap().lazy()}
    }

    ///
    /// 读取每一列第一个数据
    ///
    fn read_first(self)-> Self{
        let clone = self.df.clone().collect().unwrap();
        let column_names = clone.get_column_names();
        let exprs:Vec<Expr> = column_names.iter().map(|name|col(name).first()).collect();
        Self{df:self.df.select(exprs)}
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
    ///
    ///
    fn select(self,js_exprs:Vec<JsExpr>)->Self{
        let exprs:Vec<Expr> = js_exprs.iter().map(|js_expr|js_expr.expr.clone()).collect();

        Self { df:self.df.select(exprs)}
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
    fn agg(self,by:String,js_exprs:Vec<JsExpr>)->Self{
        let by:Vec<Expr> = by.split(",").map(|name|col(name)).collect();
        let exprs:Vec<Expr> = js_exprs.iter().map(|js_expr|js_expr.expr.clone()).collect();
        let df = self.df.groupby(by).agg(exprs);
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

    ///
    /// 多列排序
    ///
    fn sort_by_exprs(self,js_exprs:Vec<JsExpr>,reverses:String)->Self{
        let exprs:Vec<Expr> = js_exprs.iter().map(|js_expr|js_expr.expr.clone()).collect();
        let reverse:Vec<bool> = reverses.split(",").map(|name|name.eq("true")).collect();
        Self{df:self.df.sort_by_exprs(exprs,reverse)}
    }
    ///
    ///
    ///
    fn limit(self,n:i64)->Self{
        Self{df:self.df.limit(n as u32)}
    }

    ///
    ///
    ///
    fn distinct(self,col_names:Array)->Self{
        let names:Vec<String> = col_names.iter().map(|name|name.to_string()).collect();
        Self{df:self.df.distinct(Some(names),DistinctKeepStrategy::First)}
    }
}

impl JsExpr {
    fn col(name:String)->Self{
        Self{expr:col(&name)}
    }

    fn exprs(exprs: &mut Vec<Dynamic>)->Vec<JsExpr>{
        let mut list:Vec<JsExpr> = Vec::new();

        let iter = exprs.iter();

        for elem in iter {
            let js_expr:JsExpr = elem.clone_cast();
            list.push(js_expr)
        }

        list
    }

    fn cols(names:&Vec<Dynamic>)->Self{
        let names:Vec<String> = names.to_vec().iter()
            .map(|name|from_dynamic(name).unwrap()).collect();
        println!("{:?}",names);
        Self{expr:cols(names)}
    }
    ///
    ///
    ///
    fn first(self)->Self{
        Self{expr:self.expr.first()}
    }

    fn last(self)->Self{
        Self{expr:self.expr.last()}
    }

    fn count(self)->Self{
        Self{expr:self.expr.count()}
    }

    fn sum(self)->Self{
        Self{expr:self.expr.sum()}
    }

    fn min(self)->Self{
        Self{expr:self.expr.min()}
    }

    fn max(self)->Self{
        Self{expr:self.expr.max()}
    }

    fn list(self)->Self{
        Self{expr:self.expr.list()}
    }

    fn alias(self,alias_name:String)->Self{
        Self{expr:self.expr.alias(&alias_name)}
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

    fn concat_str(self,js_exprs:Vec<JsExpr>, sep: &str)-> Self{
        let exprs:Vec<Expr> = js_exprs.iter().map(|js_expr|js_expr.expr.clone()).collect();
        Self{expr:concat_str(exprs,sep)}
    }
    //
    fn str_slice(self,start: i64,len:i64)->Self{
        let func = move |s: Series| {
            let ca = s.utf8()?;
            Ok(ca.str_slice(start, Some(len as u64))?.into_series())
        };

        Self{expr:self.expr.apply(func,GetOutput::from_type(DataType::Utf8))}
    }

    fn cast_str(self)->Self{
        Self{expr:self.expr.cast(DataType::Utf8)}
    }

    fn add(self,js_expr:JsExpr)->Self{
        Self{expr:self.expr.add(js_expr.expr)}
    }

    fn sub(self,js_expr:JsExpr)->Self{
        Self{expr:self.expr.sub(js_expr.expr)}
    }

    fn mul(self,js_expr:JsExpr)->Self{
        Self{expr:self.expr.mul(js_expr.expr)}
    }

    fn div(self,js_expr:JsExpr)->Self{
        Self{expr:self.expr.div(js_expr.expr)}
    }
}

///
///
///
pub fn eval_lazy_script(script:&str) ->Result<JsLazyFrame, Box<EvalAltResult>>{
    let mut engine = Engine::new();

    engine
        .register_type::<JsLazyFrame>()
        .register_fn("read_csv",JsLazyFrame::read_csv)
        .register_fn("read_csv_header",JsLazyFrame::read_csv_header)
        .register_fn("pager_read_csv",JsLazyFrame::pager_read_csv)
        .register_fn("read_first",JsLazyFrame::read_first)
        .register_fn("select",JsLazyFrame::select)
        .register_fn("join",JsLazyFrame::join)
        .register_fn("left_join",JsLazyFrame::left_join)
        .register_fn("agg",JsLazyFrame::agg)
        .register_fn("sort",JsLazyFrame::sort)
        .register_fn("sort_by_exprs",JsLazyFrame::sort_by_exprs)
        .register_fn("filter",JsLazyFrame::filter)
        .register_fn("limit",JsLazyFrame::limit)
        .register_fn("distinct",JsLazyFrame::distinct)
        .register_type::<JsExpr>()
        .register_fn("col",JsExpr::col)
        .register_fn("cols",JsExpr::cols)
        .register_fn("first",JsExpr::first)
        .register_fn("or",JsExpr::or)
        .register_fn("and",JsExpr::and)
        .register_fn("expr",JsExpr::value_expr)
        .register_fn("expr",JsExpr::value_expr_i64)
        .register_fn("expr",JsExpr::value_expr_bool)
        .register_fn("expr",JsExpr::value_expr_f64)
        .register_fn("exprs",JsExpr::exprs)
        .register_fn("eq",JsExpr::eq)
        .register_fn("gt",JsExpr::gt)
        .register_fn("gte",JsExpr::gt_eq)
        .register_fn("lt",JsExpr::lt)
        .register_fn("lte",JsExpr::lt_eq)

        .register_fn("first",JsExpr::first)
        .register_fn("last",JsExpr::last)
        .register_fn("sum",JsExpr::sum)
        .register_fn("count",JsExpr::count)
        .register_fn("max",JsExpr::max)
        .register_fn("min",JsExpr::min)
        .register_fn("list",JsExpr::list)
        .register_fn("alias",JsExpr::alias)

        .register_fn("isNull",JsExpr::is_null)
        .register_fn("concat_str",JsExpr::concat_str)
        .register_fn("str_slice",JsExpr::str_slice)
        .register_fn("cast_str",JsExpr::cast_str)

        .register_fn("add",JsExpr::add)
        .register_fn("sub",JsExpr::sub)
        .register_fn("mul",JsExpr::mul)
        .register_fn("div",JsExpr::div);

    let result = engine.eval(script);

    result
}