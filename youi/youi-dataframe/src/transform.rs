use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::ops::Add;
use rhai::{AST, ASTNode, Engine, Stmt, Expr, FnCallExpr, Identifier};

///
/// 转换为可执行的脚本
///
pub fn transform(script:&str) ->String {
    let engine = Engine::new();
    let ast:AST = engine.compile(script).unwrap();

    let mut scripts = String::new();

    for stmt in ast.statements() {
        let res = transform_stmt(stmt);
        scripts.push_str(&res);
    }

    println!("output : {:?}",scripts);

    scripts
}

///
///
///
fn transform_stmt(stmt:&Stmt)->String{
    let mut scripts:String = String::new();

    match stmt {
        Stmt::Noop(_) => {}
        Stmt::If(_, _) => {}
        Stmt::Switch(_, _) => {}
        Stmt::While(_, _) => {}
        Stmt::Do(_, _, _) => {}
        Stmt::For(_, _) => {}
        Stmt::Var(x, ..) => {
            scripts.push_str("let ");
            scripts.push_str(&x.0.name);
            scripts.push_str(" = ");
            scripts.push_str(&transform_expr(&x.1));
            scripts.push_str(";");
        }
        Stmt::Assignment(x, _) => {
            println!("Assignment {:?}",x);
        }
        Stmt::FnCall(x, _) => {
            println!("fn_call {}",x.name);

            for e in &x.args {
                println!("{:?}",e)
            }
        }
        Stmt::Block(_) => {}
        Stmt::TryCatch(_, _) => {}
        Stmt::Expr(x) => {
            scripts.push_str(" ");
            scripts.push_str(&transform_expr(&x));
        }
        Stmt::BreakLoop(_, _) => {}
        Stmt::Return(x, _, _) => {
            //scripts.push_str(&transform_expr(&x.as_ref().unwrap()));
        }
        Stmt::Import(_, _) => {}
        Stmt::Export(_, _) => {}
        Stmt::Share(_, _) => {}
    }

    scripts
}

///
///
///
fn transform_expr(expr:&Expr) -> String{
    
    let mut scripts:String = String::new();

    match expr {
        Expr::DynamicConstant(_, _) => {}
        Expr::BoolConstant(x, _) =>{
            scripts.push_str(x.to_string().as_str());
        }
        Expr::IntegerConstant(x, _)=>{
            scripts.push_str(x.to_string().as_str());
        }
        Expr::FloatConstant (x, _)=>{
            scripts.push_str(x.to_string().as_str());
        }
        Expr::CharConstant(x, _)=> {
            scripts.push_str(x.to_string().as_str());
        }
        Expr::StringConstant(x, _) => {
            scripts.push_str("\"");
            scripts.push_str(x);
            scripts.push_str("\"");
        }
        Expr::InterpolatedString(_, _) => {}
        Expr::Array(x, _) => {
            scripts.push_str("[");
            if x.len()>0 {
                for e in x.as_ref() {
                    scripts.push_str(&transform_expr(&e));
                    scripts.push_str(",");
                }
                scripts.remove(scripts.len() - 1);
            }
            scripts.push_str("]");
        }
        Expr::Map(_, _) => {}
        Expr::Unit(_) => {}
        Expr::Variable(x, _, _) => {
            scripts.push_str(&x.3);
        }
        Expr::Property(_, _) => {}
        Expr::Stmt(_) => {

        }
        Expr::FnCall(x, _) | Expr::MethodCall(x, _) => {
            scripts.push_str(&transform_fn(&x));
        }
        Expr::Dot(x, _, _) => {
            scripts.push_str(&transform_expr(&x.lhs));
            scripts.push_str(".");
            scripts.push_str(&transform_expr(&x.rhs));
        }
        Expr::Index(_, _, _) => {}
        Expr::And(_, _) => {}
        Expr::Or(_, _) => {}
        Expr::Custom(_, _) => {}
    }

    scripts
}

///
///处理四则运行
///
fn transform_fn(fnExpr:&FnCallExpr) -> String{
    let mut scripts:String = String::new();

    match fnExpr.name.as_str() {
        "+" => {
            scripts.push_str(&transform_addition_fn(fnExpr,"add"));
        }
        "-" => {
            scripts.push_str(&transform_addition_fn(fnExpr,"sub"));
        }
        "*" => {
            scripts.push_str(&transform_addition_fn(fnExpr,"mul"));
        }
        "/" => {
            scripts.push_str(&transform_addition_fn(fnExpr,"div"));
        }
        _ => {
            scripts.push_str(&fnExpr.name);
            scripts.push_str("(");
            if fnExpr.args.len()>0 {
                for e in &fnExpr.args {
                    scripts.push_str(&transform_expr(&e));
                    scripts.push_str(",");
                }
                scripts.remove(scripts.len() - 1);
            }
            scripts.push_str(")");
        }
    }
    scripts
}
///
/// 四则运算函数处理
///
fn transform_addition_fn(fnExpr:&FnCallExpr,op:&str)->String{
    let mut scripts:String = String::new();
    scripts.push_str(&transform_addition(&fnExpr.args[0]));
    scripts.push_str(".");
    scripts.push_str(op);
    scripts.push_str("(");
    scripts.push_str(&transform_addition(&fnExpr.args[1]));
    scripts.push_str(")");
    scripts
}

///
/// 四则运算参数处理
///
fn transform_addition(expr:&Expr)->String{
    let mut scripts:String = String::new();
    match expr {
        Expr::IntegerConstant(_, _) | Expr::FloatConstant(_,_) => {
            scripts.push_str("expr(");
            scripts.push_str(&transform_expr(expr));
            scripts.push_str(")");
        }
        Expr::FnCall(x, _) | Expr::MethodCall(x, _) => {
            scripts.push_str(&transform_expr(expr));
        }
        _=>{}
    }
    scripts
}
