extern crate serde;

use serde::Serialize;

pub mod json_render;

#[derive(Serialize)]
pub struct PagerRecords<T: Sized+Serialize>{
    records:Vec<T>
}

