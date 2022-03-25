extern crate serde;

use serde::Serialize;

mod json_render;

#[derive(Serialize)]
pub struct PagerRecords<T: Sized+Serialize>{
    records:Vec<T>
}

