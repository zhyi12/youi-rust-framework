extern crate serde;

use serde::Serialize;

pub mod json_render;

#[derive(Serialize)]
pub struct PagerRecords<T: Sized+Serialize>{
    pub records:Vec<T>
}

