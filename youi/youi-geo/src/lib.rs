mod json;
pub mod cluster;

extern crate geo;

use geo::Rect;

#[derive(Clone)]
pub struct AddressPoint {
    pub lng:f64,//经度
    pub lat:f64,//纬度
    pub count:i32,
    pub group:f64
}

pub struct AddressArea{
    pub index:i32,
    pub x:i32,
    pub y:i32,
    pub count:i32,
    pub rect:Rect<f64>,
    pub points:Vec<AddressPoint>,
    pub group:f64
}
