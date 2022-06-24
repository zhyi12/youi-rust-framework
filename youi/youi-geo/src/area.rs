use std::cmp::Ordering;
use std::ops::Div;
use geo::{coord, Coordinate, LineString, Point, Polygon, polygon};
use geo::prelude::{Area, BoundingRect, Centroid, Contains};
use geojson::{FeatureCollection, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct PolygonGrid{
    points:Vec<[f64;2]>,
    total:i32,
    x_block:i32,
    y_block:i32
}

///
/// 获取行政区划主要区域
///
pub fn find_main_poly(feature_collection:&FeatureCollection)->Polygon<f64>{
    let mut polygons:Vec<Polygon<f64>> = feature_collection.features.iter()
        .map(|feature| parse_polygon(feature)).collect();

    //
    polygons.sort_by(|a,b|{
        if a.unsigned_area().abs()>b.unsigned_area().abs(){
            return Ordering::Less;
        }
        Ordering::Greater
    });

    let poly = polygons.get(0).unwrap();

    poly.clone()
}

///
/// 多边形区域网格切分
///
pub fn find_poly_grid_point(poly:&Polygon<f64>,block_x:i32,block_y:i32)->String{

    let mut split_distance:f64 = 0.00025;

    let rect = poly.bounding_rect().unwrap();

    let min_coord = rect.min();
    let max_coord = rect.max();

    let x_distance =(min_coord.x - max_coord.x).abs();
    let y_distance =(min_coord.y - max_coord.y).abs();

    split_distance = (x_distance.min(y_distance)/10.0).min(split_distance);

    let x_split_count:i32 = ((max_coord.x - min_coord.x)/split_distance).ceil() as i32;
    let y_split_count:i32 = ((max_coord.y - min_coord.y)/split_distance).ceil() as i32;

    let block_size = 36;

    let x_block_count = x_split_count.div(block_size);
    let y_block_count = y_split_count.div(block_size);

    let total = x_block_count*y_block_count;

    println!("{} {} {} {} {}",x_split_count,y_split_count,x_block_count,y_block_count,total);

    let start_x = block_x*block_size;
    let end_x = ((block_x+1)*block_size).min(x_split_count);
    let start_y= block_y*block_size;
    let end_y = ((block_y+1)*block_size).min(y_split_count);

    println!("{} {} {} {} ",start_x,end_x,start_y,end_y);

    let mut points:Vec<[f64;2]> = Vec::new();

    let mut scripts = String::new();
    scripts.push_str("[");
    for dx in start_x..end_x {
        for dy in start_y..end_y {
            let cp = find_grid_center(dx,dy,&min_coord,split_distance);
            if poly.contains(&cp){
                points.push([cp.x(),cp.y()]);
            }
        }
    }

    println!("{}",points.len());
    let point_json = serde_json::to_string(&PolygonGrid{
        points: points.clone(),
        total,
        x_block:x_block_count,
        y_block:y_block_count
    });

    point_json.unwrap()
}

fn find_grid_center(dx:i32,dy:i32,min_coord:&Coordinate<f64>,split_distance:f64)->Point<f64>{

    let left: f64 = min_coord.x + (dx as f64 - 1.0) * split_distance;
    let right: f64 = min_coord.x + dx as f64 * split_distance;
    let top: f64 = min_coord.y + (dy as f64 - 1.0) * split_distance;
    let bottom: f64 = min_coord.y + dy as f64 * split_distance;

    let poly = polygon![
                 (x:  left, y: top),
                 (x:  right, y: top),
                 (x: right, y: bottom),
                 (x:  left, y: bottom),
            ];

    poly.centroid().unwrap()
}


///
/// Feature 转 Polygon
///
fn parse_polygon(feature:&geojson::Feature)->Polygon<f64>{
    let geometry = feature.geometry.as_ref().unwrap();

    let value = geometry.value.clone();

    match value {
        Value::Polygon(polygon) => {
            let flat = polygon.iter()
                .map(|ps|ps.iter()).flatten();
            let points:Vec<Coordinate<f64>> = flat.map(|f|{
                let x = f.get(0).unwrap().clone();
                let y  = f.get(1).unwrap().clone();
                coord!(x:x,y:y)
            }).collect();

            let line = LineString::new(points);

            Polygon::new(line,vec![])
        }
        _=>{
            Polygon::new(LineString::new(vec![]),vec![])
        }
    }
}

