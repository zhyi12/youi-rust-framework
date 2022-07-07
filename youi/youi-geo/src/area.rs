use std::cmp::Ordering;
use std::ops::Div;
use coord_transforms::geo::ll2utm;
use coord_transforms::prelude::{geo_ellipsoid, Vector2};
use geo::{coord, Coordinate, LineString, Point, point, Polygon, polygon};
use geo::prelude::{Area, BoundingRect, Centroid, Contains, EuclideanDistance};
use geojson::{FeatureCollection, GeoJson, Value};
use serde::{Deserialize, Serialize};

///
///
///
#[derive(Serialize, Deserialize,Debug)]
pub struct PolygonGrid{
    pub grids:Vec<GridPoint>,
    pub total:i32,
    pub x_block:i32,
    pub y_block:i32
}

#[derive(Serialize, Deserialize,Debug)]
pub struct GridPoint{
    pub x:f64,
    pub y:f64,
    pub r:f64
}

///
///
///
pub fn point_to_poly(x:f64,y:f64,distance:f64)->Polygon<f64>{
    let half = distance/2.0;
    let top = y-half;
    let bottom = y +half;
    let left = x - half;
    let right = x +half;

    polygon![
         (x: left, y: top),
         (x: right, y: top),
         (x: right, y: bottom),
         (x: left, y: bottom),
         (x: left, y: top),
    ]
}

pub fn parse_area_polys(feature_collection:&FeatureCollection)->Vec<Polygon<f64>>{
    feature_collection.features.iter()
        .map(|feature| parse_polygon(feature)).collect()
}

///
/// 获取行政区划主要区域
///
pub fn find_main_poly(geo_json:GeoJson)->Polygon<f64>{
    let feature_collection:FeatureCollection = geo_json.to_json_value().try_into().unwrap();

    let mut polygons:Vec<Polygon<f64>> = parse_area_polys(&feature_collection);
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
///
///
pub fn find_area_grid(poly:&Polygon<f64>){
    let rect = poly.bounding_rect().unwrap();
    let min_coord = rect.min();
    let max_coord = rect.max();
    println!("{:?} {:?}",min_coord,max_coord);
    let p1 = coord_transform(min_coord.x,min_coord.y);
    let p2 = coord_transform(max_coord.x,max_coord.y);

    let x_distance =(p1.x() - p2.x()).abs();
    let y_distance =(p1.y() - p2.y()).abs();

    println!("{} {}",x_distance,y_distance);
}

///
/// 多边形区域网格切分
///
pub fn find_poly_grid_point(poly:&Polygon<f64>,block_x:i32,block_y:i32,distance:f64)->String{
    let poly_grid = find_poly_grid(poly,block_x,block_y,distance);
    let point_json = serde_json::to_string(&poly_grid);
    point_json.unwrap()
}
///
///
///
pub fn find_poly_grid(poly:&Polygon<f64>,block_x:i32,block_y:i32,split_distance:f64)->PolygonGrid{

    let rect = poly.bounding_rect().unwrap();

    let min_coord = rect.min();
    let max_coord = rect.max();

    let x_split_count:i32 = ((max_coord.x - min_coord.x)/split_distance).ceil() as i32;
    let y_split_count:i32 = ((max_coord.y - min_coord.y)/split_distance).ceil() as i32;

    let block_size = 36;

    let x_block_count = (x_split_count as f64).div(block_size as f64).ceil() as i32;
    let y_block_count = (y_split_count as f64).div(block_size as f64).ceil() as i32;

    let total = x_block_count*y_block_count;

    println!("x_split_count:{} y_split_count:{} x_block_count:{} y_block_count:{} total:{}",x_split_count,y_split_count,x_block_count,y_block_count,total);

    let start_x = block_x*block_size;
    let end_x = ((block_x+1)*block_size).min(x_split_count);
    let start_y= block_y*block_size;
    let end_y = ((block_y+1)*block_size).min(y_split_count);

    println!("{} {} {} {} ",start_x,end_x,start_y,end_y);

    let mut points:Vec<[f64;2]> = Vec::new();

    let mut scripts = String::new();
    scripts.push_str("[");

    let mut grids:Vec<GridPoint> = Vec::new();

    for dx in start_x..end_x {
        for dy in start_y..end_y {
            let grid_poly = find_grid_poly(dx,dy,&min_coord,split_distance);
            let cp =grid_poly.centroid().unwrap();
            if poly.contains(&cp){
                points.push([cp.x(),cp.y()]);
                //坐标系转换
                let t_poly = poly_area_transform(&grid_poly);
                let t_max = t_poly.bounding_rect().unwrap().max();
                let t_p = &t_poly.centroid().unwrap();
                let r = t_p.euclidean_distance(&point!(x:t_max.x,y:t_max.y));
                //println!("[中心点{},{}] 面积：{}平方米,半径{} {}",cp.x(),cp.y(),area,r,3.14*r.powi(2));
                grids.push(GridPoint{
                    x: cp.x(),
                    y: cp.y(),
                    r
                })
            }
        }
    }

    PolygonGrid{
        grids,
        total,
        x_block:x_block_count,
        y_block:y_block_count
    }
}

///
///  坐标系转换
///
pub fn coord_transform(lon:f64,lat:f64)->Point<f64>{
    let ellipsoid = geo_ellipsoid::geo_ellipsoid::new(geo_ellipsoid::WGS84_SEMI_MAJOR_AXIS_METERS,
                                                      geo_ellipsoid::WGS84_FLATTENING);
    let ll_vec: Vector2<f64> = Vector2::new(lat.to_radians(),lon.to_radians());
    let utm = ll2utm(&ll_vec,&ellipsoid);
    point!(x:utm.get_easting(),y:utm.get_northing())
}

///
///
///
pub fn poly_area_transform(poly:&Polygon<f64>)->Polygon<f64>{
    let points = poly.exterior().points();
    let t_points:Vec<Point<f64>> = points.map(|p|coord_transform(p.x(),p.y())).collect();
    let t_poly = Polygon::new(LineString::from(t_points),vec![]);
    t_poly
}

fn find_grid_poly(dx:i32,dy:i32,min_coord:&Coordinate<f64>,split_distance:f64)->Polygon<f64>{

    let left: f64 = min_coord.x + (dx as f64 - 1.0) * split_distance;
    let right: f64 = min_coord.x + dx as f64 * split_distance;
    let top: f64 = min_coord.y + (dy as f64 - 1.0) * split_distance;
    let bottom: f64 = min_coord.y + dy as f64 * split_distance;

    polygon![
                 (x:  left, y: top),
                 (x:  right, y: top),
                 (x: right, y: bottom),
                 (x:  left, y: bottom),
            ]
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

