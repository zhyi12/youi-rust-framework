use std::collections::{HashMap, HashSet};
use geo::{LineString, Point, Polygon};
use geo::prelude::{Centroid, ConvexHull};
use geojson::{FeatureCollection, GeoJson, Feature, JsonObject, JsonValue};

///
///
///
pub fn to_geo_json(k_area_points:&HashMap<String,Vec<Point<f64>>>) -> GeoJson {
    //凸包输出多边形区域
    let mut geometries:Vec<Feature> = k_area_points.iter().map(|entry|{
        let poly_points:Polygon<f64> = Polygon::new(LineString::from(entry.1.clone()),vec![]);
        //凸包
        let hull =  poly_points.convex_hull();
        let centroid = hull.centroid().unwrap();
        let mut properties:JsonObject = JsonObject::new();
        properties.insert(String::from("areaKey"),JsonValue::from(entry.0.to_string()));
        properties.insert(String::from("count"),JsonValue::from(entry.1.len()));
        properties.insert(String::from("centerX"),JsonValue::from(centroid.x()));
        properties.insert(String::from("centerY"),JsonValue::from(centroid.y()));
        Feature{
            bbox: None,
            geometry: Some(geojson::Geometry::from(&hull)),
            id: None,
            properties: Some(properties),
            foreign_members: None
        }
    }).collect();

    let mut point_keys = HashSet::new();
    k_area_points.iter().for_each(|entry|{
        entry.1.iter().for_each(|p|{
            let mut xy:String = String::from(p.x().to_string());
            xy.push_str(",");
            xy.push_str(p.y().to_string().as_str());
            if !point_keys.contains(&xy){
                geometries.push(build_point_feature(entry.0,p));
                point_keys.insert(xy);
            }
        });
    });

    GeoJson::FeatureCollection(FeatureCollection{
        bbox: None,
        features: geometries,
        foreign_members: None
    })
}
///
///
///
fn build_point_feature(key:&str,point:&Point<f64>)->Feature{
    let mut properties:JsonObject = JsonObject::new();
    properties.insert(String::from("k"),JsonValue::from(key));

    Feature{
        bbox: None,
        geometry: Some(geojson::Geometry::from(point)),
        id: None,
        properties:Some(properties),
        foreign_members: None
    }
}