use std::collections::HashMap;
use geo::{Geometry, LineString, Point, point, Polygon};
use geo::prelude::ConvexHull;
use geojson::{FeatureCollection, GeoJson, Feature, JsonObject, JsonValue};
use crate::AddressPoint;

///
///
///
pub fn to_geo_json(k_area_points:&HashMap<String,Vec<Point<f64>>>,address_points:&Vec<AddressPoint>) -> GeoJson {

    //凸包输出多边形区域
    let mut geometries:Vec<Feature> = k_area_points.iter().map(|entry|{
        let poly_points:Polygon<f64> = Polygon::new(LineString::from(entry.1.clone()),vec![]);
        //凸包
        let hull =  poly_points.convex_hull();
        let mut properties:JsonObject = JsonObject::new();
        properties.insert(String::from("areaKey"),JsonValue::from(entry.0.to_string()));
        Feature{
            bbox: None,
            geometry: Some(geojson::Geometry::from(&hull)),
            id: None,
            properties: Some(properties),
            foreign_members: None
        }
    }).collect();

    address_points.iter().for_each(|p|{
        let p1: Geometry<f64> = point!(x: p.lng, y: p.lat).into();
        let mut properties:JsonObject = JsonObject::new();
        properties.insert(String::from("k"),JsonValue::from(p.group));

        geometries.push(Feature{
            bbox: None,
            geometry: Some(geojson::Geometry::from(&p1)),
            id: None,
            properties:Some(properties),
            foreign_members: None
        });
    });

    GeoJson::FeatureCollection(FeatureCollection{
        bbox: None,
        features: geometries,
        foreign_members: None
    })
}