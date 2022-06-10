use std::iter::Cycle;
use geo::{Geometry, GeometryCollection, Point, point, Polygon};
use geojson::{FeatureCollection, GeoJson};
use smartcore::linalg::BaseVector;

///
///
///
pub fn to_geo_json(polys:&Vec<Polygon<f64>>,points:&Vec<Vec<f64>>) -> GeoJson {

    let mut geometries:Vec<Geometry<f64>> = polys.iter().map(|poly|Geometry::from(poly.clone())).collect();

    points.iter().for_each(|p|{
        let p1: Cycle<f64> = (p.get(0), p.get(1)).into();
        let gp = Geometry::from(p1);
        geometries.push(gp);
    });

    let geometry_collection = GeometryCollection::new_from(geometries);

    let feature_collection:FeatureCollection = FeatureCollection::from(&geometry_collection);

    GeoJson::FeatureCollection(feature_collection)
}