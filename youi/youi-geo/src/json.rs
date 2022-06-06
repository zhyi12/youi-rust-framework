use geo::{Geometry, GeometryCollection, Polygon};
use geojson::{FeatureCollection, GeoJson};

///
///
///
pub fn to_geo_json(polys:&Vec<Polygon<f64>>) -> GeoJson {

    let geometries:Vec<Geometry<f64>> = polys.iter().map(|poly|Geometry::from(poly.clone())).collect();

    let geometry_collection = GeometryCollection::new_from(geometries);

    let feature_collection:FeatureCollection = FeatureCollection::from(&geometry_collection);

    GeoJson::FeatureCollection(feature_collection)
}