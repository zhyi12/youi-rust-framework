use geojson::{Feature, FeatureCollection, GeoJson, JsonObject, JsonValue, Value};
use geojson::feature::Id;
use serde_json::Number;
use crate::task::GeoSpiderTask;

///
/// 输出行政区划分块任务的geo json
///
pub fn geo_tasks(tasks:Vec<GeoSpiderTask>)->String{

    let geometries:Vec<Feature> = tasks.iter().map(|task|task_feature(task)).collect();

    let geo = GeoJson::FeatureCollection(FeatureCollection{
        bbox: None,
        features: geometries,
        foreign_members: None
    });

    geo.to_string()
}
///
/// 分区任务信息转 Feature Point
///
fn task_feature(task:&GeoSpiderTask)->Feature{
    let mut properties:JsonObject = JsonObject::new();
    properties.insert(String::from("r"),JsonValue::from(task.r));
    properties.insert(String::from("d"),JsonValue::from(task.distance));
    properties.insert(String::from("count"),JsonValue::from(task.count));
    properties.insert(String::from("pid"),JsonValue::from(task.pid));
    properties.insert(String::from("current"),JsonValue::from(task.current));
    properties.insert(String::from("page"),JsonValue::from(task.page));
    let hd =  task.distance/2.0;
    let left = task.x - hd;
    let right = task.x+hd;
    let top = task.y - hd;
    let bottom = task.y+hd;

    Feature{
        bbox: None,
        geometry: Some(geojson::Geometry::from(
            Value::Polygon(vec![vec![vec![left,top],vec![right,top],vec![right,bottom],vec![left,bottom],vec![left,top]]])
        )),
        id: Some(Id::Number(Number::from(task.id))),
        properties: Some(properties),
        foreign_members: None
    }
}