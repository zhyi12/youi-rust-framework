use std::str::FromStr;
use geojson::{Feature, GeoJson};
use sqlx::{Error, Pool, query, Row, Sqlite};

///
///
///
pub async fn find_area_geo_json(pool:&Pool<Sqlite>,area_id:&str)->Result<GeoJson,Error>{
    let sql = "select geo_json from stats_area_geo_json where area_id=?1";
    let mut geo_json = String::from("{}");
    let result = query(sql).bind(area_id).fetch_optional(pool).await?;
    match result {
        None => {
            println!("{}没有找到geo json.",area_id);
        }
        Some(row) => {
            geo_json = row.get::<String, usize>(0);
        }
    }

    let geo_result = GeoJson::from_str(&geo_json);

    match geo_result {
        Ok(json) => {
            return Ok(json);
        }
        Err(e) => {
            println!("geo parse error:{:?}",e);
        }
    };

    Ok(GeoJson::Feature(Feature{
        bbox: None,
        geometry: None,
        id: None,
        properties: None,
        foreign_members: None
    }))
}