#[macro_use]
extern crate diesel;
extern crate core;

pub mod models;
pub mod schema;

use diesel::prelude::*;

use schema::stats_desktop_config::dsl::{stats_desktop_config,value};
use crate::models::{AppConfigItem};

///
///获取配置信息列表
///
pub fn find_config_list(conn: &SqliteConnection) -> Vec<AppConfigItem> {
    let items = stats_desktop_config.limit(1000)
        .load::<models::AppConfigItem>(conn)
        .unwrap().to_vec();

    items
}


///
/// 更新配置值
///
pub fn update_config_value(conn: &SqliteConnection,id:i32,newValue:&str){
    diesel::update(stats_desktop_config.find(id))
        .set(value.eq(newValue))
        .execute(conn).unwrap();
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
