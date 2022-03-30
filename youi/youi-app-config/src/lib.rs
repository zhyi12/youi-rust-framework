#[macro_use]
extern crate diesel;
extern crate core;

pub mod models;
pub mod schema;

use diesel::prelude::*;

use schema::stats_desktop_config::dsl::stats_desktop_config;
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
