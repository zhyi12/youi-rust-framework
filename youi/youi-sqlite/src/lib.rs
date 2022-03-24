
extern crate diesel;
extern crate r2d2;

use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use r2d2::Pool;

/// 连接池
///
pub fn create_sqlite_pool(database_url : &String) -> Pool<ConnectionManager<SqliteConnection>> {
    let manager:ConnectionManager<SqliteConnection> = diesel::r2d2::ConnectionManager::new(database_url);

    let pool = r2d2::Pool::builder()
        .max_size(15)
        .build(manager)
        .unwrap();

    pool
}
/// 创建数据库连接
///
pub fn create_sqlite_connection(database_url : &String) -> SqliteConnection {
    let conn = SqliteConnection::establish(database_url).unwrap();

    conn
}