
#[derive(Queryable,serde::Serialize,serde::Deserialize,Clone)]
pub struct AppConfigItem {
    pub id: i32,
    pub name: String,
    pub value: String,
}


