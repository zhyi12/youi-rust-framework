
#[derive(Queryable,serde::Serialize)]
pub struct AppConfigItem {
    pub id: i32,
    pub value: String,
    pub name: String
}

impl Clone for AppConfigItem{
    fn clone(&self) -> Self {
        AppConfigItem {
            id: self.id,
            value: String::from(&self.value),
            name: String::from(&self.name)
        }
    }
}

