use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct MySQLConfig {
    pub sql_url: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Configuration {
    pub mysql_config: MySQLConfig,
    pub json_filename: String,
    pub write_sql: bool,
    pub write_json: bool,
    pub download_db: bool,
}
