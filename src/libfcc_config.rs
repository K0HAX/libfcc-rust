use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct MySQLConfig {
    pub sql_url: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Configuration {
    pub mysql_config: MySQLConfig,
}
