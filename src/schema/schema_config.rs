use serde_derive::Deserialize;
use indexmap::IndexMap;

use super::{
    ParamKey,
    FilterType,
    Transform,
    Engine,
};

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaConfig {
    pub annotations: Option<IndexMap<String, String>>,
    pub endpoints: Vec<EndpointConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointConfig {
    pub name: String,
    pub sql_select: SqlSelectConfig,
    pub primary: Option<String>,
    pub engine: Option<Engine>,
    pub interface: InterfaceConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InterfaceConfig(pub IndexMap<ParamKey, ParamValueConfig>);

#[derive(Debug, Clone, Deserialize)]
pub struct ParamValueConfig {
    pub column: Option<String>,
    pub filter_type: Option<FilterType>,
    pub visible: Option<bool>,
    pub dimension: Option<DimensionConfig>,
    pub is_text: Option<bool>,
    pub is_template_var: Option<bool>,
    pub weight: Option<f32>,
    pub transform: Option<Transform>,
}

// TODO remove. template sql should replace the need for this.
#[derive(Debug, Clone, Deserialize)]
pub struct DimensionConfig {
    pub sql_table: String,
    pub parents: InterfaceConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub enum SqlSelectConfig {
    #[serde(rename="table")]
    Table {
        name: String,
    },
    #[serde(rename="template")]
    Template {
        template_path: String,
    },
}
