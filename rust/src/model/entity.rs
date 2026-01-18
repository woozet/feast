use crate::proto::feast::core;

#[derive(Debug, Clone)]
pub struct Entity {
    pub name: String,
    pub join_key: String,
}

impl Entity {
    pub fn from_proto(proto: &core::Entity) -> Self {
        let spec = proto.spec.as_ref();
        Self {
            name: spec.map(|s| s.name.clone()).unwrap_or_default(),
            join_key: spec.map(|s| s.join_key.clone()).unwrap_or_default(),
        }
    }
}
