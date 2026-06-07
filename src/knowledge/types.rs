use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entity {
    pub name: String,
    pub entity_type: String,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Relationship {
    pub from: String,
    pub to: String,
    pub relationship_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionResult {
    pub entities: Vec<Entity>,
    pub relationships: Vec<Relationship>,
}

#[derive(Debug, Clone)]
pub struct KnowledgeJob {
    pub session_id: String,
    pub message_id: String,
    pub text: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn entity_round_trips_with_attributes() {
        let entity = Entity {
            name: "Alice".to_string(),
            entity_type: "Person".to_string(),
            attributes: [("role".to_string(), "engineer".to_string())].into(),
        };
        let json = serde_json::to_string(&entity).unwrap();
        let back: Entity = serde_json::from_str(&json).unwrap();
        assert_eq!(back, entity);
    }

    #[test]
    fn entity_empty_attributes_round_trips() {
        let entity = Entity {
            name: "OpenAI".to_string(),
            entity_type: "Organization".to_string(),
            attributes: HashMap::new(),
        };
        let json = serde_json::to_string(&entity).unwrap();
        let back: Entity = serde_json::from_str(&json).unwrap();
        assert!(back.attributes.is_empty());
    }

    #[test]
    fn relationship_round_trips() {
        let rel = Relationship {
            from: "Alice".to_string(),
            to: "OpenAI".to_string(),
            relationship_type: "works_at".to_string(),
        };
        let json = serde_json::to_string(&rel).unwrap();
        let back: Relationship = serde_json::from_str(&json).unwrap();
        assert_eq!(back, rel);
    }

    #[test]
    fn extraction_result_round_trips() {
        let result = ExtractionResult {
            entities: vec![
                Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() },
                Entity { name: "OpenAI".into(), entity_type: "Organization".into(), attributes: HashMap::new() },
            ],
            relationships: vec![
                Relationship { from: "Alice".into(), to: "OpenAI".into(), relationship_type: "works_at".into() },
            ],
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: ExtractionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.entities.len(), 2);
        assert_eq!(back.relationships[0].relationship_type, "works_at");
    }
}
