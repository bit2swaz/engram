use serde::{Deserialize, Serialize};
use crate::knowledge::types::{Entity, Relationship};

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphExport {
    pub session_id: String,
    pub entities: Vec<Entity>,
    pub edges: Vec<Relationship>,
}

impl GraphExport {
    pub fn new(session_id: impl Into<String>, entities: Vec<Entity>, edges: Vec<Relationship>) -> Self {
        Self { session_id: session_id.into(), entities, edges }
    }
}

pub fn to_dot(export: &GraphExport) -> String {
    let mut lines = vec!["digraph knowledge {".to_string()];
    for entity in &export.entities {
        lines.push(format!(
            "  \"{}\" [label=\"{}\\n({})\"];",
            escape_dot(&entity.name),
            escape_dot(&entity.name),
            entity.entity_type,
        ));
    }
    for edge in &export.edges {
        lines.push(format!(
            "  \"{}\" -> \"{}\" [label=\"{}\"];",
            escape_dot(&edge.from),
            escape_dot(&edge.to),
            edge.relationship_type,
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn escape_dot(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::types::{Entity, Relationship};
    use std::collections::HashMap;

    fn make_export() -> GraphExport {
        GraphExport {
            session_id: "s1".to_string(),
            entities: vec![
                Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() },
                Entity { name: "OpenAI".into(), entity_type: "Organization".into(), attributes: HashMap::new() },
            ],
            edges: vec![
                Relationship { from: "Alice".into(), to: "OpenAI".into(), relationship_type: "works_at".into() },
            ],
        }
    }

    #[test]
    fn empty_graph_produces_minimal_dot() {
        let empty = GraphExport { session_id: "s1".into(), entities: vec![], edges: vec![] };
        let dot = to_dot(&empty);
        assert_eq!(dot.trim(), "digraph knowledge {\n}");
    }

    #[test]
    fn dot_contains_entity_nodes_with_type() {
        let dot = to_dot(&make_export());
        assert!(dot.contains("\"Alice\""));
        assert!(dot.contains("\"OpenAI\""));
        assert!(dot.contains("Person"));
    }

    #[test]
    fn dot_contains_edge_with_label() {
        let dot = to_dot(&make_export());
        assert!(dot.contains("\"Alice\" -> \"OpenAI\""));
        assert!(dot.contains("works_at"));
    }

    #[test]
    fn dot_escapes_double_quotes_in_names() {
        let export = GraphExport {
            session_id: "s1".into(),
            entities: vec![Entity { name: "Alice \"A\"".into(), entity_type: "Person".into(), attributes: HashMap::new() }],
            edges: vec![],
        };
        let dot = to_dot(&export);
        assert!(!dot.contains("Alice \"A\""));
    }

    #[test]
    fn graph_export_json_round_trips() {
        let export = make_export();
        let json = serde_json::to_string(&export).unwrap();
        let back: GraphExport = serde_json::from_str(&json).unwrap();
        assert_eq!(back.entities.len(), 2);
        assert_eq!(back.edges.len(), 1);
        assert_eq!(back.session_id, "s1");
    }
}
