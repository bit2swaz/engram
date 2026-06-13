use std::collections::{HashMap, HashSet, VecDeque};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::{Deserialize, Serialize};

use crate::knowledge::types::{Entity, Relationship};

struct EntityNode {
    name: String,
    entity_type: String,
    attributes: HashMap<String, String>,
}

struct RelEdge {
    relationship_type: String,
}

struct SessionGraph {
    graph: DiGraph<EntityNode, RelEdge>,
    name_to_idx: HashMap<String, NodeIndex>,
}

impl SessionGraph {
    fn new() -> Self {
        Self { graph: DiGraph::new(), name_to_idx: HashMap::new() }
    }

    fn ensure_entity(&mut self, name: &str, entity_type: &str, attributes: HashMap<String, String>) -> NodeIndex {
        if let Some(&idx) = self.name_to_idx.get(name) {
            return idx;
        }
        let idx = self.graph.add_node(EntityNode {
            name: name.to_string(),
            entity_type: entity_type.to_string(),
            attributes,
        });
        self.name_to_idx.insert(name.to_string(), idx);
        idx
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RelationshipDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelatedEntity {
    pub name: String,
    pub entity_type: String,
    pub relationship_type: String,
    pub direction: RelationshipDirection,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PathEdge {
    pub from: String,
    pub relationship_type: String,
    pub to: String,
}

/// Per-session in-memory knowledge graph.
/// Wrap in `Arc<RwLock<KnowledgeGraph>>` for shared access.
pub struct KnowledgeGraph {
    sessions: HashMap<String, SessionGraph>,
    /// Dedup set using "session_id\x00message_id" keys.
    processed: HashSet<String>,
}

impl KnowledgeGraph {
    pub fn new() -> Self {
        Self { sessions: HashMap::new(), processed: HashSet::new() }
    }

    fn dedup_key(session_id: &str, message_id: &str) -> String {
        format!("{}\x00{}", session_id, message_id)
    }

    pub fn is_processed(&self, session_id: &str, message_id: &str) -> bool {
        self.processed.contains(&Self::dedup_key(session_id, message_id))
    }

    /// Returns `false` if this (session_id, message_id) was already processed.
    pub fn apply_extraction(
        &mut self,
        session_id: &str,
        message_id: &str,
        entities: Vec<Entity>,
        relationships: Vec<Relationship>,
    ) -> bool {
        let key = Self::dedup_key(session_id, message_id);
        if self.processed.contains(&key) {
            return false;
        }
        self.processed.insert(key);

        let session = self.sessions.entry(session_id.to_string()).or_insert_with(SessionGraph::new);

        for entity in &entities {
            session.ensure_entity(&entity.name, &entity.entity_type, entity.attributes.clone());
        }
        for rel in &relationships {
            let from_idx = session.ensure_entity(&rel.from, "Other", HashMap::new());
            let to_idx   = session.ensure_entity(&rel.to,   "Other", HashMap::new());
            session.graph.add_edge(from_idx, to_idx, RelEdge { relationship_type: rel.relationship_type.clone() });
        }
        true
    }

    pub fn get_related(&self, session_id: &str, entity_name: &str) -> Vec<RelatedEntity> {
        let Some(session) = self.sessions.get(session_id) else { return vec![] };
        let Some(&idx) = session.name_to_idx.get(entity_name) else { return vec![] };

        let mut related = Vec::new();
        for edge in session.graph.edges_directed(idx, Direction::Outgoing) {
            let node = &session.graph[edge.target()];
            related.push(RelatedEntity {
                name: node.name.clone(),
                entity_type: node.entity_type.clone(),
                relationship_type: edge.weight().relationship_type.clone(),
                direction: RelationshipDirection::Outgoing,
            });
        }
        for edge in session.graph.edges_directed(idx, Direction::Incoming) {
            let node = &session.graph[edge.source()];
            related.push(RelatedEntity {
                name: node.name.clone(),
                entity_type: node.entity_type.clone(),
                relationship_type: edge.weight().relationship_type.clone(),
                direction: RelationshipDirection::Incoming,
            });
        }
        related
    }

    /// BFS shortest path following outgoing edges. Returns `None` if no path exists.
    pub fn find_path(&self, session_id: &str, from: &str, to: &str) -> Option<Vec<PathEdge>> {
        let session = self.sessions.get(session_id)?;
        let &from_idx = session.name_to_idx.get(from)?;
        let &to_idx   = session.name_to_idx.get(to)?;

        if from_idx == to_idx { return Some(vec![]); }

        let mut parent: HashMap<NodeIndex, (NodeIndex, String)> = HashMap::new();
        let mut queue = VecDeque::new();
        queue.push_back(from_idx);

        'bfs: while let Some(current) = queue.pop_front() {
            for edge in session.graph.edges_directed(current, Direction::Outgoing) {
                let next = edge.target();
                if parent.contains_key(&next) { continue; }
                parent.insert(next, (current, edge.weight().relationship_type.clone()));
                if next == to_idx { break 'bfs; }
                queue.push_back(next);
            }
        }

        if !parent.contains_key(&to_idx) { return None; }

        let mut path = Vec::new();
        let mut node = to_idx;
        while node != from_idx {
            let (prev, rel) = parent.remove(&node).unwrap();
            path.push(PathEdge {
                from: session.graph[prev].name.clone(),
                relationship_type: rel,
                to: session.graph[node].name.clone(),
            });
            node = prev;
        }
        path.reverse();
        Some(path)
    }

    pub fn all_entities(&self, session_id: &str) -> Vec<Entity> {
        let Some(session) = self.sessions.get(session_id) else { return vec![] };
        session.graph.node_indices()
            .map(|idx| {
                let n = &session.graph[idx];
                Entity { name: n.name.clone(), entity_type: n.entity_type.clone(), attributes: n.attributes.clone() }
            })
            .collect()
    }

    pub fn all_relationships(&self, session_id: &str) -> Vec<Relationship> {
        let Some(session) = self.sessions.get(session_id) else { return vec![] };
        session.graph.edge_indices()
            .map(|eidx| {
                let (src, tgt) = session.graph.edge_endpoints(eidx).unwrap();
                Relationship {
                    from: session.graph[src].name.clone(),
                    to:   session.graph[tgt].name.clone(),
                    relationship_type: session.graph[eidx].relationship_type.clone(),
                }
            })
            .collect()
    }

    pub fn delete_session(&mut self, session_id: &str) {
        self.sessions.remove(session_id);
        let prefix = format!("{}\x00", session_id);
        self.processed.retain(|k| !k.starts_with(&prefix));
    }
}

impl Default for KnowledgeGraph {
    fn default() -> Self { Self::new() }
}

/// Portable snapshot of the knowledge graph for inclusion in Raft snapshots.
/// Expanded in Task 6 with full entity/relationship serialization.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphSnapshot;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::types::{Entity, Relationship};
    use std::collections::HashMap;

    fn entity(name: &str, t: &str) -> Entity {
        Entity { name: name.into(), entity_type: t.into(), attributes: HashMap::new() }
    }
    fn rel(from: &str, to: &str, t: &str) -> Relationship {
        Relationship { from: from.into(), to: to.into(), relationship_type: t.into() }
    }

    #[test]
    fn who_works_at_openai() {
        let mut kg = KnowledgeGraph::new();
        kg.apply_extraction("s1", "m1",
            vec![entity("Alice","Person"), entity("OpenAI","Organization")],
            vec![rel("Alice","OpenAI","works_at")]);
        kg.apply_extraction("s1", "m2",
            vec![entity("Bob","Person"), entity("OpenAI","Organization")],
            vec![rel("Bob","OpenAI","works_at")]);

        let related = kg.get_related("s1", "OpenAI");
        let workers: Vec<&str> = related.iter()
            .filter(|r| r.relationship_type == "works_at" && matches!(r.direction, RelationshipDirection::Incoming))
            .map(|r| r.name.as_str())
            .collect();

        assert!(workers.contains(&"Alice"), "Alice should work at OpenAI");
        assert!(workers.contains(&"Bob"),   "Bob should work at OpenAI");
    }

    #[test]
    fn who_does_alice_know() {
        let mut kg = KnowledgeGraph::new();
        kg.apply_extraction("s1", "m1",
            vec![entity("Alice","Person"), entity("Bob","Person")],
            vec![rel("Alice","Bob","knows")]);

        let related = kg.get_related("s1", "Alice");
        let known: Vec<&str> = related.iter()
            .filter(|r| r.relationship_type == "knows" && matches!(r.direction, RelationshipDirection::Outgoing))
            .map(|r| r.name.as_str())
            .collect();

        assert!(known.contains(&"Bob"), "Alice should know Bob");
    }

    #[test]
    fn path_alice_to_bob_direct() {
        let mut kg = KnowledgeGraph::new();
        kg.apply_extraction("s1", "m1",
            vec![entity("Alice","Person"), entity("Bob","Person")],
            vec![rel("Alice","Bob","knows")]);

        let path = kg.find_path("s1", "Alice", "Bob").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].relationship_type, "knows");
        assert_eq!(path[0].from, "Alice");
        assert_eq!(path[0].to, "Bob");
    }

    #[test]
    fn path_alice_to_bob_via_openai() {
        let mut kg = KnowledgeGraph::new();
        kg.apply_extraction("s1", "m1",
            vec![entity("Alice","Person"), entity("OpenAI","Organization")],
            vec![rel("Alice","OpenAI","works_at")]);
        kg.apply_extraction("s1", "m2",
            vec![entity("Bob","Person"), entity("OpenAI","Organization")],
            vec![rel("OpenAI","Bob","employs")]);

        let path = kg.find_path("s1", "Alice", "Bob").unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].from, "Alice");
        assert_eq!(path[1].to, "Bob");
    }

    #[test]
    fn apply_extraction_is_idempotent() {
        let mut kg = KnowledgeGraph::new();
        assert!(kg.apply_extraction("s1", "m1", vec![entity("Alice","Person")], vec![]));
        assert!(!kg.apply_extraction("s1", "m1", vec![entity("Alice","Person")], vec![]));
        assert_eq!(kg.all_entities("s1").len(), 1);
    }

    #[test]
    fn delete_session_removes_all_data_and_resets_dedup() {
        let mut kg = KnowledgeGraph::new();
        kg.apply_extraction("s1", "m1",
            vec![entity("Alice","Person"), entity("Bob","Person")],
            vec![rel("Alice","Bob","knows")]);
        kg.delete_session("s1");
        assert!(kg.all_entities("s1").is_empty());
        assert!(kg.apply_extraction("s1", "m1", vec![entity("Alice","Person")], vec![]));
    }

    #[test]
    fn get_related_empty_for_unknown_entity() {
        let kg = KnowledgeGraph::new();
        assert!(kg.get_related("s1", "nobody").is_empty());
    }

    #[test]
    fn find_path_returns_none_when_no_path_exists() {
        let mut kg = KnowledgeGraph::new();
        kg.apply_extraction("s1", "m1", vec![entity("Alice","Person")], vec![]);
        kg.apply_extraction("s1", "m2", vec![entity("Bob","Person")], vec![]);
        assert!(kg.find_path("s1", "Alice", "Bob").is_none());
    }

    #[test]
    fn session_isolation_prevents_cross_session_queries() {
        let mut kg = KnowledgeGraph::new();
        kg.apply_extraction("s1", "m1", vec![entity("Alice","Person")], vec![]);
        assert!(kg.all_entities("s2").is_empty());
    }
}
