use std::collections::{HashMap, HashSet, VecDeque};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::{Deserialize, Serialize};

use crate::knowledge::graph::{PathEdge, RelatedEntity, RelationshipDirection};
use crate::knowledge::types::{Entity, Relationship};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Visibility {
    Private,
    Shared,
}

impl Default for Visibility {
    fn default() -> Self {
        Visibility::Private
    }
}

#[derive(Debug, Clone)]
struct AttrValue {
    value: String,
    index: u64,
}

#[derive(Debug, Clone)]
struct GlobalNode {
    name: String,
    entity_type: String,
    entity_type_index: u64,
    attributes: HashMap<String, AttrValue>,
    /// session_id -> agent_id (None if no agent was specified for that session)
    provenance: HashMap<String, Option<String>>,
}

#[derive(Debug, Clone)]
struct GlobalEdge {
    relationship_type: String,
    sources: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    pub from: String,
    pub relationship_type: String,
    pub targets: Vec<String>,
}

pub struct GlobalGraph {
    graph: DiGraph<GlobalNode, GlobalEdge>,
    name_to_idx: HashMap<String, NodeIndex>,
}

impl GlobalGraph {
    pub fn new() -> Self {
        Self { graph: DiGraph::new(), name_to_idx: HashMap::new() }
    }

    fn ensure_entity(
        &mut self,
        name: &str,
        entity_type: &str,
        session_id: &str,
        agent_id: Option<&str>,
        index: u64,
    ) -> NodeIndex {
        if let Some(&idx) = self.name_to_idx.get(name) {
            let node = &mut self.graph[idx];
            node.provenance.entry(session_id.to_string())
                .or_insert_with(|| agent_id.map(|s| s.to_string()));
            // Update entity_type with LWW; "Other" sentinel is never a real type.
            if entity_type != "Other" && index >= node.entity_type_index {
                node.entity_type = entity_type.to_string();
                node.entity_type_index = index;
            }
            return idx;
        }
        let idx = self.graph.add_node(GlobalNode {
            name: name.to_string(),
            entity_type: entity_type.to_string(),
            entity_type_index: index,
            attributes: HashMap::new(),
            provenance: {
                let mut m = HashMap::new();
                m.insert(session_id.to_string(), agent_id.map(|s| s.to_string()));
                m
            },
        });
        self.name_to_idx.insert(name.to_string(), idx);
        idx
    }

    pub fn merge_with_agent(
        &mut self,
        session_id: &str,
        agent_id: Option<&str>,
        index: u64,
        entities: Vec<Entity>,
        relationships: Vec<Relationship>,
    ) {
        for entity in &entities {
            let idx = self.ensure_entity(&entity.name, &entity.entity_type, session_id, agent_id, index);
            let node = &mut self.graph[idx];
            for (key, value) in &entity.attributes {
                let should_update = match node.attributes.get(key) {
                    Some(existing) => index >= existing.index,
                    None => true,
                };
                if should_update {
                    node.attributes.insert(key.clone(), AttrValue { value: value.clone(), index });
                }
            }
        }

        for rel in &relationships {
            let from_idx = self.ensure_entity(&rel.from, "Other", session_id, agent_id, index);
            let to_idx = self.ensure_entity(&rel.to, "Other", session_id, agent_id, index);

            // Dedup by (from, to, relationship_type); accumulate sources.
            let existing = self.graph
                .edges_directed(from_idx, Direction::Outgoing)
                .find(|e| {
                    e.target() == to_idx
                        && self.graph[e.id()].relationship_type == rel.relationship_type
                })
                .map(|e| e.id());

            if let Some(eidx) = existing {
                self.graph[eidx].sources.insert(session_id.to_string());
            } else {
                let mut sources = HashSet::new();
                sources.insert(session_id.to_string());
                self.graph.add_edge(from_idx, to_idx, GlobalEdge {
                    relationship_type: rel.relationship_type.clone(),
                    sources,
                });
            }
        }
    }

    pub fn merge(
        &mut self,
        session_id: &str,
        index: u64,
        entities: Vec<Entity>,
        relationships: Vec<Relationship>,
    ) {
        self.merge_with_agent(session_id, None, index, entities, relationships);
    }

    pub fn get_related(&self, name: &str) -> Vec<RelatedEntity> {
        let Some(&idx) = self.name_to_idx.get(name) else { return vec![] };
        let mut related = Vec::new();
        for edge in self.graph.edges_directed(idx, Direction::Outgoing) {
            let node = &self.graph[edge.target()];
            related.push(RelatedEntity {
                name: node.name.clone(),
                entity_type: node.entity_type.clone(),
                relationship_type: self.graph[edge.id()].relationship_type.clone(),
                direction: RelationshipDirection::Outgoing,
            });
        }
        for edge in self.graph.edges_directed(idx, Direction::Incoming) {
            let node = &self.graph[edge.source()];
            related.push(RelatedEntity {
                name: node.name.clone(),
                entity_type: node.entity_type.clone(),
                relationship_type: self.graph[edge.id()].relationship_type.clone(),
                direction: RelationshipDirection::Incoming,
            });
        }
        related
    }

    pub fn entity_sources(&self, name: &str) -> Vec<String> {
        let Some(&idx) = self.name_to_idx.get(name) else { return vec![] };
        let mut sources: Vec<String> = self.graph[idx].provenance.keys().cloned().collect();
        sources.sort();
        sources
    }

    pub fn entity_agents(&self, name: &str) -> Vec<String> {
        let Some(&idx) = self.name_to_idx.get(name) else { return vec![] };
        let mut agents: Vec<String> = self.graph[idx]
            .provenance
            .values()
            .filter_map(|a| a.as_ref())
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        agents.sort();
        agents
    }

    pub fn entity_attribute(&self, name: &str, key: &str) -> Option<String> {
        let &idx = self.name_to_idx.get(name)?;
        self.graph[idx].attributes.get(key).map(|v| v.value.clone())
    }

    pub fn all_entities(&self) -> Vec<Entity> {
        self.graph
            .node_indices()
            .map(|idx| {
                let n = &self.graph[idx];
                Entity {
                    name: n.name.clone(),
                    entity_type: n.entity_type.clone(),
                    attributes: n
                        .attributes
                        .iter()
                        .map(|(k, v)| (k.clone(), v.value.clone()))
                        .collect(),
                }
            })
            .collect()
    }

    pub fn all_relationships(&self) -> Vec<Relationship> {
        self.graph
            .edge_indices()
            .map(|eidx| {
                let (src, tgt) = self.graph.edge_endpoints(eidx).unwrap();
                Relationship {
                    from: self.graph[src].name.clone(),
                    to: self.graph[tgt].name.clone(),
                    relationship_type: self.graph[eidx].relationship_type.clone(),
                }
            })
            .collect()
    }

    pub fn find_path(&self, from: &str, to: &str) -> Option<Vec<PathEdge>> {
        let &from_idx = self.name_to_idx.get(from)?;
        let &to_idx = self.name_to_idx.get(to)?;

        if from_idx == to_idx {
            return Some(vec![]);
        }

        let mut parent: HashMap<NodeIndex, (NodeIndex, String)> = HashMap::new();
        let mut queue = VecDeque::new();
        queue.push_back(from_idx);

        'bfs: while let Some(current) = queue.pop_front() {
            for edge in self.graph.edges_directed(current, Direction::Outgoing) {
                let next = edge.target();
                if parent.contains_key(&next) {
                    continue;
                }
                parent.insert(next, (current, self.graph[edge.id()].relationship_type.clone()));
                if next == to_idx {
                    break 'bfs;
                }
                queue.push_back(next);
            }
        }

        if !parent.contains_key(&to_idx) {
            return None;
        }

        let mut path = Vec::new();
        let mut node = to_idx;
        while node != from_idx {
            let (prev, rel) = parent.remove(&node).unwrap();
            path.push(PathEdge {
                from: self.graph[prev].name.clone(),
                relationship_type: rel,
                to: self.graph[node].name.clone(),
            });
            node = prev;
        }
        path.reverse();
        Some(path)
    }

    /// Remove all contributions from `session_id`. Elements with no remaining
    /// contributors are dropped. Uses a rebuild to avoid petgraph swap-remove
    /// index invalidation.
    pub fn prune_session(&mut self, session_id: &str) {
        // Collect surviving node data after removing session.
        let nodes_data: Vec<(String, String, u64, HashMap<String, AttrValue>, HashMap<String, Option<String>>)> =
            self.graph
                .node_indices()
                .map(|idx| {
                    let n = &self.graph[idx];
                    let mut prov = n.provenance.clone();
                    prov.remove(session_id);
                    (n.name.clone(), n.entity_type.clone(), n.entity_type_index, n.attributes.clone(), prov)
                })
                .filter(|(_, _, _, _, prov)| !prov.is_empty())
                .collect();

        let surviving: HashSet<&str> = nodes_data.iter().map(|(n, _, _, _, _)| n.as_str()).collect();

        let edges_data: Vec<(String, String, String, HashSet<String>)> = self
            .graph
            .edge_indices()
            .map(|eidx| {
                let (src, tgt) = self.graph.edge_endpoints(eidx).unwrap();
                let mut sources = self.graph[eidx].sources.clone();
                sources.remove(session_id);
                (
                    self.graph[src].name.clone(),
                    self.graph[tgt].name.clone(),
                    self.graph[eidx].relationship_type.clone(),
                    sources,
                )
            })
            .filter(|(from, to, _, sources)| {
                !sources.is_empty()
                    && surviving.contains(from.as_str())
                    && surviving.contains(to.as_str())
            })
            .collect();

        // Rebuild from surviving data.
        self.graph = DiGraph::new();
        self.name_to_idx = HashMap::new();

        for (name, entity_type, entity_type_index, attributes, provenance) in &nodes_data {
            let idx = self.graph.add_node(GlobalNode {
                name: name.clone(),
                entity_type: entity_type.clone(),
                entity_type_index: *entity_type_index,
                attributes: attributes.clone(),
                provenance: provenance.clone(),
            });
            self.name_to_idx.insert(name.clone(), idx);
        }

        for (from, to, rel_type, sources) in &edges_data {
            if let (Some(&fi), Some(&ti)) = (self.name_to_idx.get(from), self.name_to_idx.get(to)) {
                self.graph.add_edge(fi, ti, GlobalEdge {
                    relationship_type: rel_type.clone(),
                    sources: sources.clone(),
                });
            }
        }
    }

    pub fn conflicts(&self) -> Vec<Conflict> {
        let mut result = Vec::new();
        for idx in self.graph.node_indices() {
            let from_name = &self.graph[idx].name;
            let mut by_type: HashMap<String, Vec<String>> = HashMap::new();
            for edge in self.graph.edges_directed(idx, Direction::Outgoing) {
                let rel_type = self.graph[edge.id()].relationship_type.clone();
                let target = self.graph[edge.target()].name.clone();
                by_type.entry(rel_type).or_default().push(target);
            }
            for (rel_type, targets) in by_type {
                if targets.len() > 1 {
                    result.push(Conflict {
                        from: from_name.clone(),
                        relationship_type: rel_type,
                        targets,
                    });
                }
            }
        }
        result
    }

    pub fn to_snapshot(&self) -> GlobalGraphSnapshot {
        let nodes = self
            .graph
            .node_indices()
            .map(|idx| {
                let n = &self.graph[idx];
                GlobalNodeSnapshot {
                    name: n.name.clone(),
                    entity_type: n.entity_type.clone(),
                    entity_type_index: n.entity_type_index,
                    attributes: n
                        .attributes
                        .iter()
                        .map(|(k, v)| (k.clone(), v.value.clone(), v.index))
                        .collect(),
                    provenance: n.provenance.iter().map(|(s, a)| (s.clone(), a.clone())).collect(),
                }
            })
            .collect();

        let edges = self
            .graph
            .edge_indices()
            .map(|eidx| {
                let (src, tgt) = self.graph.edge_endpoints(eidx).unwrap();
                GlobalEdgeSnapshot {
                    from: self.graph[src].name.clone(),
                    to: self.graph[tgt].name.clone(),
                    relationship_type: self.graph[eidx].relationship_type.clone(),
                    sources: self.graph[eidx].sources.iter().cloned().collect(),
                }
            })
            .collect();

        GlobalGraphSnapshot { nodes, edges }
    }

    pub fn from_snapshot(snap: GlobalGraphSnapshot) -> Self {
        let mut g = GlobalGraph::new();
        for ns in &snap.nodes {
            let attributes = ns
                .attributes
                .iter()
                .map(|(k, v, i)| (k.clone(), AttrValue { value: v.clone(), index: *i }))
                .collect();
            let provenance = ns.provenance.iter().map(|(s, a)| (s.clone(), a.clone())).collect();
            let idx = g.graph.add_node(GlobalNode {
                name: ns.name.clone(),
                entity_type: ns.entity_type.clone(),
                entity_type_index: ns.entity_type_index,
                attributes,
                provenance,
            });
            g.name_to_idx.insert(ns.name.clone(), idx);
        }
        for es in &snap.edges {
            if let (Some(&fi), Some(&ti)) = (g.name_to_idx.get(&es.from), g.name_to_idx.get(&es.to)) {
                g.graph.add_edge(fi, ti, GlobalEdge {
                    relationship_type: es.relationship_type.clone(),
                    sources: es.sources.iter().cloned().collect(),
                });
            }
        }
        g
    }
}

impl Default for GlobalGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalNodeSnapshot {
    pub name: String,
    pub entity_type: String,
    pub entity_type_index: u64,
    /// (key, value, raft_log_index)
    pub attributes: Vec<(String, String, u64)>,
    /// (session_id, agent_id)
    pub provenance: Vec<(String, Option<String>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalEdgeSnapshot {
    pub from: String,
    pub to: String,
    pub relationship_type: String,
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalGraphSnapshot {
    pub nodes: Vec<GlobalNodeSnapshot>,
    pub edges: Vec<GlobalEdgeSnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visibility_defaults_private_and_round_trips() {
        assert_eq!(Visibility::default(), Visibility::Private);
        let j = serde_json::to_string(&Visibility::Shared).unwrap();
        assert_eq!(serde_json::from_str::<Visibility>(&j).unwrap(), Visibility::Shared);
    }
}

#[cfg(test)]
mod graph_tests {
    use super::*;
    use crate::knowledge::types::{Entity, Relationship};
    use std::collections::HashMap;

    fn ent(name: &str, t: &str) -> Entity {
        Entity { name: name.into(), entity_type: t.into(), attributes: HashMap::new() }
    }
    fn ent_attr(name: &str, k: &str, v: &str) -> Entity {
        let mut a = HashMap::new();
        a.insert(k.into(), v.into());
        Entity { name: name.into(), entity_type: "Person".into(), attributes: a }
    }
    fn rel(f: &str, t: &str, ty: &str) -> Relationship {
        Relationship { from: f.into(), to: t.into(), relationship_type: ty.into() }
    }

    #[test]
    fn merges_two_sessions_into_one_global_view() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("Alice", "Person"), ent("OpenAI", "Organization")], vec![rel("Alice", "OpenAI", "works_at")]);
        g.merge("s2", 1, vec![ent("Bob", "Person"), ent("OpenAI", "Organization")], vec![rel("Bob", "OpenAI", "works_at")]);
        let related = g.get_related("OpenAI");
        let names: Vec<&str> = related.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }

    #[test]
    fn provenance_lists_contributing_sessions() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("OpenAI", "Organization")], vec![]);
        g.merge("s2", 1, vec![ent("OpenAI", "Organization")], vec![]);
        let mut sources = g.entity_sources("OpenAI");
        sources.sort();
        assert_eq!(sources, vec!["s1".to_string(), "s2".to_string()]);
    }

    #[test]
    fn attribute_conflict_resolves_last_writer_wins_by_index() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 5, vec![ent_attr("Alice", "role", "eng")], vec![]);
        g.merge("s2", 9, vec![ent_attr("Alice", "role", "manager")], vec![]);
        assert_eq!(g.entity_attribute("Alice", "role").as_deref(), Some("manager"));
        // Lower index applied later must NOT overwrite a higher index.
        g.merge("s3", 7, vec![ent_attr("Alice", "role", "intern")], vec![]);
        assert_eq!(g.entity_attribute("Alice", "role").as_deref(), Some("manager"));
    }

    #[test]
    fn pruning_one_session_keeps_elements_with_other_contributors() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("Alice", "Person")], vec![]);
        g.merge("s2", 1, vec![ent("Alice", "Person")], vec![]);
        g.prune_session("s1");
        assert!(g.all_entities().iter().any(|e| e.name == "Alice"));
        g.prune_session("s2");
        assert!(g.all_entities().is_empty());
    }

    #[test]
    fn pruning_drops_orphaned_edges() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("Alice", "Person"), ent("Bob", "Person")], vec![rel("Alice", "Bob", "knows")]);
        g.prune_session("s1");
        assert!(g.find_path("Alice", "Bob").is_none());
    }

    #[test]
    fn prune_shared_entity_unique_relationship() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("Alice", "Person"), ent("Bob", "Person")], vec![rel("Alice", "Bob", "knows")]);
        g.merge("s2", 1, vec![ent("Alice", "Person")], vec![]);
        g.prune_session("s1");
        assert!(g.all_entities().iter().any(|e| e.name == "Alice"), "Alice survives via s2");
        assert!(g.find_path("Alice", "Bob").is_none(), "the unique s1 edge is gone");
    }

    #[test]
    fn prune_unique_entity_shared_relationship() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("Alice", "Person"), ent("Bob", "Person"), ent("Carol", "Person")], vec![rel("Alice", "Bob", "knows")]);
        g.merge("s2", 1, vec![ent("Alice", "Person"), ent("Bob", "Person")], vec![rel("Alice", "Bob", "knows")]);
        g.prune_session("s1");
        assert!(g.find_path("Alice", "Bob").is_some(), "shared edge survives via s2");
        assert!(!g.all_entities().iter().any(|e| e.name == "Carol"), "unique Carol is gone");
    }

    #[test]
    fn prune_shared_entity_and_shared_relationship() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("Alice", "Person"), ent("Bob", "Person")], vec![rel("Alice", "Bob", "knows")]);
        g.merge("s2", 1, vec![ent("Alice", "Person"), ent("Bob", "Person")], vec![rel("Alice", "Bob", "knows")]);
        g.prune_session("s1");
        assert!(g.find_path("Alice", "Bob").is_some(), "fully shared element survives one prune");
        g.prune_session("s2");
        assert!(g.all_entities().is_empty(), "removing the last contributor clears it");
    }

    #[test]
    fn global_graph_tracks_agent_provenance() {
        let mut g = GlobalGraph::new();
        g.merge_with_agent("s1", Some("agent-7"), 0, vec![ent("OpenAI", "Organization")], vec![]);
        assert_eq!(g.entity_agents("OpenAI"), vec!["agent-7".to_string()]);
    }

    #[test]
    fn agent_provenance_survives_snapshot_round_trip() {
        let mut g = GlobalGraph::new();
        g.merge_with_agent("s1", Some("agent-7"), 0, vec![ent("OpenAI", "Organization")], vec![]);
        let g2 = GlobalGraph::from_snapshot(g.to_snapshot());
        assert_eq!(g2.entity_agents("OpenAI"), vec!["agent-7".to_string()]);
        assert_eq!(g2.entity_sources("OpenAI"), vec!["s1".to_string()]);
    }

    #[test]
    fn contradictions_are_reported() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 0, vec![ent("Alice", "Person"), ent("X", "Organization")], vec![rel("Alice", "X", "works_at")]);
        g.merge("s2", 1, vec![ent("Alice", "Person"), ent("Y", "Organization")], vec![rel("Alice", "Y", "works_at")]);
        let conflicts = g.conflicts();
        assert!(conflicts.iter().any(|c| c.from == "Alice" && c.relationship_type == "works_at"));
    }

    #[test]
    fn snapshot_round_trips_with_provenance() {
        let mut g = GlobalGraph::new();
        g.merge("s1", 3, vec![ent_attr("Alice", "role", "eng"), ent("OpenAI", "Organization")], vec![rel("Alice", "OpenAI", "works_at")]);
        let snap = g.to_snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        let back: GlobalGraphSnapshot = serde_json::from_str(&json).unwrap();
        let g2 = GlobalGraph::from_snapshot(back);
        assert_eq!(g2.entity_sources("OpenAI"), vec!["s1".to_string()]);
        assert_eq!(g2.entity_attribute("Alice", "role").as_deref(), Some("eng"));
        assert!(g2.find_path("Alice", "OpenAI").is_some());
    }
}
