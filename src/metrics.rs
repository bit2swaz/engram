use prometheus::{
    Encoder, HistogramOpts, HistogramTimer, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    Opts, Registry, TextEncoder,
};

pub const DEFAULT_EMBEDDING_MODEL_LABEL: &str = "default";
pub const DEFAULT_VECTOR_STORE_LABEL: &str = "vector_store";

pub struct AppMetrics {
    pub registry: Registry,
    messages_added_total: IntCounterVec,
    context_requests_total: IntCounter,
    embedding_duration_seconds: HistogramVec,
    vector_search_duration_seconds: HistogramVec,
    short_term_store_errors_total: IntCounterVec,
    embedding_queue_size: IntGauge,
    pub raft_term: IntGauge,
    pub raft_commit_index: IntGauge,
    pub raft_is_leader: IntGauge,
    pub raft_leader_changes_total: IntCounter,
    knowledge_extraction_duration_seconds: HistogramVec,
    knowledge_entities_extracted_total: IntCounter,
    knowledge_relationships_extracted_total: IntCounter,
    knowledge_queue_size: IntGauge,
}

impl AppMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new_custom(Some("engram".to_string()), None)?;

        let messages_added_total = IntCounterVec::new(
            Opts::new(
                "memory_messages_added_total",
                "Total number of messages accepted by the add-message endpoint.",
            ),
            &["role"],
        )?;
        registry.register(Box::new(messages_added_total.clone()))?;

        let context_requests_total = IntCounter::with_opts(Opts::new(
            "memory_context_requests_total",
            "Total number of context assembly requests.",
        ))?;
        registry.register(Box::new(context_requests_total.clone()))?;

        let embedding_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "memory_embedding_duration_seconds",
                "Duration of embedding generation requests in seconds.",
            ),
            &["model"],
        )?;
        registry.register(Box::new(embedding_duration_seconds.clone()))?;

        let vector_search_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "memory_vector_search_duration_seconds",
                "Duration of vector search requests in seconds.",
            ),
            &["store"],
        )?;
        registry.register(Box::new(vector_search_duration_seconds.clone()))?;

        let short_term_store_errors_total = IntCounterVec::new(
            Opts::new(
                "memory_short_term_store_errors_total",
                "Total number of short-term store operation errors.",
            ),
            &["operation"],
        )?;
        registry.register(Box::new(short_term_store_errors_total.clone()))?;

        let embedding_queue_size = IntGauge::with_opts(Opts::new(
            "memory_embedding_queue_size",
            "Current number of pending embedding jobs.",
        ))?;
        registry.register(Box::new(embedding_queue_size.clone()))?;

        let raft_term = IntGauge::with_opts(Opts::new(
            "raft_term",
            "Current Raft term on this node.",
        ))?;
        registry.register(Box::new(raft_term.clone()))?;

        let raft_commit_index = IntGauge::with_opts(Opts::new(
            "raft_commit_index",
            "Index of the last log entry applied to the state machine.",
        ))?;
        registry.register(Box::new(raft_commit_index.clone()))?;

        let raft_is_leader = IntGauge::with_opts(Opts::new(
            "raft_is_leader",
            "1 if this node is the current Raft leader, 0 otherwise.",
        ))?;
        registry.register(Box::new(raft_is_leader.clone()))?;

        let raft_leader_changes_total = IntCounter::with_opts(Opts::new(
            "raft_leader_changes_total",
            "Total number of Raft leader changes observed by this node.",
        ))?;
        registry.register(Box::new(raft_leader_changes_total.clone()))?;

        let knowledge_extraction_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "knowledge_extraction_duration_seconds",
                "Duration of knowledge extraction calls in seconds.",
            ),
            &["model"],
        )?;
        registry.register(Box::new(knowledge_extraction_duration_seconds.clone()))?;

        let knowledge_entities_extracted_total = IntCounter::with_opts(Opts::new(
            "knowledge_entities_extracted_total",
            "Total entities extracted from messages.",
        ))?;
        registry.register(Box::new(knowledge_entities_extracted_total.clone()))?;

        let knowledge_relationships_extracted_total = IntCounter::with_opts(Opts::new(
            "knowledge_relationships_extracted_total",
            "Total relationships extracted from messages.",
        ))?;
        registry.register(Box::new(knowledge_relationships_extracted_total.clone()))?;

        let knowledge_queue_size = IntGauge::with_opts(Opts::new(
            "knowledge_queue_size",
            "Current number of pending knowledge extraction jobs.",
        ))?;
        registry.register(Box::new(knowledge_queue_size.clone()))?;

        Ok(Self {
            registry,
            messages_added_total,
            context_requests_total,
            embedding_duration_seconds,
            vector_search_duration_seconds,
            short_term_store_errors_total,
            embedding_queue_size,
            raft_term,
            raft_commit_index,
            raft_is_leader,
            raft_leader_changes_total,
            knowledge_extraction_duration_seconds,
            knowledge_entities_extracted_total,
            knowledge_relationships_extracted_total,
            knowledge_queue_size,
        })
    }

    pub fn increment_messages_added(&self, role: &str) {
        self.messages_added_total.with_label_values(&[role]).inc();
    }

    pub fn increment_context_requests(&self) {
        self.context_requests_total.inc();
    }

    pub fn start_embedding_timer(&self, model: &str) -> HistogramTimer {
        self.embedding_duration_seconds
            .with_label_values(&[model])
            .start_timer()
    }

    pub fn observe_embedding_duration(&self, model: &str, seconds: f64) {
        self.embedding_duration_seconds
            .with_label_values(&[model])
            .observe(seconds);
    }

    pub fn start_vector_search_timer(&self, store: &str) -> HistogramTimer {
        self.vector_search_duration_seconds
            .with_label_values(&[store])
            .start_timer()
    }

    pub fn observe_vector_search_duration(&self, store: &str, seconds: f64) {
        self.vector_search_duration_seconds
            .with_label_values(&[store])
            .observe(seconds);
    }

    pub fn increment_short_term_store_error(&self, operation: &str) {
        self.short_term_store_errors_total
            .with_label_values(&[operation])
            .inc();
    }

    pub fn set_embedding_queue_size(&self, size: usize) {
        self.embedding_queue_size.set(size as i64);
    }

    pub fn embedding_queue_size(&self) -> i64 {
        self.embedding_queue_size.get()
    }

    pub fn start_knowledge_extraction_timer(&self) -> HistogramTimer {
        self.knowledge_extraction_duration_seconds.with_label_values(&["gpt-4o-mini"]).start_timer()
    }

    pub fn increment_knowledge_entities(&self, count: u64) {
        self.knowledge_entities_extracted_total.inc_by(count);
    }

    pub fn increment_knowledge_relationships(&self, count: u64) {
        self.knowledge_relationships_extracted_total.inc_by(count);
    }

    pub fn set_knowledge_queue_size(&self, size: usize) {
        self.knowledge_queue_size.set(size as i64);
    }

    pub fn render(&self) -> Result<String, String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder
            .encode(&self.registry.gather(), &mut buffer)
            .map_err(|error| error.to_string())?;

        String::from_utf8(buffer).map_err(|error| error.to_string())
    }
}