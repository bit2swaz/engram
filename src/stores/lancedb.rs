use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow_array::Array;
use arrow_array::types::Float32Type;
use arrow_array::{
    FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use lancedb::connection::Connection;
use lancedb::index::scalar::BTreeIndexBuilder;
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::index::{Index, IndexType};
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{DistanceType, Table, connect};

use crate::core::{SearchResult, StoreError, VectorStore};

const DEFAULT_TABLE_NAME: &str = "memories";
const MIN_VECTOR_INDEX_ROWS: usize = 256;

pub struct LanceDBStore {
    db: Connection,
    table_name: String,
    embedding_dimension: i32,
    next_id: AtomicI64,
}

impl LanceDBStore {
    pub async fn connect(path: impl AsRef<Path>, embedding_dimension: usize) -> Result<Self, StoreError> {
        let uri = path
            .as_ref()
            .to_str()
            .ok_or_else(|| StoreError::Message("LanceDB path must be valid UTF-8".to_string()))?;
        let embedding_dimension = i32::try_from(embedding_dimension).map_err(|_| {
            StoreError::Message("embedding dimension exceeds supported range".to_string())
        })?;
        let db = connect(uri).execute().await.map_err(store_error)?;
        let table_name = DEFAULT_TABLE_NAME.to_string();
        let table = ensure_table(&db, &table_name, embedding_dimension).await?;
        ensure_indexes(&table).await?;
        let next_id = load_next_id(&table).await?;

        Ok(Self {
            db,
            table_name,
            embedding_dimension,
            next_id: AtomicI64::new(next_id),
        })
    }

    async fn open_table(&self) -> Result<Table, StoreError> {
        self.db
            .open_table(&self.table_name)
            .execute()
            .await
            .map_err(store_error)
    }

    fn next_row_id(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn contains_message_id(
        &self,
        table: &Table,
        message_id: &str,
    ) -> Result<bool, StoreError> {
        let filter = format!("message_id = '{}'", escape_sql_string(message_id));
        let mut stream = table
            .query()
            .only_if(filter)
            .limit(1)
            .execute()
            .await
            .map_err(store_error)?;

        Ok(stream
            .try_next()
            .await
            .map_err(store_error)?
            .is_some_and(|batch| batch.num_rows() > 0))
    }
}

#[async_trait]
impl VectorStore for LanceDBStore {
    async fn insert(
        &self,
        session_id: &str,
        text: &str,
        embedding: Vec<f32>,
        message_id: &str,
    ) -> Result<(), StoreError> {
        let table = self.open_table().await?;
        if self.contains_message_id(&table, message_id).await? {
            return Ok(());
        }

        let row_id = self.next_row_id();
        let created_at = Utc::now().timestamp_micros();
        let reader = make_reader(
            row_id,
            session_id,
            message_id,
            text,
            &embedding,
            created_at,
            self.embedding_dimension,
        )?;

        table.add(reader).execute().await.map_err(store_error)?;
        ensure_indexes(&table).await?;

        Ok(())
    }

    async fn search(
        &self,
        session_id: &str,
        query_embedding: &[f32],
        top_k: usize,
    ) -> Result<Vec<SearchResult>, StoreError> {
        if top_k == 0 {
            return Ok(Vec::new());
        }
        if query_embedding.len() != self.embedding_dimension as usize {
            return Err(StoreError::Message(format!(
                "query embedding must have dimension {}, got {}",
                self.embedding_dimension,
                query_embedding.len()
            )));
        }

        let table = self.open_table().await?;
        let filter = format!("session_id = '{}'", escape_sql_string(session_id));
        let batches = table
            .query()
            .only_if(filter)
            .nearest_to(query_embedding)
            .map_err(store_error)?
            .distance_type(DistanceType::Cosine)
            .limit(top_k)
            .execute()
            .await
            .map_err(store_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(store_error)?;

        let mut results = Vec::new();
        for batch in batches {
            let texts = batch
                .column_by_name("text")
                .ok_or_else(|| {
                    StoreError::Message("search results missing text column".to_string())
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    StoreError::Message(
                        "search results text column had unexpected type".to_string(),
                    )
                })?;
            let distances = batch
                .column_by_name("_distance")
                .ok_or_else(|| {
                    StoreError::Message("search results missing _distance column".to_string())
                })?
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    StoreError::Message(
                        "search results _distance column had unexpected type".to_string(),
                    )
                })?;

            for (text, distance) in texts.iter().zip(distances.iter()).take(batch.num_rows()) {
                if let (Some(text), Some(distance)) = (text, distance) {
                    results.push(SearchResult {
                        text: text.to_string(),
                        score: 1.0 - distance,
                    });
                }
            }
        }

        Ok(results)
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), StoreError> {
        let table = self.open_table().await?;
        let filter = format!("session_id = '{}'", escape_sql_string(session_id));
        table.delete(&filter).await.map_err(store_error)?;
        Ok(())
    }
}

async fn ensure_table(
    db: &Connection,
    table_name: &str,
    embedding_dimension: i32,
) -> Result<Table, StoreError> {
    let table_names = db.table_names().execute().await.map_err(store_error)?;
    if table_names.iter().any(|name| name == table_name) {
        db.open_table(table_name)
            .execute()
            .await
            .map_err(store_error)
    } else {
        db.create_empty_table(table_name, schema(embedding_dimension))
            .execute()
            .await
            .map_err(store_error)
    }
}

async fn ensure_indexes(table: &Table) -> Result<(), StoreError> {
    let existing_indices = table.list_indices().await.map_err(store_error)?;
    let row_count = table.count_rows(None).await.map_err(store_error)?;
    let has_session_index = existing_indices.iter().any(|index| {
        index.index_type == IndexType::BTree && index.columns == ["session_id".to_string()]
    });
    let has_embedding_index = existing_indices.iter().any(|index| {
        index.index_type == IndexType::IvfPq && index.columns == ["embedding".to_string()]
    });

    if !has_session_index {
        table
            .create_index(&["session_id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .map_err(store_error)?;
    }

    if !has_embedding_index && row_count >= MIN_VECTOR_INDEX_ROWS {
        let num_partitions = ((row_count as f64).sqrt().floor() as u32).max(1).min(256);
        table
            .create_index(
                &["embedding"],
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .distance_type(DistanceType::Cosine)
                        .num_partitions(num_partitions),
                ),
            )
            .execute()
            .await
            .map_err(store_error)?;
    }

    Ok(())
}

async fn load_next_id(table: &Table) -> Result<i64, StoreError> {
    let batches = table
        .query()
        .execute()
        .await
        .map_err(store_error)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(store_error)?;

    let max_id = batches
        .iter()
        .filter_map(|batch| batch.column_by_name("id"))
        .flat_map(|column| {
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .into_iter()
                .flat_map(|array| array.iter().flatten())
        })
        .max()
        .unwrap_or(0);

    Ok(max_id + 1)
}

fn make_reader(
    row_id: i64,
    session_id: &str,
    message_id: &str,
    text: &str,
    embedding: &[f32],
    created_at: i64,
    embedding_dimension: i32,
) -> Result<Box<dyn RecordBatchReader + Send>, StoreError> {
    if embedding.len() != embedding_dimension as usize {
        return Err(StoreError::Message(format!(
            "embedding must have dimension {}, got {}",
            embedding_dimension,
            embedding.len()
        )));
    }

    let schema = schema(embedding_dimension);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![row_id])),
            Arc::new(StringArray::from(vec![session_id])),
            Arc::new(StringArray::from(vec![message_id])),
            Arc::new(StringArray::from(vec![text])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(
                        embedding.iter().copied().map(Some).collect::<Vec<_>>(),
                    )],
                    embedding_dimension,
                ),
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![created_at])),
        ],
    )
    .map_err(store_error)?;

    Ok(Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    )))
}

fn schema(embedding_dimension: i32) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("message_id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                embedding_dimension,
            ),
            true,
        ),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn store_error(error: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::Other(Box::new(error))
}
