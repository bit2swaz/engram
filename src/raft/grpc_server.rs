use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::proto::raft::{
    raft_service_server::RaftService,
    AppendEntriesRequest, AppendEntriesResponse,
    VoteRequest, VoteResponse,
};
use crate::raft::types::RaftHandle;

pub struct RaftGrpcServer {
    pub raft: Arc<RaftHandle>,
}

#[tonic::async_trait]
impl RaftService for RaftGrpcServer {
    async fn vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let req = request
            .into_inner()
            .try_into()
            .map_err(|e: String| Status::invalid_argument(e))?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new((&resp).into()))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request
            .into_inner()
            .try_into()
            .map_err(|e: String| Status::invalid_argument(e))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new((&resp).into()))
    }
}
