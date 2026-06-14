use std::io;

use openraft::{
    BasicNode,
    RaftNetwork, RaftNetworkFactory,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse,
        InstallSnapshotRequest, InstallSnapshotResponse,
        VoteRequest, VoteResponse,
    },
};
use tonic::transport::Channel;

use crate::proto::raft::raft_service_client::RaftServiceClient;
use crate::raft::types::TypeConfig;

pub struct EngRaftNetwork;

impl RaftNetworkFactory<TypeConfig> for EngRaftNetwork {
    type Network = EngRaftNetworkConnection;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        // Do NOT connect here because tonic channels are lazy. Connect inside each RPC call.
        EngRaftNetworkConnection { target_addr: node.addr.clone() }
    }
}

pub struct EngRaftNetworkConnection {
    pub target_addr: String,
}

impl EngRaftNetworkConnection {
    async fn client(
        &self,
    ) -> Result<RaftServiceClient<Channel>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let endpoint = format!("http://{}", self.target_addr)
            .parse::<tonic::transport::Uri>()
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let channel = Channel::builder(endpoint)
            .connect()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(RaftServiceClient::new(channel))
    }
}

fn proto_decode_err(msg: String) -> RPCError<u64, BasicNode, RaftError<u64>> {
    RPCError::Network(NetworkError::new(&io::Error::new(io::ErrorKind::InvalidData, msg)))
}

impl RaftNetwork<TypeConfig> for EngRaftNetworkConnection {
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client().await?;
        let resp = client
            .vote(crate::proto::raft::VoteRequest::from(&rpc))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        resp.into_inner().try_into().map_err(proto_decode_err)
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client().await?;
        let resp = client
            .append_entries(crate::proto::raft::AppendEntriesRequest::from(&rpc))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        resp.into_inner().try_into().map_err(proto_decode_err)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let endpoint = format!("http://{}", self.target_addr)
            .parse::<tonic::transport::Uri>()
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let channel = Channel::builder(endpoint)
            .connect()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let mut client = RaftServiceClient::new(channel);
        let resp = client
            .install_snapshot(crate::proto::raft::InstallSnapshotRequest::from(&rpc))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        resp.into_inner()
            .try_into()
            .map_err(|e: String| RPCError::Network(NetworkError::new(&io::Error::new(io::ErrorKind::InvalidData, e))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::RaftNetworkFactory;

    #[tokio::test]
    async fn can_create_network_client_without_connecting() {
        let mut factory = EngRaftNetwork;
        let node = openraft::BasicNode::new("127.0.0.1:19000");
        // Tonic uses lazy channels so new_client must not attempt a real connection.
        let _conn = factory.new_client(1u64, &node).await;
    }

    #[tokio::test]
    async fn install_snapshot_attempts_connection_and_errors_when_unreachable() {
        let mut conn = EngRaftNetworkConnection { target_addr: "127.0.0.1:1".to_string() };
        let req = openraft::raft::InstallSnapshotRequest::<crate::raft::types::TypeConfig> {
            vote: openraft::Vote::new_committed(1, 1),
            meta: openraft::SnapshotMeta {
                last_log_id: None,
                last_membership: openraft::StoredMembership::default(),
                snapshot_id: "x".into(),
            },
            offset: 0,
            data: vec![],
            done: true,
        };
        // Port 1 is unreachable: must return an RPCError, not the Stage-1 "Unsupported".
        let res = conn.install_snapshot(req, openraft::network::RPCOption::new(std::time::Duration::from_millis(100))).await;
        assert!(res.is_err());
    }
}
