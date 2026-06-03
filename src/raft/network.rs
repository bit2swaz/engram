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

impl RaftNetwork<TypeConfig> for EngRaftNetworkConnection {
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client().await?;
        let proto_req: crate::proto::raft::VoteRequest = (&rpc).into();
        let resp = client
            .vote(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        resp.into_inner()
            .try_into()
            .map_err(|e: String| RPCError::Network(NetworkError::new(&io::Error::new(io::ErrorKind::InvalidData, e))))
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client().await?;
        let proto_req: crate::proto::raft::AppendEntriesRequest = (&rpc).into();
        let resp = client
            .append_entries(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        resp.into_inner()
            .try_into()
            .map_err(|e: String| RPCError::Network(NetworkError::new(&io::Error::new(io::ErrorKind::InvalidData, e))))
    }

    // Stage 1: InstallSnapshot is not implemented.
    // If OpenRaft calls this, it means a follower has fallen too far behind for
    // log-based catch-up. The operator must manually remove and re-add the node.
    // Stage 2 will implement snapshot transport.
    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::Unsupported,
            "InstallSnapshot not implemented in Stage 1; re-add the node manually",
        ))))
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
}
