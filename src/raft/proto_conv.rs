use crate::proto::raft as p;
use crate::raft::types::TypeConfig;
use openraft::{
    CommittedLeaderId, Entry, LeaderId, LogId, Vote,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
};

// --- Vote ---

impl From<&Vote<u64>> for p::Vote {
    fn from(v: &Vote<u64>) -> Self {
        p::Vote {
            leader_id: Some(p::LeaderId {
                term: v.leader_id.term,
                node_id: v.leader_id.node_id,
            }),
            committed: v.committed,
        }
    }
}

impl From<p::Vote> for Vote<u64> {
    fn from(v: p::Vote) -> Self {
        let lid = v.leader_id.unwrap_or_default();
        Vote {
            leader_id: LeaderId { term: lid.term, node_id: lid.node_id },
            committed: v.committed,
        }
    }
}

// --- LogId ---

impl From<&LogId<u64>> for p::LogId {
    fn from(l: &LogId<u64>) -> Self {
        p::LogId { term: l.leader_id.term, index: l.index }
    }
}

impl From<p::LogId> for LogId<u64> {
    fn from(l: p::LogId) -> Self {
        // node_id is not transmitted in LogId (term + index are sufficient for ordering).
        LogId::new(CommittedLeaderId::new(l.term, 0), l.index)
    }
}

// --- Entry ---

impl From<&Entry<TypeConfig>> for p::Entry {
    fn from(e: &Entry<TypeConfig>) -> Self {
        p::Entry {
            log_id: Some((&e.log_id).into()),
            payload: serde_json::to_vec(&e.payload).unwrap_or_default(),
        }
    }
}

impl TryFrom<p::Entry> for Entry<TypeConfig> {
    type Error = String;
    fn try_from(e: p::Entry) -> Result<Self, String> {
        let log_id = e.log_id.map(Into::into).ok_or("entry missing log_id")?;
        let payload = serde_json::from_slice(&e.payload)
            .map_err(|e| format!("entry payload decode error: {e}"))?;
        Ok(Entry { log_id, payload })
    }
}

// --- VoteRequest ---

impl From<&VoteRequest<u64>> for p::VoteRequest {
    fn from(r: &VoteRequest<u64>) -> Self {
        p::VoteRequest {
            vote: Some((&r.vote).into()),
            last_log_id: r.last_log_id.as_ref().map(Into::into),
        }
    }
}

impl TryFrom<p::VoteRequest> for VoteRequest<u64> {
    type Error = String;
    fn try_from(r: p::VoteRequest) -> Result<Self, String> {
        Ok(VoteRequest {
            vote: r.vote.ok_or("vote_request missing vote")?.into(),
            last_log_id: r.last_log_id.map(Into::into),
        })
    }
}

// --- VoteResponse ---

impl From<&VoteResponse<u64>> for p::VoteResponse {
    fn from(r: &VoteResponse<u64>) -> Self {
        p::VoteResponse {
            vote: Some((&r.vote).into()),
            vote_granted: r.vote_granted,
            last_log_id: r.last_log_id.as_ref().map(Into::into),
        }
    }
}

impl TryFrom<p::VoteResponse> for VoteResponse<u64> {
    type Error = String;
    fn try_from(r: p::VoteResponse) -> Result<Self, String> {
        Ok(VoteResponse {
            vote: r.vote.ok_or("vote_response missing vote")?.into(),
            vote_granted: r.vote_granted,
            last_log_id: r.last_log_id.map(Into::into),
        })
    }
}

// --- AppendEntriesRequest ---

impl From<&AppendEntriesRequest<TypeConfig>> for p::AppendEntriesRequest {
    fn from(r: &AppendEntriesRequest<TypeConfig>) -> Self {
        p::AppendEntriesRequest {
            vote: Some((&r.vote).into()),
            prev_log_id: r.prev_log_id.as_ref().map(Into::into),
            entries: r.entries.iter().map(Into::into).collect(),
            leader_commit: r.leader_commit.as_ref().map(Into::into),
        }
    }
}

impl TryFrom<p::AppendEntriesRequest> for AppendEntriesRequest<TypeConfig> {
    type Error = String;
    fn try_from(r: p::AppendEntriesRequest) -> Result<Self, String> {
        Ok(AppendEntriesRequest {
            vote: r.vote.ok_or("append_entries_request missing vote")?.into(),
            prev_log_id: r.prev_log_id.map(Into::into),
            entries: r
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            leader_commit: r.leader_commit.map(Into::into),
        })
    }
}

// --- AppendEntriesResponse ---
//
// AppendEntriesResponse is an enum with four variants. Proto encoding:
//   Success                -> all fields empty
//   PartialSuccess(log_id) -> last_log_id set (None variant indistinguishable from Success on wire)
//   Conflict               -> conflict = true
//   HigherVote(vote)       -> rejected_by set

impl From<&AppendEntriesResponse<u64>> for p::AppendEntriesResponse {
    fn from(r: &AppendEntriesResponse<u64>) -> Self {
        match r {
            AppendEntriesResponse::Success => p::AppendEntriesResponse {
                rejected_by: None,
                conflict: false,
                last_log_id: None,
            },
            AppendEntriesResponse::PartialSuccess(log_id) => p::AppendEntriesResponse {
                rejected_by: None,
                conflict: false,
                last_log_id: log_id.as_ref().map(Into::into),
            },
            AppendEntriesResponse::Conflict => p::AppendEntriesResponse {
                rejected_by: None,
                conflict: true,
                last_log_id: None,
            },
            AppendEntriesResponse::HigherVote(vote) => p::AppendEntriesResponse {
                rejected_by: Some(vote.into()),
                conflict: false,
                last_log_id: None,
            },
        }
    }
}

impl TryFrom<p::AppendEntriesResponse> for AppendEntriesResponse<u64> {
    type Error = String;
    fn try_from(r: p::AppendEntriesResponse) -> Result<Self, String> {
        if let Some(vote) = r.rejected_by {
            Ok(AppendEntriesResponse::HigherVote(vote.into()))
        } else if r.conflict {
            Ok(AppendEntriesResponse::Conflict)
        } else if let Some(log_id) = r.last_log_id {
            Ok(AppendEntriesResponse::PartialSuccess(Some(log_id.into())))
        } else {
            Ok(AppendEntriesResponse::Success)
        }
    }
}

// --- InstallSnapshotRequest ---

impl From<&openraft::raft::InstallSnapshotRequest<TypeConfig>> for p::InstallSnapshotRequest {
    fn from(r: &openraft::raft::InstallSnapshotRequest<TypeConfig>) -> Self {
        p::InstallSnapshotRequest {
            vote: Some((&r.vote).into()),
            meta: serde_json::to_vec(&r.meta).unwrap_or_default(),
            offset: r.offset,
            data: r.data.clone(),
            done: r.done,
        }
    }
}

impl TryFrom<p::InstallSnapshotRequest> for openraft::raft::InstallSnapshotRequest<TypeConfig> {
    type Error = String;
    fn try_from(r: p::InstallSnapshotRequest) -> Result<Self, String> {
        let meta = serde_json::from_slice(&r.meta)
            .map_err(|e| format!("install_snapshot meta decode error: {e}"))?;
        Ok(openraft::raft::InstallSnapshotRequest {
            vote: r.vote.ok_or("install_snapshot_request missing vote")?.into(),
            meta,
            offset: r.offset,
            data: r.data,
            done: r.done,
        })
    }
}

// --- InstallSnapshotResponse ---

impl From<&openraft::raft::InstallSnapshotResponse<u64>> for p::InstallSnapshotResponse {
    fn from(r: &openraft::raft::InstallSnapshotResponse<u64>) -> Self {
        p::InstallSnapshotResponse { vote: Some((&r.vote).into()) }
    }
}

impl TryFrom<p::InstallSnapshotResponse> for openraft::raft::InstallSnapshotResponse<u64> {
    type Error = String;
    fn try_from(r: p::InstallSnapshotResponse) -> Result<Self, String> {
        Ok(openraft::raft::InstallSnapshotResponse {
            vote: r.vote.ok_or("install_snapshot_response missing vote")?.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::raft as proto;
    use openraft::raft::{AppendEntriesResponse, VoteRequest};

    #[test]
    fn install_snapshot_request_round_trips() {
        use openraft::raft::InstallSnapshotRequest;
        use openraft::{SnapshotMeta, StoredMembership};
        let req = InstallSnapshotRequest::<crate::raft::types::TypeConfig> {
            vote: openraft::Vote::new_committed(2, 1),
            meta: SnapshotMeta {
                last_log_id: Some(openraft::LogId::new(openraft::CommittedLeaderId::new(2, 1), 9)),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-1".into(),
            },
            offset: 0,
            data: vec![1, 2, 3],
            done: true,
        };
        let p: proto::InstallSnapshotRequest = (&req).into();
        let back: InstallSnapshotRequest<crate::raft::types::TypeConfig> = p.try_into().unwrap();
        assert_eq!(back.offset, 0);
        assert!(back.done);
        assert_eq!(back.data, vec![1, 2, 3]);
        assert_eq!(back.meta.snapshot_id, "snap-1");
        assert_eq!(back.meta.last_log_id.unwrap().index, 9);
    }

    #[test]
    fn install_snapshot_response_round_trips() {
        use openraft::raft::InstallSnapshotResponse;
        let resp = InstallSnapshotResponse::<u64> { vote: openraft::Vote::new_committed(3, 2) };
        let p: proto::InstallSnapshotResponse = (&resp).into();
        let back: InstallSnapshotResponse<u64> = p.try_into().unwrap();
        assert_eq!(back.vote.leader_id().term, 3);
    }

    #[test]
    fn vote_round_trips() {
        let original = openraft::Vote::<u64>::new_committed(1, 1);
        let p: proto::Vote = (&original).into();
        let back: openraft::Vote<u64> = p.into();
        assert_eq!(back.leader_id(), original.leader_id());
        assert_eq!(back.is_committed(), original.is_committed());
    }

    #[test]
    fn log_id_round_trips() {
        let original = openraft::LogId::new(openraft::CommittedLeaderId::new(3, 2), 10);
        let p: proto::LogId = (&original).into();
        let back: openraft::LogId<u64> = p.into();
        assert_eq!(back.index, original.index);
    }

    #[test]
    fn vote_request_round_trips() {
        let req = VoteRequest {
            vote: openraft::Vote::new(2, 3),
            last_log_id: Some(openraft::LogId::new(openraft::CommittedLeaderId::new(2, 1), 5)),
        };
        let p: proto::VoteRequest = (&req).into();
        let back: VoteRequest<u64> = p.try_into().unwrap();
        assert_eq!(back.vote.leader_id().term, req.vote.leader_id().term);
        assert_eq!(back.vote.leader_id().node_id, req.vote.leader_id().node_id);
        assert_eq!(back.last_log_id.map(|l| l.index), req.last_log_id.map(|l| l.index));
    }

    #[test]
    fn append_entries_response_success_round_trips() {
        let resp = AppendEntriesResponse::<u64>::Success;
        let p: proto::AppendEntriesResponse = (&resp).into();
        let back: AppendEntriesResponse<u64> = p.try_into().unwrap();
        assert!(matches!(back, AppendEntriesResponse::Success));
    }

    #[test]
    fn append_entries_response_conflict_round_trips() {
        let resp = AppendEntriesResponse::<u64>::Conflict;
        let p: proto::AppendEntriesResponse = (&resp).into();
        let back: AppendEntriesResponse<u64> = p.try_into().unwrap();
        assert!(matches!(back, AppendEntriesResponse::Conflict));
    }

    #[test]
    fn append_entries_response_higher_vote_round_trips() {
        let vote = openraft::Vote::new(3, 2);
        let resp = AppendEntriesResponse::HigherVote(vote);
        let p: proto::AppendEntriesResponse = (&resp).into();
        let back: AppendEntriesResponse<u64> = p.try_into().unwrap();
        assert!(matches!(back, AppendEntriesResponse::HigherVote(v) if v.leader_id().term == 3));
    }
}
