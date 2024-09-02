package org.disalg.met.api;

public enum ModelAction {
    // set init state
    SetInitState,

    // external events
    NodeCrash,
    NodeStart,
    PartitionStart,
    PartitionRecover,

    ClientGetData,

    // election & discovery
    FLENotmsgTimeout, FLEHandleNotmsg, FLEReceiveNotmsg, FLEWaitNewNotmsg,
    Election,
    ElectionAndDiscovery,

    // sync
    FollowerProcessLEADERINFO,
    LeaderProcessACKEPOCH,
    LeaderSyncFollower,
    FollowerProcessSyncMessage, FollowerProcessDIFF, FollowerProcessTRUNC, FollowerProcessSNAP,
    FollowerProcessPROPOSALInSync,
    FollowerProcessCOMMITInSync,
    FollowerProcessNEWLEADER, LearnerHandlerReadRecord,
    FollowerProcessNEWLEADERAfterCurrentEpochUpdated, FollowerLogRequestWhenProcessingNEWLEADER,
    LeaderProcessACKLD,
    FollowerProcessUPTODATE,

    // broadcast with sub-actions
    LeaderProcessRequest, LeaderLog,
    LeaderProcessACK, FollowerToLeaderACK, LeaderCommit,
    FollowerProcessPROPOSAL, LeaderToFollowerProposal,
    FollowerSyncProcessorLogRequest, FollowerLog,// follower here also needs to LogPROPOSAL
    FollowerProcessCOMMIT, LeaderToFollowerCOMMIT,
    FollowerCommitProcessorCommit, FollowerCommit// follower here also needs to ProcessCOMMIT

}
