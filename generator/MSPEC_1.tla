----------------------------- MODULE MSPEC_1 -----------------------------
(* This is the mixed-grained specification for ZooKeeper version 3.9.1. *)
(* Note: This specification serves as a basic material for developing further 
    test specifications. *)

EXTENDS Integers, FiniteSets, Sequences, Naturals, TLC
-----------------------------------------------------------------------------
\* The set of server identifiers
CONSTANT Server
\* Server states
CONSTANTS LOOKING, FOLLOWING, LEADING
\* Zab states of server
CONSTANTS ELECTION, DISCOVERY, SYNCHRONIZATION, BROADCAST
\* Message types
CONSTANTS FOLLOWERINFO, LEADERINFO, ACKEPOCH, NEWLEADER, ACKLD, UPTODATE, PROPOSAL, ACK, COMMIT
CONSTANTS TRUNC, DIFF
\* [MaxTimeoutFailures, MaxTransactionNum, MaxEpoch, MaxCrashes, MaxPartitions]
CONSTANT Parameters

MAXEPOCH == 10
\* NullPoint == CHOOSE p: p \notin Server
CONSTANT NullPoint, ONLINE, OFFLINE
Quorums == {Q \in SUBSET Server: Cardinality(Q)*2 > Cardinality(Server)}
-----------------------------------------------------------------------------
\* variables

\* Variables that all servers use.
VARIABLES state,          \* State of server, in {LOOKING, FOLLOWING, LEADING}.
          zabState,       \* Current phase of server, in
                          \* {ELECTION, DISCOVERY, SYNCHRONIZATION, BROADCAST}.
          acceptedEpoch,  \* Epoch of the last LEADERINFO packet accepted,
                          \* namely f.p in paper.
          currentEpoch,   \* Epoch of the last NEWLEADER packet accepted,
                          \* namely f.a in paper.
          history,        \* History of servers: sequence of transactions,
                          \* containing: [zxid, value, ackSid, epoch].
          lastCommitted,  \* Maximum index and zxid known to be committed,
                          \* namely 'lastCommitted' in Leader. Starts from 0,
                          \* and increases monotonically before restarting.
          lastProcessed,  \* Index and zxid of the last processed txn.                          
          initialHistory  \* history that server initially has before election.

\* Variables only used for leader.
VARIABLES learners,       \* Set of servers leader connects, 
                          \* namely 'learners' in Leader.
          connecting,     \* Set of learners leader has received 
                          \* FOLLOWERINFO from, namely  
                          \* 'connectingFollowers' in Leader.
                          \* Set of record [sid, connected].
          electing,       \* Set of learners leader has received
                          \* ACKEPOCH from, namely 'electingFollowers'
                          \* in Leader. Set of record 
                          \* [sid, peerLastZxid, inQuorum].
                          \* And peerLastZxid = <<-1,-1>> means has done
                          \* syncFollower with this sid.
                          \* inQuorum = TRUE means in code it is one
                          \* element in 'electingFollowers'.
          ackldRecv,      \* Set of learners leader has received
                          \* ACK of NEWLEADER from, namely
                          \* 'newLeaderProposal' in Leader.
                          \* Set of record [sid, connected].         
          forwarding,     \* Set of learners that are synced with
                          \* leader, namely 'forwardingFollowers'
                          \* in Leader.
          tempMaxEpoch    \* ({Maximum epoch in FOLLOWEINFO} + 1) that 
                          \* leader has received from learners,
                          \* namely 'epoch' in Leader.

\* Variables only used for follower.
VARIABLES leaderAddr, \* If follower has connected with leader.
                      \* If follower lost connection, then null.
          packetsSync \* packets of PROPOSAL and COMMIT from leader,
                      \* namely 'packetsNotCommitted' and
                      \* 'packetsCommitted' in SyncWithLeader
                      \* in Learner.

VARIABLE leaderOracle \* Current leader.
                      \* Currently not used for node to search leader.

\* Variables about network channel.
VARIABLE rcvBuffer  \* Simulates network channel.
                    \* rcvBuffer[i][j] means the receive buffer of server j 
                    \* from server i.

\* Variables about status of cluster network and node presence.
VARIABLES status,    \* Whether the server is online or offline.
          partition  \* network partion.

\* Variables only used in verifying properties.
VARIABLES epochLeader,       \* Set of leaders in every epoch.
          proposalMsgsLog,   \* Set of all broadcast messages.
          committedLog       \* txns committed.
          
VARIABLES daInv,             \* Check invariants during action.
          aaInv              \* Check invariants after action.

VARIABLES doInit     \* Give model a initial state after Init.

\* Variable used for recording critical data,
\* to constrain state space or update values.
VARIABLE recorder \* Consists: members of Parameters and pc, values.
                  \* Form is record: 
                  \* [pc, nTransaction, maxEpoch, nTimeout, nClientRequest,
                  \*  nPartition, nCrash]

serverVars   == <<state, zabState, acceptedEpoch, currentEpoch>>
logVars      == <<history, initialHistory, lastCommitted, lastProcessed>>
disVars      == <<connecting, tempMaxEpoch>>
noDisVars    == <<learners, electing, ackldRecv, forwarding>>
leaderVars   == <<disVars, noDisVars>>
followerVars == <<leaderAddr, packetsSync>>
electionVars == <<leaderOracle>>
netVars      == <<rcvBuffer>>
envVars      == <<status, partition>>
verifyVars   == <<epochLeader, proposalMsgsLog>>
invVars      == <<daInv, aaInv>>
commitVars   == <<committedLog>>
initVars     == <<doInit>>

vars == <<serverVars, logVars, leaderVars, followerVars, electionVars, 
        netVars, envVars, verifyVars, invVars, commitVars, initVars, recorder>>
-----------------------------------------------------------------------------
\* utils

\* Return the maximum value from the set S
Maximum(S) == IF S = {} THEN -1
                        ELSE CHOOSE n \in S: \A m \in S: n >= m

\* Return the minimum value from the set S
Minimum(S) == IF S = {} THEN -1
                        ELSE CHOOSE n \in S: \A m \in S: n <= m

\* Check server state       
IsON(s)  == status[s] = ONLINE 
IsOFF(s) == status[s] = OFFLINE 

IsLeader(s)   == state[s] = LEADING
IsFollower(s) == state[s] = FOLLOWING
IsLooking(s)  == state[s] = LOOKING

IsMyLearner(i, j) == j \in learners[i]
IsMyLeader(i, j)  == leaderAddr[i] = j
HasNoLeader(i)    == leaderAddr[i] = NullPoint
HasLeader(i)      == leaderAddr[i] /= NullPoint

\* Check if s is a quorum
IsQuorum(s) == s \in Quorums

HasPartitioned(i, j) == /\ partition[i][j] = TRUE 
                        /\ partition[j][i] = TRUE

\* FALSE: zxid1 <= zxid2; TRUE: zxid1 > zxid2
ZxidComparePredicate(zxid1, zxid2) == \/ zxid1[1] > zxid2[1]
                                      \/ /\ zxid1[1] = zxid2[1]
                                         /\ zxid1[2] > zxid2[2]

ZxidEqualPredicate(zxid1, zxid2) == /\ zxid1[1] = zxid2[1] 
                                    /\ zxid1[2] = zxid2[2]

TxnZxidEqual(txn, z) == txn.zxid[1] = z[1] /\ txn.zxid[2] = z[2]

TxnEqual(txn1, txn2) == /\ ZxidEqualPredicate(txn1.zxid, txn2.zxid)
                        /\ txn1.value = txn2.value

EpochPrecedeInTxn(txn1, txn2) == txn1.zxid[1] < txn2.zxid[1]

------
\* Actions about recorder
GetParameter(p) == IF p \in DOMAIN Parameters THEN Parameters[p] ELSE 0
GetRecorder(p)  == IF p \in DOMAIN recorder   THEN recorder[p]   ELSE 0

RecorderGetHelper(m) == (m :> recorder[m])
RecorderIncHelper(m) == (m :> recorder[m] + 1)

RecorderIncTimeout == RecorderIncHelper("nTimeout")
RecorderGetTimeout == RecorderGetHelper("nTimeout")
RecorderIncCrash   == RecorderIncHelper("nCrash")
RecorderGetCrash   == RecorderGetHelper("nCrash")

RecorderSetTransactionNum(pc) == ("nTransaction" :> 
                                IF \/ pc[1] = "LeaderProcessRequest"
                                   \/ pc[1] = "SetInitState" THEN
                                    LET s == CHOOSE i \in Server: 
                                        \A j \in Server: Len(history'[i]) >= Len(history'[j])                       
                                    IN Len(history'[s])
                                ELSE recorder["nTransaction"])
RecorderSetMaxEpoch(pc)       == ("maxEpoch" :> 
                                IF \/ pc[1] = "ElectionAndDiscovery"
                                   \/ pc[1] = "SetInitState" THEN
                                    LET s == CHOOSE i \in Server:
                                        \A j \in Server: acceptedEpoch'[i] >= acceptedEpoch'[j]
                                    IN acceptedEpoch'[s]
                                ELSE recorder["maxEpoch"])
RecorderSetRequests(pc)       == ("nClientRequest" :>
                                IF pc[1] = "LeaderProcessRequest" THEN
                                    recorder["nClientRequest"] + 1
                                ELSE recorder["nClientRequest"] )
RecorderSetPartition(pc)      == ("nPartition" :> 
                                IF pc[1] = "PartitionStart" THEN recorder["nPartition"] + 1
                                                            ELSE recorder["nPartition"] )  
RecorderSetConsequentFailure(pc) == ("nConsequentFailure" :> 
                                IF pc[1] \in {"NodeCrash", "PartitionStart"} THEN
                                    recorder["nConsequentFailure"] + 1
                                ELSE IF pc[1] = "ElectionAndDiscovery" THEN 0 
                                     ELSE recorder["nConsequentFailure"] )

RECURSIVE ReleaseNoExecute(_,_)
ReleaseNoExecute(remaining, S) == 
                               IF remaining = {} THEN {}
                               ELSE LET event == CHOOSE e \in remaining: TRUE 
                                        re     == remaining \ {event}
                                    IN CASE event[1] = "NodeCrash" ->
                                                IF event[2] \in S THEN ReleaseNoExecute(re, S)
                                                ELSE {event} \union ReleaseNoExecute(re, S)
                                       []   event[1] = "NodeStart" ->
                                                ReleaseNoExecute(re, S)
                                       []   event[1] = "PartitionStart" ->
                                                IF \E s \in event[2]: s \in S THEN ReleaseNoExecute(re, S)
                                                ELSE {event} \union ReleaseNoExecute(re, S)
                                       []   event[1] = "PartitionRecover" ->
                                                ReleaseNoExecute(re, S)
RecorderSetNoExecute(pc) == ("noExecute" :> 
                                IF pc[1] \in {"NodeCrash", "PartitionStart"} THEN 
                                    CASE pc[1] = "NodeCrash" -> 
                                            LET s == pc[2]
                                            IN IF state[s] = LOOKING 
                                               THEN recorder["noExecute"] \union {<<"NodeStart", s>>, <<"NodeCrash", s>>}
                                               ELSE recorder["noExecute"] \union {<<"NodeCrash", s>>}
                                    []   pc[1] = "PartitionStart" ->
                                            LET s == pc[2]
                                                v == pc[3]
                                            IN IF state[s] = LOOKING /\ state[v] = LOOKING
                                               THEN recorder["noExecute"] \union {<<"PartitionRecover", {s, v}>>,
                                                                                <<"PartitionStart", {s, v}>>}
                                               ELSE recorder["noExecute"] \union {<<"PartitionStart", {s, v}>>}
                                ELSE IF pc[1] = "ElectionAndDiscovery" THEN 
                                        LET l  == pc[2]
                                            fs == pc[3]
                                        IN ReleaseNoExecute(recorder["noExecute"], {l} \union fs)
                                     ELSE recorder["noExecute"] )                        
RecorderSetPc(pc)      == ("pc" :> pc)
RecorderSetFailure(pc) == CASE pc[1] = "Timeout"         -> RecorderIncTimeout @@ RecorderGetCrash
                          []   pc[1] = "LeaderTimeout"   -> RecorderIncTimeout @@ RecorderGetCrash 
                          []   pc[1] = "FollowerTimeout" -> RecorderIncTimeout @@ RecorderGetCrash 
                          []   pc[1] = "PartitionStart"  -> IF IsLooking(pc[2]) 
                                                            THEN RecorderGetTimeout @@ RecorderGetCrash
                                                            ELSE RecorderIncTimeout @@ RecorderGetCrash
                          []   pc[1] = "NodeCrash"       -> IF IsLooking(pc[2]) 
                                                            THEN RecorderGetTimeout @@ RecorderIncCrash 
                                                            ELSE RecorderIncTimeout @@ RecorderIncCrash 
                          []   OTHER                     -> RecorderGetTimeout @@ RecorderGetCrash

UpdateRecorder(pc) == recorder' = RecorderSetPartition(pc) @@ RecorderSetConsequentFailure(pc)
                                  @@ RecorderSetNoExecute(pc)
                                  @@  RecorderSetFailure(pc)  @@ RecorderSetTransactionNum(pc)
                                  @@ RecorderSetMaxEpoch(pc)  @@ RecorderSetPc(pc) 
                                  @@ RecorderSetRequests(pc)  @@ recorder
UnchangeRecorder   == UNCHANGED recorder

CheckParameterHelper(n, p, Comp(_,_)) == IF p \in DOMAIN Parameters 
                                         THEN Comp(n, Parameters[p])
                                         ELSE TRUE
CheckParameterLimit(n, p) == CheckParameterHelper(n, p, LAMBDA i, j: i < j)
\* For compress state space, we limit that external failure cannot appear too frequently,
\* or continuously.
CheckExternalFailure == /\ recorder.nConsequentFailure + 1 < Cardinality(Server)
                        /\ recorder.nCrash + recorder.nPartition <= (Cardinality(Server) - 1) * (recorder.maxEpoch + 1)
CheckExternalEventExecute(pc) == 
            LET consider == IF pc[1] \in {"NodeCrash", "NodeStart", "PartitionStart", "PartitionRecover"} THEN TRUE ELSE FALSE 
            IN \/ ~consider
               \/ /\ consider 
                  /\ \/ /\ pc[1] \in {"NodeCrash", "NodeStart"}
                        /\ pc \notin recorder.noExecute
                     \/ /\ pc[1] \in {"PartitionStart", "PartitionRecover"}
                        /\ LET exist == \E event \in recorder.noExecute: event[1] = pc[1] 
                           IN \/ ~exist 
                              \/ /\ exist 
                                 /\ LET events == {event \in recorder.noExecute: event[1] = pc[1]} 
                                    IN \A event \in events: \/ pc[2] \notin event[2]
                                                            \/ pc[3] \notin event[2]
                                 
CheckTimeout        == CheckParameterLimit(recorder.nTimeout,     "MaxTimeoutFailures")
CheckTransactionNum == CheckParameterLimit(recorder.nTransaction, "MaxTransactionNum")
CheckEpoch          == CheckParameterLimit(recorder.maxEpoch,     "MaxEpoch")
CheckPartition      == /\ CheckTimeout
                       /\ CheckParameterLimit(recorder.nPartition,   "MaxPartitions")
                       /\ CheckExternalFailure
CheckCrash(i)       == /\ \/ IsLooking(i)
                          \/ /\ ~IsLooking(i)
                             /\ CheckTimeout
                       /\ CheckParameterLimit(recorder.nCrash,    "MaxCrashes")
                       /\ CheckExternalFailure

\* CheckStateConstraints == CheckTimeout /\ CheckTransactionNum /\ CheckEpoch /\ CheckPartition

------
\* invariants after action.

\* There is most one established leader for a certain epoch.
Leadership1 == \A i, j \in Server:
                   /\ state'[i] = LEADING /\ zabState'[i] \in {SYNCHRONIZATION, BROADCAST}
                   /\ state'[j] = LEADING /\ zabState'[j] \in {SYNCHRONIZATION, BROADCAST}
                   /\ acceptedEpoch'[i] = acceptedEpoch'[j]
                  => i = j
Leadership2 == \A epoch \in 1..MAXEPOCH: Cardinality(epochLeader'[epoch]) <= 1
\* PrefixConsistency: Entries that have been committed 
\* in history in any process is the same.
PrefixConsistency == \A i, j \in Server:
                        LET minCommit == Minimum({lastCommitted'[i].index, lastCommitted'[j].index})
                        IN \/ minCommit = 0
                           \/ /\ minCommit > 0
                              /\ \A index \in 1..minCommit:
                                   TxnEqual(history'[i][index], history'[j][index])
\* Integrity: If some follower delivers one transaction, then some primary has broadcast it.
Integrity == \A i \in Server:
                /\ state'[i] = LEADING 
                /\ lastCommitted'[i].index > 0
                => \A idx \in 1..lastCommitted'[i].index:
                     \E proposal \in proposalMsgsLog':
                       LET txn_proposal == [ zxid  |-> proposal.zxid,
                                             value |-> proposal.data ]
                       IN  TxnEqual(history'[i][idx], txn_proposal)
\* Agreement: If some follower f delivers transaction a and some follower f' delivers transaction b,
\*            then f' delivers a or f delivers b.
Agreement == \A i, j \in Server:
                /\ state'[i] = FOLLOWING /\ lastCommitted'[i].index > 0
                /\ state'[j] = FOLLOWING /\ lastCommitted'[j].index > 0
                =>
                \A idx1 \in 1..lastCommitted'[i].index, idx2 \in 1..lastCommitted'[j].index :
                    \/ \E idx_j \in 1..lastCommitted'[j].index:
                        TxnEqual(history'[j][idx_j], history'[i][idx1])
                    \/ \E idx_i \in 1..lastCommitted'[i].index:
                        TxnEqual(history'[i][idx_i], history'[j][idx2])
\* Total order: If some follower delivers a before b, then any process that delivers b
\*              must also deliver a and deliver a before b.
TotalOrder == \A i, j \in Server: 
                LET committed1 == lastCommitted'[i].index 
                    committed2 == lastCommitted'[j].index  
                IN committed1 >= 2 /\ committed2 >= 2
                    => \A idx_i1 \in 1..(committed1 - 1) : \A idx_i2 \in (idx_i1 + 1)..committed1 :
                    LET logOk == \E idx \in 1..committed2 :
                                     TxnEqual(history'[i][idx_i2], history'[j][idx])
                    IN \/ ~logOk 
                       \/ /\ logOk 
                          /\ \E idx_j2 \in 1..committed2 : 
                                /\ TxnEqual(history'[i][idx_i2], history'[j][idx_j2])
                                /\ \E idx_j1 \in 1..(idx_j2 - 1):
                                       TxnEqual(history'[i][idx_i1], history'[j][idx_j1])
\* Local primary order: If a primary broadcasts a before it broadcasts b, then a follower that
\*                      delivers b must also deliver a before b.
LocalPrimaryOrder == LET p_set(i, e) == {p \in proposalMsgsLog': /\ p.source = i 
                                                                 /\ p.epoch  = e }
                         txn_set(i, e) == { [ zxid  |-> p.zxid, 
                                              value |-> p.data ] : p \in p_set(i, e) }
                     IN \A i \in Server: \A e \in 1..currentEpoch'[i]:
                         \/ Cardinality(txn_set(i, e)) < 2
                         \/ /\ Cardinality(txn_set(i, e)) >= 2
                            /\ \E txn1, txn2 \in txn_set(i, e):
                             \/ TxnEqual(txn1, txn2)
                             \/ /\ ~TxnEqual(txn1, txn2)
                                /\ LET TxnPre  == IF ZxidComparePredicate(txn1.zxid, txn2.zxid) THEN txn2 ELSE txn1
                                       TxnNext == IF ZxidComparePredicate(txn1.zxid, txn2.zxid) THEN txn1 ELSE txn2
                                   IN \A j \in Server: /\ lastCommitted'[j].index >= 2
                                                       /\ \E idx \in 1..lastCommitted'[j].index: 
                                                            TxnEqual(history'[j][idx], TxnNext)
                                        => \E idx2 \in 1..lastCommitted'[j].index: 
                                            /\ TxnEqual(history'[j][idx2], TxnNext)
                                            /\ idx2 > 1
                                            /\ \E idx1 \in 1..(idx2 - 1): 
                                                TxnEqual(history'[j][idx1], TxnPre)
\* Global primary order: A follower f delivers both a with epoch e and b with epoch e', and e < e',
\*                       then f must deliver a before b.
GlobalPrimaryOrder == \A i \in Server: lastCommitted'[i].index >= 2
                         => \A idx1, idx2 \in 1..lastCommitted'[i].index:
                                \/ ~EpochPrecedeInTxn(history'[i][idx1], history'[i][idx2])
                                \/ /\ EpochPrecedeInTxn(history'[i][idx1], history'[i][idx2])
                                   /\ idx1 < idx2
\* Leader's committed log will always become longer and each txn committed is always the same.
RECURSIVE TxnEqualHelper(_,_,_,_)
TxnEqualHelper(leaderLog, standardLog, cur, end) ==
        IF cur > end THEN TRUE
        ELSE IF ~TxnEqual(leaderLog[cur], standardLog[cur]) THEN FALSE 
             ELSE TxnEqualHelper(leaderLog, standardLog, cur+1, end)

RECURSIVE TxnNotEqualHelper(_,_,_,_)
TxnNotEqualHelper(followerLog, standardLog, cur, end) ==
        IF cur > end THEN TRUE
        ELSE IF TxnEqual(followerLog[cur], standardLog[cur]) THEN FALSE
             ELSE TxnNotEqualHelper(followerLog, standardLog, cur+1, end)

CommittedLogMonotonicity ==
                 \/ leaderOracle' \notin Server
                 \/ /\ leaderOracle' \in Server
                    /\ LET leader == leaderOracle'
                           index  == lastCommitted'[leader].index
                           on   == status'[leader] = ONLINE
                           lead == state'[leader] = LEADING 
                           bc   == zabState'[leader] = BROADCAST
                       IN \/ ~on \/ ~lead \/ ~bc 
                          \/ /\ on
                             /\ lead 
                             /\ bc
                              => /\ index >= Len(committedLog)
                                 /\ TxnEqualHelper(history'[leader], committedLog,
                                                   1, Minimum({index, Len(committedLog)}))

ProcessConsistency == \A i, j \in Server: 
                        /\ state'[i] = LEADING   /\ j \in learners'[i]
                        /\ state'[j] = FOLLOWING /\ leaderAddr'[j] = i
                        /\ currentEpoch'[j] = acceptedEpoch'[j] \* namely j has received NEWLEADER
                     => /\ Len(history'[i]) >= Len(history'[j])
                        /\ TxnEqualHelper(history'[i], history'[j],
                                          1, Minimum({Len(history'[i]), Len(history'[j])}))

LeaderLastCommitted(i) == IF zabState'[i] = BROADCAST THEN lastCommitted'[i]
                          ELSE LET commitIndex == Len(initialHistory'[i])
                               IN IF commitIndex = 0 THEN [ index |-> 0,
                                                            zxid  |-> <<0, 0>> ]
                                  ELSE [ index |-> commitIndex,
                                         zxid  |-> history'[i][commitIndex].zxid ]

LeaderLogCompleteness == \/ leaderOracle' \notin Server
                         \/ /\ leaderOracle' \in Server
                            /\ LET leader == leaderOracle'
                                   index  == LeaderLastCommitted(leader).index
                                   on     == status'[leader] = ONLINE
                                   lead   == state'[leader] = LEADING
                               IN \/ ~on \/ ~lead
                                  \/ /\ on
                                     /\ lead
                                      => /\ index >= Len(committedLog)
                                         /\ TxnEqualHelper(history'[leader], committedLog,
                                                           1, Minimum({index, Len(committedLog)}))

CommittedLogDurability == \/ recorder'.pc[1] /= "FollowerProcessSyncMessage"
                          \/ /\ recorder'.pc[1] = "FollowerProcessSyncMessage"
                             /\ LET node == recorder'.pc[2]
                                    oldLen == Len(history[node])
                                    newLen == Len(history'[node])
                                IN
                                \/ oldLen <= newLen
                                \/ /\ oldLen > newLen \* follower's log is truncated
                                   /\ \/ Len(committedLog) <= newLen
                                      \/ /\ Len(committedLog) > newLen
                                         /\ TxnNotEqualHelper(history[node], committedLog,
                                                              newLen + 1,
                                                              Minimum({oldLen, Len(committedLog)}))

(*
ProcessConsistency == \A s \in Server: state'[s] = LOOKING 
                                        => LET lastIndex == Len(history'[s]) 
                                           IN \/ lastIndex = 0
                                              \/ /\ lastIndex > 0 
                                                 /\ lastProcessed'[s].index = lastIndex 
                                                 /\ ZxidEqualPredicate(lastProcessed'[s].zxid, history'[s][lastIndex].zxid)
*)
UpdateAfterAction == /\ aaInv' = [    leadership        |-> Leadership1 /\ Leadership2 ,
                                      prefixConsistency |-> PrefixConsistency ,
                                      integrity         |-> Integrity ,
                                      agreement         |-> Agreement ,
                                      totalOrder        |-> TotalOrder,
                                      primaryOrder      |-> LocalPrimaryOrder /\ GlobalPrimaryOrder,
                                      monotonicRead     |-> CommittedLogMonotonicity,
                                      processConsistency|-> ProcessConsistency,
                                      leaderLogCompleteness  |-> LeaderLogCompleteness,
                                      committedLogDurability |-> CommittedLogDurability  ]
                     /\ committedLog' = LET leader == leaderOracle'
                                        IN IF leader \notin Server THEN committedLog
                                           ELSE LET index  == lastCommitted'[leader].index
                                                    on   == status'[leader] = ONLINE
                                                    lead == state'[leader] = LEADING
                                                    bc   == zabState'[leader] = BROADCAST
                                                IN 
                                                IF /\ on
                                                   /\ lead 
                                                   /\ bc
                                                THEN IF index <= 0 THEN << >> 
                                                     ELSE SubSeq(history'[leader], 1, index)
                                                ELSE committedLog
                     /\ doInit' = IF recorder'["pc"][1] = "SetInitState" THEN TRUE 
                                  ELSE doInit

\* For TLC model checker
AfterActionCheck == \A p \in DOMAIN aaInv: aaInv[p] = TRUE
DuringActionCheck == \A p \in DOMAIN daInv: daInv[p] = TRUE
------
\* Actions about network
PendingFOLLOWERINFO(i, j) == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = FOLLOWERINFO
PendingLEADERINFO(i, j)   == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = LEADERINFO
PendingACKEPOCH(i, j)     == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = ACKEPOCH
PendingNEWLEADER(i, j)    == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = NEWLEADER
PendingACKLD(i, j)        == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = ACKLD
PendingUPTODATE(i, j)     == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = UPTODATE
PendingPROPOSAL(i, j)     == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = PROPOSAL
PendingACK(i, j)          == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = ACK
PendingCOMMIT(i, j)       == /\ rcvBuffer[j][i] /= << >>
                             /\ rcvBuffer[j][i][1].mtype = COMMIT

AddMsg(msg, toAdd, from, to) == IF partition[from][to] = FALSE
                                THEN msg \o toAdd
                                ELSE msg

\* Add a message to rcvBuffer - add a message m to rcvBuffer.
Send(i, j, m) == rcvBuffer' = [rcvBuffer EXCEPT ![i][j] = AddMsg(@, <<m>>, i, j) ]

SendPackets(i, j, ms, needDiscard) ==
        rcvBuffer' = [rcvBuffer EXCEPT ![i][j] = AddMsg(@, ms, i, j),
                                       ![j][i] = IF needDiscard = TRUE THEN Tail(@)
                                                                       ELSE @ ]
\* Remove a message from rcvBuffer - discard head of rcvBuffer.
Discard(i, j) == rcvBuffer' = IF rcvBuffer[i][j] /= << >> THEN [rcvBuffer EXCEPT ![i][j] = Tail(@)]
                                                          ELSE rcvBuffer
\* Leader broadcasts a message(PROPOSAL/COMMIT) to all other servers in forwardingFollowers.
Broadcast(i, j, m, needDiscard) ==
        rcvBuffer' = [rcvBuffer EXCEPT ![j][i] = IF needDiscard = TRUE THEN Tail(@)
                                                             ELSE @ ,
                             ![i] = [v \in Server |-> IF /\ v \in forwarding[i]
                                                         /\ v /= i
                                                      THEN AddMsg(rcvBuffer[i][v], <<m>>, i, v)
                                                      ELSE rcvBuffer[i][v]] ]          
\* Leader broadcasts LEADERINFO to all other servers in connectingFollowers.
BroadcastLEADERINFO(i, j, m, needDiscard) ==
        LET new_connecting_quorum == {c \in connecting'[i]: c.connected = TRUE }
            new_sid_connecting == {c.sid: c \in new_connecting_quorum }
        IN 
        rcvBuffer' = [rcvBuffer EXCEPT ![j][i] = IF needDiscard = TRUE THEN Tail(@)
                                                             ELSE @ ,
                             ![i] = [v \in Server |-> IF /\ v \in new_sid_connecting
                                                         /\ v \in learners[i] 
                                                         /\ v /= i
                                                      THEN AddMsg(rcvBuffer[i][v], <<m>>, i, v)
                                                      ELSE rcvBuffer[i][v] ] ]
\* Leader broadcasts UPTODATE to all other servers in newLeaderProposal.
BroadcastUPTODATE(i, j, m, needDiscard) ==
        LET new_ackldRecv_quorum == {a \in ackldRecv'[i]: a.connected = TRUE }
            new_sid_ackldRecv == {a.sid: a \in new_ackldRecv_quorum}
        IN
        rcvBuffer' = [rcvBuffer EXCEPT ![j][i] = IF needDiscard = TRUE THEN Tail(@)
                                                             ELSE @ ,
                             ![i] = [v \in Server |-> IF /\ v \in new_sid_ackldRecv
                                                         /\ v \in learners[i] 
                                                         /\ v /= i
                                                      THEN AddMsg(rcvBuffer[i][v], <<m>>, i, v)
                                                      ELSE rcvBuffer[i][v] ] ]
\* Combination of Send and Discard - discard head of rcvBuffer[j][i] and add m into rcvBuffer.
Reply(i, j, m) == rcvBuffer' = [rcvBuffer EXCEPT ![j][i] = Tail(@),
                                                 ![i][j] = AddMsg(@, <<m>>, i, j) ]

\* Shuffle input buffer.
Clean(i, j) == rcvBuffer' = [rcvBuffer EXCEPT ![j][i] = << >>, ![i][j] = << >>]     

CleanInputBuffer(S) == rcvBuffer' = [s \in Server |-> 
                                     [v \in Server |-> IF v \in S THEN << >>
                                                       ELSE rcvBuffer[s][v] ] ]                      
-----------------------------------------------------------------------------
\* init
InitServerVars == /\ state         = [s \in Server |-> LOOKING]
                  /\ zabState      = [s \in Server |-> ELECTION]
                  /\ acceptedEpoch = [s \in Server |-> 0]
                  /\ currentEpoch  = [s \in Server |-> 0]
InitLogVars == /\ history = [s \in Server |-> << >>]
               /\ initialHistory = [s \in Server |-> << >>]
               /\ lastCommitted = [s \in Server |-> [ index |-> 0,
                                                      zxid  |-> <<0, 0>> ] ]
               /\ lastProcessed = [s \in Server |-> [ index |-> 0,
                                                      zxid  |-> <<0, 0>>] ]            
InitLeaderVars == /\ learners       = [s \in Server |-> {}]
                  /\ connecting     = [s \in Server |-> {}]
                  /\ electing       = [s \in Server |-> {}]
                  /\ ackldRecv      = [s \in Server |-> {}]
                  /\ forwarding     = [s \in Server |-> {}]
                  /\ tempMaxEpoch   = [s \in Server |-> 0 ]
InitFollowerVars == /\ leaderAddr  = [s \in Server |-> NullPoint]
                    /\ packetsSync = [s \in Server |->
                                        [ notCommitted |-> << >>,
                                          committed    |-> << >> ] ]
InitElectionVars == leaderOracle = NullPoint
InitMsgVars == /\ rcvBuffer = [s \in Server |-> [v \in Server |-> << >>] ]
InitEnvVars == /\ status    = [s \in Server |-> ONLINE ]
               /\ partition = [s \in Server |-> [v \in Server |-> FALSE] ]
InitVerifyVars == /\ proposalMsgsLog  = {}
                  /\ epochLeader      = [i \in 1..MAXEPOCH |-> {} ]
InitInvVars == /\ daInv  = [stateConsistent    |-> TRUE,
                            proposalConsistent |-> TRUE,
                            commitConsistent   |-> TRUE,
                            ackConsistent      |-> TRUE,
                            messageLegal       |-> TRUE ]
               /\ aaInv  = [ leadership        |-> TRUE,
                             prefixConsistency |-> TRUE,
                             integrity         |-> TRUE,
                             agreement         |-> TRUE,
                             totalOrder        |-> TRUE,
                             primaryOrder      |-> TRUE,
                             monotonicRead     |-> TRUE,
                             processConsistency|-> TRUE,
                             leaderLogCompleteness  |-> TRUE,
                             committedLogDurability |-> TRUE ]
InitCommitVars == committedLog = << >>
InitInitVars == doInit = FALSE 
InitRecorder == recorder = [nTimeout       |-> 0,
                            nTransaction   |-> 0,
                            nPartition     |-> 0,
                            maxEpoch       |-> 0,
                            nCrash         |-> 0,
                            pc             |-> <<"Init">>,
                            nClientRequest |-> 0,
                            nConsequentFailure |-> 0,
                            noExecute          |-> {} ]

Init == /\ InitServerVars
        /\ InitLogVars
        /\ InitLeaderVars
        /\ InitFollowerVars
        /\ InitElectionVars
        /\ InitMsgVars
        /\ InitEnvVars
        /\ InitVerifyVars
        /\ InitInvVars
        /\ InitCommitVars
        /\ InitInitVars
        /\ InitRecorder

AfterInitState == doInit = TRUE 

RECURSIVE UpdateConnectingHelper(_)
UpdateConnectingHelper(idSet) == 
        IF idSet = { } THEN { }
        ELSE LET chosen == CHOOSE id \in idSet: TRUE
             IN { [sid       |-> chosen,
                   connected |-> TRUE] } 
                \union UpdateConnectingHelper(idSet \ {chosen})

RECURSIVE UpdateElectingHelper(_,_)
UpdateElectingHelper(idSet, needSync) ==
        IF idSet = { } THEN { }
        ELSE LET chosen == CHOOSE id \in idSet: TRUE 
             IN { [sid          |-> chosen,
                   peerLastZxid |-> IF needSync 
                                    THEN lastProcessed[chosen].zxid
                                    ELSE <<-1, -1>>,
                   inQuorum     |-> TRUE] } 
                \union UpdateElectingHelper(idSet \ {chosen}, needSync)

RECURSIVE UpdateAckldRecvHelper(_)
UpdateAckldRecvHelper(idSet) ==
        IF idSet = { } THEN { }
        ELSE LET chosen == CHOOSE id \in idSet: TRUE
             IN { [sid       |-> chosen,
                   connected |-> TRUE] }
                \union UpdateAckldRecvHelper(idSet \ {chosen})

\* Processing of idTable and order comparing
RECURSIVE InitializeIdTable(_)
InitializeIdTable(Remaining) == IF Remaining = {} THEN {}
                                ELSE LET chosen == CHOOSE i \in Remaining: TRUE
                                         re     == Remaining \ {chosen}
                                     IN {<<chosen, Cardinality(Remaining)>>} \union InitializeIdTable(re)

IdTable == InitializeIdTable(Server) 

IdComparePredicate(s1, s2) == LET item1 == CHOOSE item \in IdTable: item[1] = s1
                                  item2 == CHOOSE item \in IdTable: item[1] = s2
                              IN item1[2] > item2[2]

\* TotalOrderPredicate(s1, s2) = TRUE represents s1 is at least as up-to-date as s2.
TotalOrderPredicate(s1, s2) == \/ currentEpoch[s1] > currentEpoch[s2]
                               \/ /\ currentEpoch[s1] = currentEpoch[s2]
                                  /\ \/ ZxidComparePredicate(lastProcessed[s1].zxid, lastProcessed[s2].zxid)
                                     \/ /\ ZxidEqualPredicate(lastProcessed[s1].zxid, lastProcessed[s2].zxid)
                                        /\ IdComparePredicate(s1, s2)
    
\* All node elecion + discovery, sync + broadcast and commit <1, 1>,
\* leader broadcasts <1, 2>.
SetInitState == 
        /\ LET l == CHOOSE i \in Server: \A s \in (Server\{i}): TotalOrderPredicate(i, s)
               f == Server \ {l}
           IN 
           /\ state' = [s \in Server |-> IF s = l THEN LEADING ELSE FOLLOWING]
           /\ zabState' = [s \in Server |-> BROADCAST]
           /\ acceptedEpoch' = [s \in Server |-> 1]
           /\ currentEpoch' = [s \in Server |-> 1]
           /\ LET txn_1 == [zxid   |-> <<1, 1>>, value |-> 101, \* create session, omit
                            ackSid |-> Server,   epoch |-> 1 ] \* create key, committed
                  txn_2 == [zxid   |-> <<1, 2>>, value |-> 102,
                            ackSid |-> {l},      epoch |-> 1 ] \* set data, not commit
              IN history' = [s \in Server |-> 
                                IF s = l THEN <<txn_1, txn_2>> 
                                ELSE <<txn_1>> ]
           /\ lastCommitted' = [s \in Server |-> [ index |-> 1,
                                                   zxid  |-> <<1, 1>> ]]
           /\ lastProcessed' = [s \in Server |-> [ index |-> 1,
                                                   zxid  |-> <<1, 1>> ]]
           /\ tempMaxEpoch' = [tempMaxEpoch EXCEPT ![l] = 1]
           /\ learners' = [learners EXCEPT ![l] = Server]  
           /\ connecting' = [connecting EXCEPT ![l] = { [ sid       |-> l,
                                                          connected |-> TRUE ] } 
                                                   \union UpdateConnectingHelper(f) ]
           /\ electing' = [electing EXCEPT ![l] = { [ sid          |-> l,
                                                      peerLastZxid |-> <<-1,-1>>,
                                                      inQuorum     |-> TRUE ] }
                                                   \union UpdateElectingHelper(f, FALSE) ]
           /\ ackldRecv' = [ackldRecv EXCEPT ![l] = {[ sid       |-> l,
                                                       connected |-> TRUE ]}
                                                   \union UpdateConnectingHelper(f) ]
           /\ forwarding' = [forwarding EXCEPT ![l] = f]
           /\ leaderAddr' = [s \in Server |-> IF s \in f THEN l 
                                                         ELSE leaderAddr[s] ]
           /\ leaderOracle' = l 
           /\ LET pp2 == [mtype |-> PROPOSAL, mzxid |-> <<1, 2>>, mdata |-> 102]
              IN rcvBuffer' = [rcvBuffer EXCEPT ![l] = [v \in Server |-> IF v \in f
                                                                         THEN <<pp2>>
                                                                         ELSE << >>]]
           /\ epochLeader' = [epochLeader EXCEPT ![1] = {l}]
           /\ LET p1 == [source |-> l, epoch |-> 1, zxid |-> <<1, 1>>, data |-> 101]
                  p2 == [source |-> l, epoch |-> 1, zxid |-> <<1, 2>>, data |-> 102]
              IN proposalMsgsLog' = {p1, p2}
           /\ UNCHANGED <<initialHistory, packetsSync, envVars, daInv>>
           /\ UpdateRecorder(<<"SetInitState", l, f>>)
        /\ UpdateAfterAction

-----------------------------------------------------------------------------
InitLastProcessed(i) == IF Len(history[i]) = 0 THEN [ index |-> 0, 
                                                      zxid  |-> <<0, 0>> ]
                        ELSE
                        LET lastIndex == Len(history[i])
                            entry     == history[i][lastIndex]
                        IN [ index |-> lastIndex,
                             zxid  |-> entry.zxid ]

RECURSIVE InitAcksidInTxns(_,_)
InitAcksidInTxns(txns, src) == IF Len(txns) = 0 THEN << >>
                               ELSE LET newTxn == [ zxid   |-> txns[1].zxid,
                                                    value  |-> txns[1].value,
                                                    ackSid |-> {src},
                                                    epoch  |-> txns[1].epoch ]
                                    IN <<newTxn>> \o InitAcksidInTxns( Tail(txns), src)

InitHistory(i) == LET newState == state'[i] IN 
                    IF newState = LEADING THEN InitAcksidInTxns(history[i], i)
                    ELSE history[i]

SwitchToLeading(i) == 
        /\ state'          = [state    EXCEPT ![i] = LEADING ]
        /\ zabState'       = [zabState EXCEPT ![i] = DISCOVERY]
        /\ history'        = [history  EXCEPT ![i] = InitHistory(i) ]
        /\ lastProcessed'  = [lastProcessed  EXCEPT ![i] = InitLastProcessed(i)]
        /\ initialHistory' = [initialHistory EXCEPT ![i] = history'[i]]
        /\ learners'       = [learners   EXCEPT ![i] = {i}]
        /\ connecting'     = [connecting EXCEPT ![i] = { [ sid       |-> i,
                                                           connected |-> TRUE ] }]
        /\ tempMaxEpoch'   = [tempMaxEpoch   EXCEPT ![i] = acceptedEpoch[i] + 1]
        /\ electing'       = [electing   EXCEPT ![i] = { [ sid          |-> i,
                                                           peerLastZxid |-> <<-1,-1>>,
                                                           inQuorum     |-> TRUE ] }]
        /\ ackldRecv'      = [ackldRecv  EXCEPT ![i] = { [ sid       |-> i,
                                                           connected |-> TRUE ] }]
        /\ forwarding'     = [forwarding EXCEPT ![i] = {}]        

SwitchToFollowing(i) ==
        /\ state'    = [state    EXCEPT ![i] = FOLLOWING]
        /\ zabState' = [zabState EXCEPT ![i] = DISCOVERY]
        /\ history'        = [history  EXCEPT ![i] = InitHistory(i) ]
        /\ lastProcessed'  = [lastProcessed  EXCEPT ![i] = InitLastProcessed(i)]
        /\ initialHistory' = [initialHistory EXCEPT ![i] = history'[i]]
        /\ packetsSync'    = [packetsSync    EXCEPT ![i].notCommitted = << >>, 
                                                    ![i].committed = << >> ]

Shutdown(S, crashSet) ==
        /\ state'         = [s \in Server |-> IF s \in S THEN LOOKING ELSE state[s] ]
        /\ lastProcessed' = [s \in Server |-> IF s \in S THEN InitLastProcessed(s)
                                                         ELSE lastProcessed[s] ]
        /\ zabState'      = [s \in Server |-> IF s \in S THEN ELECTION ELSE zabState[s] ]
        /\ leaderAddr'    = [s \in Server |-> IF s \in S THEN NullPoint ELSE leaderAddr[s] ]
        /\ CleanInputBuffer(S)
        /\ packetsSync' = [s \in Server |-> IF s \in S THEN [ notCommitted |-> << >>,
                                                              committed |-> << >> ]
                                                       ELSE packetsSync[s] ]

FollowerShutdown(i, isCrash) ==
        /\ state'      = [state      EXCEPT ![i] = LOOKING]
        /\ zabState'   = [zabState   EXCEPT ![i] = ELECTION]
        /\ leaderAddr' = [leaderAddr EXCEPT ![i] = NullPoint]
        \* in version 3.7+, lastProcessed will be modified when turning to LOOKING
        /\ lastProcessed' = [lastProcessed EXCEPT ![i] = InitLastProcessed(i)]
        /\ packetsSync' = [packetsSync EXCEPT ![i].notCommitted = << >>, 
                                              ![i].committed = << >>]

LeaderShutdown(i, crashSet) ==
        /\ LET cluster == {i} \union learners[i]
           IN Shutdown(cluster, crashSet)
        /\ learners'   = [learners   EXCEPT ![i] = {}]
        /\ forwarding' = [forwarding EXCEPT ![i] = {}]

RemoveElecting(set, sid) ==
        LET sid_electing == {s.sid: s \in set }
        IN IF sid \notin sid_electing THEN set
           ELSE LET info == CHOOSE s \in set: s.sid = sid
                    new_info == [ sid          |-> sid,
                                  peerLastZxid |-> <<-1, -1>>,
                                  inQuorum     |-> info.inQuorum ]
                IN (set \ {info}) \union {new_info}
RemoveConnectingOrAckldRecv(set, sid) ==
        LET sid_set == {s.sid: s \in set}
        IN IF sid \notin sid_set THEN set
           ELSE LET info == CHOOSE s \in set: s.sid = sid
                    new_info == [ sid       |-> sid,
                                  connected |-> FALSE ]
                IN (set \ {info}) \union {new_info}

\* See removeLearnerHandler for details.
RemoveLearner(i, j) ==
        /\ learners'   = [learners   EXCEPT ![i] = @ \ {j}] 
        /\ forwarding' = [forwarding EXCEPT ![i] = IF j \in forwarding[i] 
                                                   THEN @ \ {j} ELSE @ ]
        /\ electing'   = [electing   EXCEPT ![i] = RemoveElecting(@, j) ]
        /\ connecting' = [connecting EXCEPT ![i] = RemoveConnectingOrAckldRecv(@, j) ]
        /\ ackldRecv'  = [ackldRecv  EXCEPT ![i] = RemoveConnectingOrAckldRecv(@, j) ]

(*
\* We compress state of election, to two parts: elect leader and look for leader.
ElectLeader(i) ==
        /\ IsLooking(i)
        /\ leaderOracle /= i 
        /\ \E q \in Quorums: /\ i \in q 
                             /\ \A s \in q\{i}: TotalOrderPredicate(i, s)
        /\ leaderOracle' = i 
        /\ SwitchToLeading(i)
        /\ UNCHANGED <<acceptedEpoch, currentEpoch, lastCommitted, followerVars,
                netVars, verifyVars, daInv>>
        /\ UpdateRecorder(<<"ElectLeader", i>>)
FollowLeader(i) ==
        /\ IsON(i)
        /\ IsLooking(i)
        /\ leaderOracle /= NullPoint
        /\ \/ /\ leaderOracle = i 
              /\ SwitchToLeading(i)
              /\ UNCHANGED packetsSync
           \/ /\ leaderOracle /= i
              /\ SwitchToFollowing(i)
              /\ UNCHANGED leaderVars
        /\ UNCHANGED <<acceptedEpoch, currentEpoch, lastCommitted, leaderAddr, electionVars,
                netVars, envVars, verifyVars, daInv>> 
        /\ UpdateRecorder(<<"FollowLeader", i>>)
*)

\* Assuming there only exists two conditions that may trigger partition:
\* 1. leader and learner that follows it. 2. looking and looking
PartitionStart(i, j) == 
        /\ CheckExternalEventExecute(<<"PartitionStart", i, j>>)
        /\ CheckPartition
        /\ i /= j
        /\ IsON(i) 
        /\ IsON(j)
        /\ \lnot HasPartitioned(i, j)
        /\ \/ /\ IsLeader(i)   /\ IsMyLearner(i, j)
              /\ IsFollower(j) /\ IsMyLeader(j, i)
              /\ LET newCluster == learners[i] \ {j}
                 IN \/ /\ IsQuorum(newCluster)    \* just remove this learner
                       /\ RemoveLearner(i, j) 
                       /\ FollowerShutdown(j, FALSE)
                       /\ Clean(i, j)
                    \/ /\ ~IsQuorum(newCluster)   \* leader switches to looking
                       /\ LeaderShutdown(i, {})
                       /\ UNCHANGED <<electing, connecting, ackldRecv>>
           \/ /\ IsLooking(i) 
              /\ IsLooking(j)
              /\ IdComparePredicate(i, j) \* to compress state space
              /\ UNCHANGED <<state, zabState, lastProcessed, connecting, noDisVars,
                             packetsSync, leaderAddr, netVars>>
        /\ partition' = [partition EXCEPT ![i][j] = TRUE, ![j][i] = TRUE ]
        /\ UNCHANGED <<acceptedEpoch, currentEpoch, history, initialHistory, lastCommitted,
                       tempMaxEpoch, electionVars, status, verifyVars, daInv>>
        /\ UpdateRecorder(<<"PartitionStart", i, j>>)
        /\ UpdateAfterAction 

PartitionRecover(i, j) == 
        /\ CheckExternalEventExecute(<<"PartitionRecover", i, j>>)
        /\ IsON(i)
        /\ IsON(j)
        /\ IdComparePredicate(i, j) \* to compress state space
        /\ HasPartitioned(i, j)
        /\ partition' = [partition EXCEPT ![i][j] = FALSE, ![j][i] = FALSE ]
        /\ UNCHANGED <<serverVars, logVars, leaderVars, followerVars, electionVars, netVars,
                status, verifyVars, daInv>>
        /\ UpdateRecorder(<<"PartitionRecover", i, j>>)
        /\ UpdateAfterAction 

NodeCrash(i) ==
        /\ CheckExternalEventExecute(<<"NodeCrash", i>>)
        /\ CheckCrash(i)
        /\ IsON(i)
        /\ status' = [status EXCEPT ![i] = OFFLINE ]
        /\ \/ /\ IsLooking(i)
              /\ UNCHANGED <<state, zabState, lastProcessed, connecting, noDisVars,
                             packetsSync, leaderAddr, netVars>>
           \/ /\ IsFollower(i)
              /\ LET connectedWithLeader == HasLeader(i)
                 IN \/ /\ connectedWithLeader
                       /\ LET leader == leaderAddr[i]
                              newCluster == learners[leader] \ {i}
                          IN 
                          \/ /\ IsQuorum(newCluster)
                             /\ RemoveLearner(leader, i) 
                             /\ FollowerShutdown(i, TRUE)
                             /\ Clean(leader, i)
                          \/ /\ ~IsQuorum(newCluster)
                             /\ LeaderShutdown(leader, {i})
                             /\ UNCHANGED <<electing, connecting, ackldRecv>>
                    \/ /\ ~connectedWithLeader \* In current spec this condition should not happen 
                       /\ CleanInputBuffer({i})
                       /\ UNCHANGED <<state, zabState, lastProcessed, connecting, 
                                      packetsSync, noDisVars, leaderAddr>>
           \/ /\ IsLeader(i)
              /\ LeaderShutdown(i, {i})
              /\ UNCHANGED <<connecting, electing, ackldRecv>>
        /\ UNCHANGED <<acceptedEpoch, currentEpoch, history, initialHistory, tempMaxEpoch,
                       electionVars, partition, verifyVars, daInv, lastCommitted>>
        /\ UpdateRecorder(<<"NodeCrash", i>>)
        /\ UpdateAfterAction 

NodeStart(i) == 
        /\ CheckExternalEventExecute(<<"NodeStart", i>>)
        /\ IsOFF(i)
        /\ status'     = [status     EXCEPT ![i] = ONLINE ]
        /\ state'      = [state      EXCEPT ![i] = LOOKING ]
        /\ zabState'   = [zabState   EXCEPT ![i] = ELECTION ]
        /\ leaderAddr' = [leaderAddr EXCEPT ![i] = NullPoint]
        /\ lastProcessed' = [lastProcessed  EXCEPT ![i] = InitLastProcessed(i)]
        /\ lastCommitted' = [lastCommitted  EXCEPT ![i] = [ index |-> 0,
                                                            zxid  |-> <<0, 0>> ] ]
        /\ UNCHANGED <<acceptedEpoch, currentEpoch, history, initialHistory,
                leaderVars, packetsSync, electionVars, netVars, partition,
                verifyVars, daInv>>                                                   
        /\ UpdateRecorder(<<"NodeStart", i>>)
        /\ UpdateAfterAction 
-----------------------------------------------------------------------------
\* ld: leader, fs: set of followers
UpdateStateToPhaseSync(ld, fs) ==
        /\ state'          = [s \in Server |-> IF s = ld THEN LEADING 
                                               ELSE IF s \in fs THEN FOLLOWING 
                                                                ELSE state[s] ]
        /\ zabState'       = [s \in Server |-> IF s = ld \/ s \in fs THEN SYNCHRONIZATION
                                                                     ELSE zabState[s] ]
        /\ initialHistory' = [s \in Server |-> IF s = ld THEN InitHistory(s)
                                               ELSE IF s \in fs THEN history[s] 
                                                                ELSE initialHistory[s] ]
        /\ LET S == {ld} \union fs
               finalTempMaxEpoch == Maximum({acceptedEpoch[s]: s \in S }) + 1 
           IN 
           acceptedEpoch'  = [s \in Server |-> IF s \in S THEN finalTempMaxEpoch
                                                          ELSE acceptedEpoch[s] ]
        \* initialize leader
        /\ currentEpoch'   = [currentEpoch EXCEPT ![ld] = acceptedEpoch'[ld]]
        /\ history'        = [history      EXCEPT ![ld] = InitHistory(ld) ]
        /\ learners'       = [learners     EXCEPT ![ld] = {ld} \union fs ]
        /\ connecting'     = [connecting   EXCEPT ![ld] = { [ sid       |-> ld,
                                                              connected |-> TRUE ] } 
                                                            \union UpdateConnectingHelper(fs) ]
        /\ electing'       = [electing     EXCEPT ![ld] = { [ sid          |-> ld,
                                                              peerLastZxid |-> <<-1,-1>>,
                                                              inQuorum     |-> TRUE ] }
                                                            \union UpdateElectingHelper(fs, TRUE) ]
        /\ ackldRecv'      = [ackldRecv    EXCEPT ![ld] = { [ sid       |-> ld,
                                                              connected |-> TRUE ] }]
        /\ forwarding'     = [forwarding   EXCEPT ![ld] = {} ]   
        /\ LET newEpoch == acceptedEpoch'[ld]
           IN /\ tempMaxEpoch' = [tempMaxEpoch EXCEPT ![ld] = newEpoch]
\*              /\ currentEpoch' = [currentEpoch EXCEPT ![ld] = newEpoch]
              /\ epochLeader'  = [epochLeader  EXCEPT ![newEpoch] = @ \union {ld}] 
        \* initialize follower
        /\ packetsSync'    = [s \in Server |-> IF s \in fs THEN [notCommitted |-> << >>,
                                                                 committed    |-> << >> ]
                                                           ELSE packetsSync[s] ]
        /\ leaderAddr'     = [s \in Server |-> IF s \in fs THEN ld 
                                                           ELSE leaderAddr[s] ]                

\* Since now we do not care bugs in election and discovery, here we merge all actions in 
\* these two modules into one action.
ElectionAndDiscovery(i, S) ==
        \* /\ CheckEpoch
        /\ i \in S 
        /\ IsQuorum(S)
        /\ \A s \in S: /\ IsON(s)
                       /\ ~HasPartitioned(i, s)
                       /\ IsLooking(s)
        /\ \A s \in (Server\S): \/ IsOFF(s)
                                \/ HasPartitioned(i, s)
                                \/ ~IsLooking(s)
        /\ \A s \in (S\{i}): TotalOrderPredicate(i, s)
        /\ leaderOracle' = i
        /\ UpdateStateToPhaseSync(i, S\{i})
        \* Election and connection finished, then complete discovery
        /\ UNCHANGED <<lastCommitted, lastProcessed,
                       netVars, envVars, proposalMsgsLog, daInv>>
        /\ UpdateRecorder(<<"ElectionAndDiscovery", i, S\{i} >>)
        /\ UpdateAfterAction 

\* waitingForNewEpoch in Leader
WaitingForNewEpoch(i, set) == (i \in set /\ IsQuorum(set)) = FALSE
WaitingForNewEpochTurnToFalse(i, set) == /\ i \in set
                                         /\ IsQuorum(set) 

\* There may exists some follower in connecting but shuts down and
\* connects again. So when leader broadcasts LEADERINFO, the
\* follower will receive LEADERINFO, and receive it again after
\* sending FOLLOWERINFO. So connected makes sure each follower
\* will only receive LEADERINFO at most once before timeout. 
UpdateConnectingOrAckldRecv(oldSet, sid) ==
        LET sid_set == {s.sid: s \in oldSet}
        IN IF sid \in sid_set
           THEN LET old_info == CHOOSE info \in oldSet: info.sid = sid
                    follower_info == [ sid       |-> sid,
                                       connected |-> TRUE ]
                IN (oldSet \ {old_info} ) \union {follower_info}
           ELSE LET follower_info == [ sid       |-> sid,
                                       connected |-> TRUE ]
                IN oldSet \union {follower_info}

(* Leader waits for receiving FOLLOWERINFO from a quorum including itself,
   and chooses a new epoch e' as its own epoch and broadcasts LEADERINFO.
   See getEpochToPropose in Leader for details. *)
(*
LeaderProcessFOLLOWERINFO(i, j) ==
        /\ CheckEpoch  
        /\ IsON(i)
        /\ IsLeader(i)
        /\ PendingFOLLOWERINFO(i, j)
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLearner(i, j)
               lastAcceptedEpoch == msg.mzxid[1]
               sid_connecting == {c.sid: c \in connecting[i]}
           IN 
           /\ infoOk
           /\ \/ \* 1. has not broadcast LEADERINFO 
                 /\ WaitingForNewEpoch(i, sid_connecting)
                 /\ \/ /\ zabState[i] = DISCOVERY
                       /\ UNCHANGED daInv
                    \/ /\ zabState[i] /= DISCOVERY
                       /\ PrintT("Exception: waitingFotNewEpoch true," \o
                          " while zabState not DISCOVERY.")
                       /\ daInv' = [daInv EXCEPT !.stateConsistent = FALSE]
                 /\ tempMaxEpoch' = [tempMaxEpoch EXCEPT ![i] = IF lastAcceptedEpoch >= tempMaxEpoch[i] 
                                                                THEN lastAcceptedEpoch + 1
                                                                ELSE @]
                 /\ connecting'   = [connecting   EXCEPT ![i] = UpdateConnectingOrAckldRecv(@, j) ]
                 /\ LET new_sid_connecting == {c.sid: c \in connecting'[i]}
                    IN
                    \/ /\ WaitingForNewEpochTurnToFalse(i, new_sid_connecting)
                       /\ acceptedEpoch' = [acceptedEpoch EXCEPT ![i] = tempMaxEpoch'[i]]
                       /\ LET newLeaderZxid == <<acceptedEpoch'[i], 0>>
                              m == [ mtype |-> LEADERINFO,
                                     mzxid |-> newLeaderZxid ]
                          IN BroadcastLEADERINFO(i, j, m, TRUE)
                    \/ /\ ~WaitingForNewEpochTurnToFalse(i, new_sid_connecting)
                       /\ Discard(j, i)
                       /\ UNCHANGED acceptedEpoch
              \/  \* 2. has broadcast LEADERINFO 
                 /\ ~WaitingForNewEpoch(i, sid_connecting)
                 /\ Reply(i, j, [ mtype |-> LEADERINFO,
                                  mzxid |-> <<acceptedEpoch[i], 0>> ] )
                 /\ UNCHANGED <<tempMaxEpoch, connecting, acceptedEpoch, daInv>>
        /\ UNCHANGED <<state, zabState, currentEpoch, logVars, noDisVars, followerVars, electionVars,
                    envVars, verifyVars>>
        /\ UpdateRecorder(<<"LeaderProcessFOLLOWERINFO", i, j>>)
        /\ UpdateAfterAction 
*)

(* Follower receives LEADERINFO. If newEpoch >= acceptedEpoch, then follower 
   updates acceptedEpoch and sends ACKEPOCH back, containing currentEpoch and
   lastProcessedZxid. After this, zabState turns to SYNC. 
   See registerWithLeader in Learner for details.*)
(*
FollowerProcessLEADERINFO(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ PendingLEADERINFO(i, j)
        /\ LET msg      == rcvBuffer[j][i][1]
               newEpoch == msg.mzxid[1]
               infoOk   == IsMyLeader(i, j)
               epochOk  == newEpoch >= acceptedEpoch[i]
               stateOk  == zabState[i] = DISCOVERY
           IN /\ infoOk
              /\ \/ \* 1. Normal case
                    /\ epochOk   
                    /\ \/ /\ stateOk
                          /\ \/ /\ newEpoch > acceptedEpoch[i]
                                /\ acceptedEpoch' = [acceptedEpoch EXCEPT ![i] = newEpoch]
                                /\ LET epochBytes == currentEpoch[i]
                                       m == [ mtype  |-> ACKEPOCH,
                                              mzxid  |-> lastProcessed[i].zxid, 
                                              mepoch |-> epochBytes ] 
                                   IN Reply(i, j, m)
                             \/ /\ newEpoch = acceptedEpoch[i]
                                /\ LET m == [ mtype  |-> ACKEPOCH,
                                              mzxid  |-> lastProcessed[i].zxid,
                                              mepoch |-> -1 ]
                                   IN Reply(i, j, m)
                                /\ UNCHANGED acceptedEpoch
                          /\ zabState' = [zabState EXCEPT ![i] = SYNCHRONIZATION]
                          /\ UNCHANGED daInv
                       \/ /\ ~stateOk
                          /\ PrintT("Exception: Follower receives LEADERINFO," \o
                             " whileZabState not DISCOVERY.")
                          /\ daInv' = [daInv EXCEPT !.stateConsistent = FALSE]
                          /\ Discard(j, i)
                          /\ UNCHANGED <<acceptedEpoch, zabState>>
                    /\ UNCHANGED <<state, lastProcessed, connecting, noDisVars, leaderAddr>>
                 \/ \* 2. Abnormal case - go back to election
                    /\ ~epochOk 
                    /\ FollowerShutdown(i)
                    /\ Clean(i, j)
                    /\ RemoveLearner(j, i)
                    /\ UNCHANGED <<acceptedEpoch, daInv>>
        /\ UNCHANGED <<currentEpoch, history, initialHistory, lastCommitted, tempMaxEpoch, packetsSync,
                electionVars, envVars, verifyVars>>
        /\ UpdateRecorder(<<"FollowerProcessLEADERINFO", i, j>>)
        /\ UpdateAfterAction 
*)
----------------------------------------------------------------------------- 
RECURSIVE UpdateAckSidHelper(_,_,_,_)
UpdateAckSidHelper(his, cur, end, target) ==
        IF cur > end THEN his
        ELSE LET curTxn == [ zxid   |-> his[1].zxid,
                             value  |-> his[1].value,
                             ackSid |-> IF target \in his[1].ackSid THEN his[1].ackSid
                                        ELSE his[1].ackSid \union {target},
                             epoch  |-> his[1].epoch ]
             IN <<curTxn>> \o UpdateAckSidHelper(Tail(his), cur + 1, end, target)

\* There originally existed one bug in LeaderProcessACK when 
\* monotonicallyInc = FALSE, and it is we did not add ackSid of 
\* history in SYNC. So we update ackSid in syncFollower.
UpdateAckSid(his, lastSeenIndex, target) ==
        IF Len(his) = 0 \/ lastSeenIndex = 0 THEN his
        ELSE UpdateAckSidHelper(his, 1, Minimum( { Len(his), lastSeenIndex} ), target)

\* return -1: this zxid appears at least twice; Len(his) + 1: does not exist;
\* 1 ~ Len(his): exists and appears just once.
RECURSIVE ZxidToIndexHepler(_,_,_,_)
ZxidToIndexHepler(his, zxid, cur, appeared) == 
        IF cur > Len(his) THEN cur  
        ELSE IF TxnZxidEqual(his[cur], zxid) 
             THEN CASE appeared = TRUE -> -1
                  []   OTHER           -> Minimum( { cur, 
                            ZxidToIndexHepler(his, zxid, cur + 1, TRUE) } ) 
             ELSE ZxidToIndexHepler(his, zxid, cur + 1, appeared)

ZxidToIndex(his, zxid) == IF ZxidEqualPredicate( zxid, <<0, 0>> ) THEN 0
                          ELSE IF Len(his) = 0 THEN 1
                               ELSE LET len == Len(his) IN
                                    IF \E idx \in 1..len: TxnZxidEqual(his[idx], zxid)
                                    THEN ZxidToIndexHepler(his, zxid, 1, FALSE)
                                    ELSE len + 1

\* Find index idx which meets: 
\* history[idx].zxid <= zxid < history[idx + 1].zxid
RECURSIVE IndexOfZxidHelper(_,_,_,_)
IndexOfZxidHelper(his, zxid, cur, end) ==
        IF cur > end THEN end
        ELSE IF ZxidComparePredicate(his[cur].zxid, zxid) THEN cur - 1
             ELSE IndexOfZxidHelper(his, zxid, cur + 1, end)

IndexOfZxid(his, zxid) == IF Len(his) = 0 THEN 0
                          ELSE LET idx == ZxidToIndex(his, zxid)
                                   len == Len(his)
                               IN 
                               IF idx <= len THEN idx
                               ELSE IndexOfZxidHelper(his, zxid, 1, len)

RECURSIVE queuePackets(_,_,_,_,_)
queuePackets(queue, his, cur, committed, end) == 
        IF cur > end THEN queue
        ELSE CASE cur > committed ->
                LET m_proposal == [ mtype |-> PROPOSAL, 
                                    mzxid |-> his[cur].zxid,
                                    mdata |-> his[cur].value ]
                IN queuePackets(Append(queue, m_proposal), his, cur + 1, committed, end)
             []   cur <= committed ->
                LET m_proposal == [ mtype |-> PROPOSAL, 
                                    mzxid |-> his[cur].zxid,
                                    mdata |-> his[cur].value ]
                    m_commit   == [ mtype |-> COMMIT,
                                    mzxid |-> his[cur].zxid ]
                    newQueue   == queue \o <<m_proposal, m_commit>>
                IN queuePackets(newQueue, his, cur + 1, committed, end)

RECURSIVE setPacketsForChecking(_,_,_,_,_,_)
setPacketsForChecking(set, src, ep, his, cur, end) ==
        IF cur > end THEN set
        ELSE LET m_proposal == [ source |-> src,
                                 epoch  |-> ep,
                                 zxid   |-> his[cur].zxid,
                                 data   |-> his[cur].value ]
             IN setPacketsForChecking((set \union {m_proposal}), src, ep, his, cur + 1, end)

(* See queueCommittedProposals in LearnerHandler and startForwarding in Leader
   for details. For proposals in committedLog and toBeApplied, send <PROPOSAL,
   COMMIT>. For proposals in outstandingProposals, send PROPOSAL only. *)
StartForwarding(i, j, lastSeenZxid, lastSeenIndex, mode, needRemoveHead) ==
        /\ LET lastCommittedIndex == IF zabState[i] = BROADCAST 
                                     THEN lastCommitted[i].index
                                     ELSE Len(initialHistory[i])
               lastProposedIndex  == Len(history[i])
               queue_origin == IF lastSeenIndex >= lastProposedIndex 
                               THEN << >>
                               ELSE queuePackets(<< >>, history[i], 
                                    lastSeenIndex + 1, lastCommittedIndex,
                                    lastProposedIndex)
               set_forChecking == IF lastSeenIndex >= lastProposedIndex 
                                  THEN {}
                                  ELSE setPacketsForChecking( { }, i, 
                                        acceptedEpoch[i], history[i],
                                        lastSeenIndex + 1, lastProposedIndex)
               m_trunc == [ mtype |-> TRUNC, mtruncZxid |-> lastSeenZxid ]
               m_diff  == [ mtype |-> DIFF,  mzxid |-> lastSeenZxid ]
               newLeaderZxid == <<acceptedEpoch[i], 0>>
               m_newleader == [ mtype |-> NEWLEADER,
                                mzxid |-> newLeaderZxid ]
               queue_toSend == CASE mode = TRUNC -> (<<m_trunc>> \o queue_origin) \o <<m_newleader>>
                               []   OTHER        -> (<<m_diff>>  \o queue_origin) \o <<m_newleader>>
           IN /\ \/ /\ needRemoveHead
                    /\ SendPackets(i, j, queue_toSend, TRUE)
                 \/ /\ ~needRemoveHead
                    /\ SendPackets(i, j, queue_toSend, FALSE)
              /\ proposalMsgsLog' = proposalMsgsLog \union set_forChecking
        /\ forwarding' = [forwarding EXCEPT ![i] = @ \union {j} ]
        /\ history' = [history EXCEPT ![i] = UpdateAckSid(@, lastSeenIndex, j) ]

(* Leader syncs with follower using DIFF/TRUNC/PROPOSAL/COMMIT...
   See syncFollower in LearnerHandler for details. *)
SyncFollower(i, j, peerLastZxid, needRemoveHead) ==
        LET \* IsPeerNewEpochZxid == peerLastZxid[2] = 0
            lastProcessedZxid == lastProcessed[i].zxid
            maxCommittedLog   == IF zabState[i] = BROADCAST 
                                 THEN lastCommitted[i].zxid
                                 ELSE LET totalLen == Len(initialHistory[i])
                                      IN IF totalLen = 0 THEN << 0, 0>>
                                         ELSE history[i][totalLen].zxid

            \* Hypothesis: 1. minCommittedLog : zxid of head of history, so no SNAP.
            \*             2. maxCommittedLog = lastCommitted, to compress state space.
            \*             3. merge queueCommittedProposals,startForwarding and 
            \*                sending NEWLEADER into StartForwarding.

        IN \/ \* case1. peerLastZxid = lastProcessedZxid
              \*        DIFF + StartForwarding(lastProcessedZxid)
              /\ ZxidEqualPredicate(peerLastZxid, lastProcessedZxid)
              /\ StartForwarding(i, j, peerLastZxid, lastProcessed[i].index, 
                                     DIFF, needRemoveHead)
           \/ /\ ~ZxidEqualPredicate(peerLastZxid, lastProcessedZxid)
              /\ \/ \* case2. peerLastZxid > maxCommittedLog
                    \*        TRUNC + StartForwarding(maxCommittedLog)
                    /\ ZxidComparePredicate(peerLastZxid, maxCommittedLog)
                    /\ LET maxCommittedIndex == IF zabState[i] = BROADCAST 
                                                THEN lastCommitted[i].index
                                                ELSE Len(initialHistory[i])
                       IN StartForwarding(i, j, maxCommittedLog, maxCommittedIndex, 
                                            TRUNC, needRemoveHead)
                 \/ \* case3. minCommittedLog <= peerLastZxid <= maxCommittedLog
                    /\ ~ZxidComparePredicate(peerLastZxid, maxCommittedLog)
                    /\ LET lastSeenIndex == ZxidToIndex(history[i], peerLastZxid)
                           exist == /\ lastSeenIndex >= 0
                                    /\ lastSeenIndex <= Len(history[i])
                           lastIndex == IF exist THEN lastSeenIndex
                                        ELSE IndexOfZxid(history[i], peerLastZxid)
                           \* Maximum zxid that < peerLastZxid
                           lastZxid  == IF exist THEN peerLastZxid
                                        ELSE IF lastIndex = 0 THEN <<0, 0>>
                                             ELSE history[i][lastIndex].zxid
                       IN 
                       \/ \* case 3.1. peerLastZxid exists in history
                          \*           DIFF + StartForwarding
                          /\ exist
                          /\ StartForwarding(i, j, peerLastZxid, lastSeenIndex, 
                                                DIFF, needRemoveHead)
                       \/ \* case 3.2. peerLastZxid does not exist in history
                          \*           TRUNC + StartForwarding
                          /\ ~exist
                          /\ StartForwarding(i, j, lastZxid, lastIndex, 
                                               TRUNC, needRemoveHead)
             \* we will not have case 4 where peerLastZxid < minCommittedLog, because
             \* minCommittedLog default value is 1 in our spec.

\* compare state summary of two servers
IsMoreRecentThan(ss1, ss2) == \/ ss1.currentEpoch > ss2.currentEpoch
                              \/ /\ ss1.currentEpoch = ss2.currentEpoch
                                 /\ ZxidComparePredicate(ss1.lastZxid, ss2.lastZxid)

\* electionFinished in Leader
ElectionFinished(i, set) == /\ i \in set
                            /\ IsQuorum(set)

\* There may exist some follower shuts down and connects again, while
\* it has sent ACKEPOCH or updated currentEpoch last time. This means
\* sid of this follower has existed in elecingFollower but its info 
\* is old. So we need to make sure each sid in electingFollower is 
\* unique and latest(newest).
UpdateElecting(oldSet, sid, peerLastZxid, inQuorum) ==
        LET sid_electing == {s.sid: s \in oldSet }
        IN IF sid \in sid_electing 
           THEN LET old_info == CHOOSE info \in oldSet : info.sid = sid
                    follower_info == 
                             [ sid          |-> sid,
                               peerLastZxid |-> peerLastZxid,
                               inQuorum     |-> (inQuorum \/ old_info.inQuorum) ]
                IN (oldSet \ {old_info} ) \union {follower_info}
           ELSE LET follower_info == 
                             [ sid          |-> sid,
                               peerLastZxid |-> peerLastZxid,
                               inQuorum     |-> inQuorum ]
                IN oldSet \union {follower_info}

LeaderTurnToSynchronization(i) ==
        /\ currentEpoch' = [currentEpoch EXCEPT ![i] = acceptedEpoch[i]]
        /\ zabState'     = [zabState     EXCEPT ![i] = SYNCHRONIZATION]

(* Leader waits for receiving ACKEPOPCH from a quorum, and check whether it has most recent
   state summary from them. After this, leader's zabState turns to SYNCHRONIZATION.
   See waitForEpochAck in Leader for details. *)
(*
LeaderProcessACKEPOCH(i, j) ==
        /\ IsON(i)
        /\ IsLeader(i)
        /\ PendingACKEPOCH(i, j)
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLearner(i, j)           
               leaderStateSummary   == [ currentEpoch |-> currentEpoch[i], 
                                         lastZxid     |-> lastProcessed[i].zxid ]
               followerStateSummary == [ currentEpoch |-> msg.mepoch,  
                                         lastZxid     |-> msg.mzxid ]
               logOk == \* whether follower is no more up-to-date than leader
                        ~IsMoreRecentThan(followerStateSummary, leaderStateSummary)
               electing_quorum == {e \in electing[i]: e.inQuorum = TRUE }
               sid_electing == {s.sid: s \in electing_quorum }
           IN /\ infoOk
              /\ \/ \* electionFinished = true, jump ouf of waitForEpochAck. 
                    \* Different from code, here we still need to record info
                    \* into electing, to help us perform syncFollower afterwards.
                    \* Since electing already meets quorum, it does not break
                    \* consistency between code and spec.
                    /\ ElectionFinished(i, sid_electing)
                    /\ electing' = [electing EXCEPT ![i] = UpdateElecting(@, j, msg.mzxid, FALSE) ]
                    /\ Discard(j, i)
                    /\ UNCHANGED <<state, zabState, currentEpoch, lastProcessed, learners,
                                forwarding, leaderAddr, epochLeader, daInv>>
                 \/ /\ ~ElectionFinished(i, sid_electing)
                    /\ \/ /\ zabState[i] = DISCOVERY
                          /\ UNCHANGED daInv
                       \/ /\ zabState[i] /= DISCOVERY
                          /\ PrintT("Exception: electionFinished false," \o
                             " while zabState not DISCOVERY.")
                          /\ daInv' = [daInv EXCEPT !.stateConsistent = FALSE ]
                    /\ \/ /\ followerStateSummary.currentEpoch = -1
                          /\ electing' = [electing EXCEPT ![i] = UpdateElecting(@, j, 
                                                                msg.mzxid, FALSE) ]
                          /\ Discard(j, i)
                          /\ UNCHANGED <<state, zabState, currentEpoch, lastProcessed, learners,
                                forwarding, leaderAddr, epochLeader>>
                       \/ /\ followerStateSummary.currentEpoch > -1
                          /\ \/ \* normal follower 
                                /\ logOk
                                /\ electing' = [electing EXCEPT ![i] = 
                                            UpdateElecting(@, j, msg.mzxid, TRUE) ]
                                /\ LET new_electing_quorum == {e \in electing'[i]: e.inQuorum = TRUE }
                                       new_sid_electing == {s.sid: s \in new_electing_quorum }
                                   IN 
                                   \/ \* electionFinished = true, jump out of waitForEpochAck,
                                      \* update currentEpoch and zabState.
                                      /\ ElectionFinished(i, new_sid_electing) 
                                      /\ LeaderTurnToSynchronization(i)
                                      /\ LET newLeaderEpoch == acceptedEpoch[i]
                                         IN epochLeader' = [epochLeader EXCEPT ![newLeaderEpoch]
                                                = @ \union {i} ] \* for checking invariants
                                   \/ \* there still exists electionFinished = false.
                                      /\ ~ElectionFinished(i, new_sid_electing)
                                      /\ UNCHANGED <<currentEpoch, zabState, epochLeader>>
                                /\ Discard(j, i)
                                /\ UNCHANGED <<state, lastProcessed, leaderAddr, learners, forwarding>>
                             \/ \* Exists follower more recent than leader
                                /\ ~logOk 
                                /\ LeaderShutdown(i)
                                /\ UNCHANGED <<electing, epochLeader, currentEpoch>>
        /\ UNCHANGED <<acceptedEpoch, history, initialHistory, lastCommitted, disVars, ackldRecv,
                        packetsSync, electionVars, envVars, proposalMsgsLog>>
        /\ UpdateRecorder(<<"LeaderProcessACKEPOCH", i, j>>)
        /\ UpdateAfterAction 
*)

\* Strip syncFollower from LeaderProcessACKEPOCH.
\* Only when electionFinished = true and there exists some
\* learnerHandler has not perform syncFollower, this 
\* action will be called.
LeaderSyncFollower(i, j) == 
        /\ IsON(i)
        /\ IsLeader(i)
        /\ LET electing_quorum == {e \in electing[i]: e.inQuorum = TRUE }
               electionFinished == ElectionFinished(i, {s.sid: s \in electing_quorum } )
               toSync == {s \in electing[i] : /\ ~ZxidEqualPredicate( s.peerLastZxid, <<-1, -1>>)
                                              /\ s.sid \in learners[i] }
               canSync == toSync /= {}
           IN
           /\ electionFinished
           /\ canSync
           /\ \E s \in toSync: s.sid = j
           /\ LET chosen == CHOOSE s \in toSync: s.sid = j
                  newChosen == [ sid          |-> chosen.sid,
                                 peerLastZxid |-> <<-1, -1>>, \* <<-1,-1>> means has handled.
                                 inQuorum     |-> chosen.inQuorum ] 
              IN /\ SyncFollower(i, chosen.sid, chosen.peerLastZxid, FALSE)
                 /\ electing' = [electing EXCEPT ![i] = (@ \ {chosen}) \union {newChosen} ]
        /\ currentEpoch' = [currentEpoch EXCEPT ![i] = acceptedEpoch[i]]
        /\ UNCHANGED <<state, zabState, acceptedEpoch, initialHistory, lastCommitted, lastProcessed, disVars,
                learners, ackldRecv, followerVars, electionVars, envVars, epochLeader, daInv>>
        /\ UpdateRecorder(<<"LeaderSyncFollower", i, j>>)
        /\ UpdateAfterAction 

TruncateLog(his, index) == IF index <= 0 THEN << >>
                           ELSE SubSeq(his, 1, index)

(* Follower receives DIFF/TRUNC, and then may receives PROPOSAL,COMMIT,NEWLEADER,
   and UPTODATE. See syncWithLeader in Learner for details. *)
FollowerProcessSyncMessage(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ rcvBuffer[j][i] /= << >>
        /\ \/ rcvBuffer[j][i][1].mtype = DIFF 
           \/ rcvBuffer[j][i][1].mtype = TRUNC
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLeader(i, j)
               stateOk == zabState[i] = SYNCHRONIZATION
           IN /\ infoOk
              /\ \/ \* Follower should receive packets in SYNC.
                    /\ ~stateOk
                    /\ PrintT("Exception: Follower receives DIFF/TRUNC," \o
                             " whileZabState not SYNCHRONIZATION.")
                    /\ daInv' = [daInv EXCEPT !.stateConsistent = FALSE]
                    /\ UNCHANGED <<logVars>>
                 \/ /\ stateOk
                    /\ \/ /\ msg.mtype = DIFF                    
                          /\ UNCHANGED <<logVars, daInv>>
                       \/ /\ msg.mtype = TRUNC
                          /\ LET truncZxid == msg.mtruncZxid
                                 truncIndex == ZxidToIndex(history[i], truncZxid)
                             IN
                             \/ /\ truncIndex > Len(history[i])
                                /\ PrintT("Exception: TRUNC error.")
                                /\ daInv' = [daInv EXCEPT !.proposalConsistent = FALSE ]
                                /\ UNCHANGED <<logVars>>
                             \/ /\ truncIndex <= Len(history[i])
                                /\ history' = [history EXCEPT 
                                                    ![i] = TruncateLog(history[i], truncIndex)]
                                /\ initialHistory' = [initialHistory EXCEPT ![i] = history'[i]]
                                /\ lastProcessed' = [lastProcessed EXCEPT 
                                                    ![i] = [ index |-> truncIndex,
                                                             zxid  |-> truncZxid] ]
                                /\ lastCommitted' = [lastCommitted EXCEPT 
                                                    ![i] = [ index |-> truncIndex,
                                                             zxid  |-> truncZxid] ]
                                /\ UNCHANGED <<daInv>>
        /\ Discard(j, i)
        /\ UNCHANGED <<serverVars, leaderVars, followerVars, electionVars, envVars, verifyVars>>
        /\ UpdateRecorder(<<"FollowerProcessSyncMessage", i, j>>)
        /\ UpdateAfterAction 

\* See lastProposed in Leader for details.
LastProposed(i) == IF Len(history[i]) = 0 THEN [ index |-> 0, 
                                                 zxid  |-> <<0, 0>> ]
                   ELSE
                   LET lastIndex == Len(history[i])
                       entry     == history[i][lastIndex]
                   IN [ index |-> lastIndex,
                        zxid  |-> entry.zxid ]

\* See lastQueued in Learner for details.
LastQueued(i) == IF ~IsFollower(i) \/ zabState[i] /= SYNCHRONIZATION 
                 THEN LastProposed(i)
                 ELSE \* condition: IsFollower(i) /\ zabState = SYNCHRONIZATION
                      LET packetsInSync == packetsSync[i].notCommitted
                          lenSync  == Len(packetsInSync)
                          totalLen == Len(history[i]) + lenSync
                      IN IF lenSync = 0 THEN LastProposed(i)
                         ELSE [ index |-> totalLen,
                                zxid  |-> packetsInSync[lenSync].zxid ]

IsNextZxid(curZxid, nextZxid) ==
            \/ \* first PROPOSAL in this epoch
               /\ nextZxid[2] = 1
               /\ curZxid[1] < nextZxid[1]
            \/ \* not first PROPOSAL in this epoch
               /\ nextZxid[2] > 1
               /\ curZxid[1] = nextZxid[1]
               /\ curZxid[2] + 1 = nextZxid[2]

FollowerProcessPROPOSALInSync(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ PendingPROPOSAL(i, j)
        /\ zabState[i] = SYNCHRONIZATION
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLeader(i, j)
               isNext == IsNextZxid(LastQueued(i).zxid, msg.mzxid)
               newTxn == [ zxid   |-> msg.mzxid,
                           value  |-> msg.mdata,
                           ackSid |-> {},    \* follower do not consider ackSid
                           epoch  |-> acceptedEpoch[i] ] \* epoch of this round
           IN /\ infoOk
              /\ \/ /\ isNext
                    /\ packetsSync' = [packetsSync EXCEPT ![i].notCommitted 
                                = Append(packetsSync[i].notCommitted, newTxn) ]
                    /\ UNCHANGED daInv
                 \/ /\ ~isNext
                    /\ PrintT("Warn: Follower receives PROPOSAL," \o
                        " while zxid != lastQueued + 1.")
                    /\ daInv' = [daInv EXCEPT !.proposalConsistent = FALSE ]
                    /\ UNCHANGED packetsSync
        \* logRequest -> SyncRequestProcessor -> SendAckRequestProcessor -> reply ack
        \* So here we do not need to send ack to leader.
        /\ Discard(j, i)
        /\ UNCHANGED <<serverVars, logVars, leaderVars, leaderAddr, electionVars, 
                envVars, verifyVars>>
        /\ LET msg == rcvBuffer[j][i][1]
           IN UpdateRecorder(<<"FollowerProcessPROPOSALInSync", i, j, msg.mzxid>>)
        /\ UpdateAfterAction 

RECURSIVE IndexOfFirstTxnWithEpoch(_,_,_,_)
IndexOfFirstTxnWithEpoch(his, epoch, cur, end) == 
            IF cur > end THEN cur 
            ELSE IF his[cur].epoch = epoch THEN cur
                 ELSE IndexOfFirstTxnWithEpoch(his, epoch, cur + 1, end)

LastCommitted(i) == IF zabState[i] = BROADCAST THEN lastCommitted[i]
                    ELSE CASE IsLeader(i)   -> 
                            LET lastInitialIndex == Len(initialHistory[i])
                            IN IF lastInitialIndex = 0 THEN [ index |-> 0,
                                                              zxid  |-> <<0, 0>> ]
                               ELSE [ index |-> lastInitialIndex,
                                      zxid  |-> history[i][lastInitialIndex].zxid ]
                         []   IsFollower(i) ->
                            LET completeHis == history[i] \o packetsSync[i].notCommitted
                                packetsCommitted == packetsSync[i].committed
                                lenCommitted == Len(packetsCommitted)
                            IN IF lenCommitted = 0 \* return last one in initial history
                               THEN LET lastInitialIndex == Len(initialHistory[i])
                                    IN IF lastInitialIndex = 0 
                                       THEN [ index |-> 0,
                                              zxid  |-> <<0, 0>> ]
                                       ELSE [ index |-> lastInitialIndex ,
                                              zxid  |-> completeHis[lastInitialIndex].zxid ]
                               ELSE                \* return tail of packetsCommitted
                                    LET committedIndex == ZxidToIndex(completeHis, 
                                                     packetsCommitted[lenCommitted] )
                                    IN [ index |-> committedIndex, 
                                         zxid  |-> packetsCommitted[lenCommitted] ]
                         []   OTHER -> lastCommitted[i]

TxnWithIndex(i, idx) == IF ~IsFollower(i) \/ zabState[i] /= SYNCHRONIZATION 
                        THEN history[i][idx]
                        ELSE LET completeHis == history[i] \o packetsSync[i].notCommitted
                             IN completeHis[idx]

(* To simplify specification, we assume snapshotNeeded = false and 
   writeToTxnLog = true. So here we just call packetsCommitted.add. *)
FollowerProcessCOMMITInSync(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ PendingCOMMIT(i, j)
        /\ zabState[i] = SYNCHRONIZATION
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLeader(i, j)
               committedIndex == LastCommitted(i).index + 1
               exist == /\ committedIndex <= LastQueued(i).index
                        /\ IsNextZxid(LastCommitted(i).zxid, msg.mzxid)
               match == ZxidEqualPredicate(msg.mzxid, TxnWithIndex(i, committedIndex).zxid )
           IN /\ infoOk 
              /\ \/ /\ exist
                    /\ \/ /\ match
                          /\ packetsSync' = [ packetsSync EXCEPT ![i].committed
                                 = Append(packetsSync[i].committed, msg.mzxid) ]
                          /\ UNCHANGED daInv
                       \/ /\ ~match
                          /\ PrintT("Warn: Follower receives COMMIT," \o
                               " but zxid not the next committed zxid in COMMIT.")
                          /\ daInv' = [daInv EXCEPT !.commitConsistent = FALSE ]
                          /\ UNCHANGED packetsSync
                 \/ /\ ~exist
                    /\ PrintT("Warn: Follower receives COMMIT," \o
                         " but no packets with its zxid exists.")
                    /\ daInv' = [daInv EXCEPT !.commitConsistent = FALSE ]
                    /\ UNCHANGED packetsSync
        /\ Discard(j, i)
        /\ UNCHANGED <<serverVars, logVars, leaderVars, leaderAddr, electionVars, 
                envVars, verifyVars>>
        /\ LET msg == rcvBuffer[j][i][1]
           IN UpdateRecorder(<<"FollowerProcessCOMMITInSync", i, j, msg.mzxid>>)
        /\ UpdateAfterAction 

RECURSIVE ACKInBatches(_,_)
ACKInBatches(queue, packets) ==
        IF packets = << >> THEN queue
        ELSE LET head == packets[1]
                 newPackets == Tail(packets)
                 m_ack == [ mtype |-> ACK,
                            mzxid |-> head.zxid ]
             IN ACKInBatches( Append(queue, m_ack), newPackets )

(* Update currentEpoch, and logRequest every packets in
   packetsNotCommitted and clear it. As syncProcessor will 
   be called in logRequest, we have to reply acks here. *)
FollowerProcessNEWLEADER(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ PendingNEWLEADER(i, j)
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLeader(i, j)
               packetsInSync == packetsSync[i].notCommitted
               m_ackld == [ mtype |-> ACKLD,
                            mzxid |-> msg.mzxid ]
               ms_ack  == ACKInBatches( << >>, packetsInSync )
               queue_toSend == <<m_ackld>> \o ms_ack \* send ACK-NEWLEADER first.
           IN /\ infoOk
              /\ currentEpoch' = [currentEpoch EXCEPT ![i] = acceptedEpoch[i] ]
              /\ history'      = [history      EXCEPT ![i] = @ \o packetsInSync ]
              /\ packetsSync'  = [packetsSync  EXCEPT ![i].notCommitted = << >> ]
              /\ SendPackets(i, j, queue_toSend, TRUE)
        /\ UNCHANGED <<state, zabState, acceptedEpoch, initialHistory, lastCommitted, lastProcessed,
                leaderVars, leaderAddr, electionVars, envVars,verifyVars, daInv>>
        /\ UpdateRecorder(<<"FollowerProcessNEWLEADER", i, j>>)
        /\ UpdateAfterAction 

\* quorumFormed in Leader
QuorumFormed(i, set) == i \in set /\ IsQuorum(set)

UpdateElectionVote(i, epoch) == TRUE
    \* UpdateProposal(i, currentVote[i].proposedLeader, currentVote[i].proposedZxid, epoch)

\* See startZkServer in Leader for details.
\* It is ok that set lastCommitted to lastProposed, since now leader just converts to
\* broadcast so it has not offer service.
StartZkServer(i) ==
        LET latest == LastProposed(i)
        IN /\ lastCommitted' = [lastCommitted EXCEPT ![i] = latest]
           /\ lastProcessed' = [lastProcessed EXCEPT ![i] = latest]
           /\ UpdateElectionVote(i, acceptedEpoch[i])

LeaderTurnToBroadcast(i) ==
        /\ StartZkServer(i)
        /\ zabState' = [zabState EXCEPT ![i] = BROADCAST]

(* Leader waits for receiving quorum of ACK whose lower bits of zxid is 0, and
   broadcasts UPTODATE. See waitForNewLeaderAck for details.  *)
LeaderProcessACKLD(i, j) ==
        /\ IsON(i)
        /\ IsLeader(i)
        /\ PendingACKLD(i, j)
        /\ LET msg    == rcvBuffer[j][i][1]
               infoOk == IsMyLearner(i, j)
               match  == ZxidEqualPredicate(msg.mzxid, <<acceptedEpoch[i], 0>>)
               currentZxid == <<acceptedEpoch[i], 0>>
               m_uptodate == [ mtype |-> UPTODATE,
                               mzxid |-> currentZxid ] \* not important
               sid_ackldRecv == {a.sid: a \in ackldRecv[i]}
           IN /\ infoOk
              /\ \/ \* just reply UPTODATE.
                    /\ QuorumFormed(i, sid_ackldRecv)
                    /\ Reply(i, j, m_uptodate)
                    /\ UNCHANGED <<ackldRecv, zabState, lastCommitted, lastProcessed, daInv>>
                 \/ /\ ~QuorumFormed(i, sid_ackldRecv)
                    /\ \/ /\ match
                          /\ ackldRecv' = [ackldRecv EXCEPT ![i] = UpdateConnectingOrAckldRecv(@, j) ]
                          /\ LET new_sid_ackldRecv == {a.sid: a \in ackldRecv'[i]}
                             IN
                             \/ \* jump out of waitForNewLeaderAck, and do startZkServer,
                                \* setZabState, and reply UPTODATE.
                                /\ QuorumFormed(i, new_sid_ackldRecv)
                                /\ LeaderTurnToBroadcast(i)
                                /\ BroadcastUPTODATE(i, j, m_uptodate, TRUE)
                             \/ \* still wait in waitForNewLeaderAck.
                                /\ ~QuorumFormed(i, new_sid_ackldRecv)
                                /\ Discard(j, i)
                                /\ UNCHANGED <<zabState, lastCommitted, lastProcessed>>
                          /\ UNCHANGED daInv
                       \/ /\ ~match
                          /\ PrintT("Exception: NEWLEADER ACK is from a different epoch. ")
                          /\ daInv' = [daInv EXCEPT !.ackConsistent = FALSE ]
                          /\ Discard(j, i)
                          /\ UNCHANGED <<ackldRecv, zabState, lastCommitted, lastProcessed>>
        /\ UNCHANGED <<state, acceptedEpoch, currentEpoch, history, initialHistory, disVars,
                learners, electing, forwarding, followerVars, electionVars, envVars, verifyVars>>
        /\ UpdateRecorder(<<"LeaderProcessACKLD", i, j>>)
        /\ UpdateAfterAction 

TxnsWithPreviousEpoch(i) ==
            LET completeHis == IF ~IsFollower(i) \/ zabState[i] /= SYNCHRONIZATION 
                               THEN history[i] 
                               ELSE history[i] \o packetsSync[i].notCommitted
                end   == Len(completeHis)
                first == IndexOfFirstTxnWithEpoch(completeHis, acceptedEpoch[i], 1, end)
            IN IF first > end THEN completeHis
               ELSE SubSeq(completeHis, 1, first - 1)

TxnsRcvWithCurEpoch(i) ==
            LET completeHis == IF ~IsFollower(i) \/ zabState[i] /= SYNCHRONIZATION 
                               THEN history[i] 
                               ELSE history[i] \o packetsSync[i].notCommitted
                end   == Len(completeHis)
                first == IndexOfFirstTxnWithEpoch(completeHis, acceptedEpoch[i], 1, end)
            IN IF first > end THEN << >>
               ELSE SubSeq(completeHis, first, end) \* completeHis[first : end]

\* Txns received in current epoch but not committed.
\* See pendingTxns in FollowerZooKeeper for details.
PendingTxns(i) == IF ~IsFollower(i) \/ zabState[i] /= SYNCHRONIZATION 
                  THEN SubSeq(history[i], lastCommitted[i].index + 1, Len(history[i]))
                  ELSE LET packetsCommitted == packetsSync[i].committed
                           completeHis == history[i] \o packetsSync[i].notCommitted
                       IN IF Len(packetsCommitted) = 0 
                          THEN SubSeq(completeHis, Len(initialHistory[i]) + 1, Len(completeHis))
                          ELSE SubSeq(completeHis, LastCommitted(i).index + 1, Len(completeHis))

CommittedTxns(i) == IF ~IsFollower(i) \/ zabState[i] /= SYNCHRONIZATION 
                    THEN SubSeq(history[i], 1, lastCommitted[i].index)
                    ELSE LET packetsCommitted == packetsSync[i].committed
                             completeHis == history[i] \o packetsSync[i].notCommitted
                         IN IF Len(packetsCommitted) = 0 THEN initialHistory[i]
                            ELSE SubSeq( completeHis, 1, LastCommitted(i).index )

\* Each zxid of packetsCommitted equals to zxid of 
\* corresponding txn in txns.
RECURSIVE TxnsAndCommittedMatch(_,_)
TxnsAndCommittedMatch(txns, packetsCommitted) ==
        LET len1 == Len(txns)
            len2 == Len(packetsCommitted)
        IN IF len2 = 0 THEN TRUE 
           ELSE IF len1 < len2 THEN FALSE 
                ELSE /\ ZxidEqualPredicate(txns[len1].zxid, packetsCommitted[len2])
                     /\ TxnsAndCommittedMatch( SubSeq(txns, 1, len1 - 1), 
                                               SubSeq(packetsCommitted, 1, len2 - 1) )

FollowerLogRequestInBatches(i, leader, ms_ack, packetsNotCommitted) ==
        /\ history' = [history EXCEPT ![i] = @ \o packetsNotCommitted ]
        /\ SendPackets(i, leader, ms_ack, TRUE)

\* Since commit will call commitProcessor.commit, which will finally 
\* update lastProcessed, we update it here atomically.
FollowerCommitInBatches(i) ==
        LET committedTxns == CommittedTxns(i)
            packetsCommitted == packetsSync[i].committed
            match == TxnsAndCommittedMatch(committedTxns, packetsCommitted)
        IN 
        \/ /\ match 
           /\ lastCommitted' = [lastCommitted EXCEPT ![i] = LastCommitted(i)]
           /\ lastProcessed' = [lastProcessed EXCEPT ![i] = lastCommitted'[i]]
           /\ UNCHANGED daInv
        \/ /\ ~match
           /\ PrintT("Warn: Committing zxid withou see txn. /" \o 
                "Committing zxid != pending txn zxid.")
           /\ daInv' = [daInv EXCEPT !.commitConsistent = FALSE ]
           /\ UNCHANGED <<lastCommitted, lastProcessed>>

(* Follower jump out of outerLoop here, and log the stuff that came in
   between snapshot and uptodate, which means calling logRequest and 
   commit to clear packetsNotCommitted and packetsCommitted. *)
FollowerProcessUPTODATE(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ PendingUPTODATE(i, j)
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLeader(i, j)
               packetsNotCommitted == packetsSync[i].notCommitted
               ms_ack == ACKInBatches(<< >>, packetsNotCommitted)
           IN /\ infoOk
              \* Here we ignore ack of UPTODATE.
              /\ UpdateElectionVote(i, acceptedEpoch[i])
              /\ FollowerLogRequestInBatches(i, j, ms_ack, packetsNotCommitted)
              /\ FollowerCommitInBatches(i)
              /\ packetsSync' = [packetsSync EXCEPT ![i].notCommitted = << >>,
                                                    ![i].committed = << >> ]
              /\ zabState' = [zabState EXCEPT ![i] = BROADCAST ]
        /\ UNCHANGED <<state, acceptedEpoch, currentEpoch, initialHistory, leaderVars,
                leaderAddr, electionVars, envVars, verifyVars>>
        /\ UpdateRecorder(<<"FollowerProcessUPTODATE", i, j>>)
        /\ UpdateAfterAction 
-----------------------------------------------------------------------------
IncZxid(s, zxid) == IF currentEpoch[s] = zxid[1] THEN <<zxid[1], zxid[2] + 1>>
                    ELSE <<currentEpoch[s], 1>>

(* Leader receives client propose and broadcasts PROPOSAL. See processRequest
   in ProposalRequestProcessor and propose in Leader for details. Since 
   prosalProcessor.processRequest -> syncProcessor.processRequest ->
   ackProcessor.processRequest -> leader.processAck, we initially set 
   txn.ackSid = {i}, assuming we have done leader.processAck.
   Note: In production, any server in traffic can receive requests and 
         forward it to leader if necessary. We choose to let leader be
         the sole one who can receive write requests, to simplify spec 
         and keep correctness at the same time.
*)
LeaderProcessRequest(i) == 
        /\ CheckTransactionNum 
        /\ IsON(i)
        /\ IsLeader(i)
        /\ zabState[i] = BROADCAST
        /\ LET inBroadcast == {s \in forwarding[i]: zabState[s] = BROADCAST}
           IN IsQuorum(inBroadcast)
        /\ LET request_value == GetRecorder("nClientRequest") \* unique value
               newTxn == [ zxid   |-> IncZxid(i, LastProposed(i).zxid),
                           value  |-> request_value, 
                           ackSid |-> {i}, \* assume we have done leader.processAck
                           epoch  |-> acceptedEpoch[i] ]
               m_proposal == [ mtype |-> PROPOSAL,
                               mzxid |-> newTxn.zxid,
                               mdata |-> request_value ]
               m_proposal_for_checking == [ source |-> i,
                                            epoch  |-> acceptedEpoch[i],
                                            zxid   |-> newTxn.zxid,
                                            data   |-> request_value ]
           IN /\ history' = [history EXCEPT ![i] = Append(@, newTxn) ]
              /\ Broadcast(i, i, m_proposal, FALSE)
              /\ proposalMsgsLog' = proposalMsgsLog \union {m_proposal_for_checking}
        /\ UNCHANGED <<serverVars, initialHistory, lastCommitted, lastProcessed, leaderVars,
                followerVars, electionVars, envVars, epochLeader, daInv>>
        /\ LET len == Len(history'[i])
               newZxid == history'[i][len].zxid 
           IN UpdateRecorder(<<"LeaderProcessRequest", i, newZxid>>)
        /\ UpdateAfterAction 

(* Follower processes PROPOSAL in BROADCAST. See processPacket
   in Follower for details. *)
FollowerProcessPROPOSAL(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ PendingPROPOSAL(i, j)
        /\ zabState[i] = BROADCAST
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLeader(i, j)
               isNext == IsNextZxid( LastQueued(i).zxid, msg.mzxid)
               newTxn == [ zxid   |-> msg.mzxid,
                           value  |-> msg.mdata,
                           ackSid |-> {},
                           epoch  |-> acceptedEpoch[i] ]
               m_ack  == [ mtype |-> ACK,
                           mzxid |-> msg.mzxid ]
          IN /\ infoOk 
             /\ \/ /\ isNext
                   /\ UNCHANGED daInv  
                \/ /\ ~isNext
                   /\ PrintT("Exception: Follower receives PROPOSAL, while" \o 
                        " the transaction is not the next.")
                   /\ daInv' = [daInv EXCEPT !.proposalConsistent = FALSE ]
             /\ history' = [history EXCEPT ![i] = Append(@, newTxn)]
             /\ Reply(i, j, m_ack)
        /\ UNCHANGED <<serverVars, initialHistory, lastCommitted, lastProcessed, leaderVars,
                followerVars, electionVars, envVars, verifyVars>>
        /\ LET msg == rcvBuffer[j][i][1]
           IN UpdateRecorder(<<"FollowerProcessPROPOSAL", i, j, msg.mzxid>>)
        /\ UpdateAfterAction 

\* See outstandingProposals in Leader
OutstandingProposals(i) == IF zabState[i] /= BROADCAST THEN << >>
                           ELSE SubSeq( history[i], lastCommitted[i].index + 1,
                                 Len(history[i]) )

LastAckIndexFromFollower(i, j) == 
        LET set_index == {idx \in 1..Len(history[i]): j \in history[i][idx].ackSid }
        IN Maximum(set_index)

\* See commit in Leader for details.
LeaderCommit(s, follower, index, zxid) ==
        /\ lastCommitted' = [lastCommitted EXCEPT ![s] = [ index |-> index,
                                                           zxid  |-> zxid ] ]
        /\ LET m_commit == [ mtype |-> COMMIT,
                             mzxid |-> zxid ]
           IN Broadcast(s, follower, m_commit, TRUE)

\* Try to commit one operation, called by LeaderProcessAck.
\* See tryToCommit in Leader for details.
\* commitProcessor.commit -> processWrite -> toBeApplied.processRequest
\* -> finalProcessor.processRequest, finally processTxn will be implemented
\* and lastProcessed will be updated. So we update it here.
LeaderTryToCommit(s, index, zxid, newTxn, follower) ==
        LET allTxnsBeforeCommitted == lastCommitted[s].index >= index - 1
                    \* Only when all proposals before zxid has been committed,
                    \* this proposal can be permitted to be committed.
            hasAllQuorums == IsQuorum(newTxn.ackSid)
                    \* In order to be committed, a proposal must be accepted
                    \* by a quorum.
            ordered == lastCommitted[s].index + 1 = index
                    \* Commit proposals in order.
        IN \/ /\ \* Current conditions do not satisfy committing the proposal.
                 \/ ~allTxnsBeforeCommitted
                 \/ ~hasAllQuorums
              /\ Discard(follower, s)
              /\ UNCHANGED <<daInv, lastCommitted, lastProcessed>>
           \/ /\ allTxnsBeforeCommitted
              /\ hasAllQuorums
              /\ \/ /\ ~ordered
                    /\ PrintT("Warn: Committing zxid " \o ToString(zxid) \o " not first.")
                    /\ daInv' = [daInv EXCEPT !.commitConsistent = FALSE]
                 \/ /\ ordered
                    /\ UNCHANGED daInv
              /\ LeaderCommit(s, follower, index, zxid)
              /\ lastProcessed' = [lastProcessed EXCEPT ![s] = [ index |-> index,
                                                                 zxid  |-> zxid ] ]

(* Leader Keeps a count of acks for a particular proposal, and try to
   commit the proposal. See case Leader.ACK in LearnerHandler,
   processRequest in AckRequestProcessor, and processAck in Leader for
   details. *)
LeaderProcessACK(i, j) ==
        /\ IsON(i)
        /\ IsLeader(i)
        /\ PendingACK(i, j)
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLearner(i, j)
               outstanding == LastCommitted(i).index < LastProposed(i).index
                        \* outstandingProposals not null
               hasCommitted == ~ZxidComparePredicate( msg.mzxid, LastCommitted(i).zxid)
                        \* namely, lastCommitted >= zxid
               index == ZxidToIndex(history[i], msg.mzxid)
               exist == index >= 1 /\ index <= LastProposed(i).index
                        \* the proposal exists in history
               ackIndex == LastAckIndexFromFollower(i, j)
               monotonicallyInc == \/ ackIndex = -1
                                   \/ ackIndex + 1 = index
                        \* TCP makes everytime ackIndex should just increase by 1
           IN /\ infoOk 
              /\ \/ /\ exist
                    /\ monotonicallyInc
                    /\ LET txn == history[i][index]
                           txnAfterAddAck == [ zxid   |-> txn.zxid,
                                               value  |-> txn.value,
                                               ackSid |-> txn.ackSid \union {j} ,
                                               epoch  |-> txn.epoch ]   
                       IN \* p.addAck(sid)
                       /\ history' = [history EXCEPT ![i][index] = txnAfterAddAck ] 
                       /\ \/ /\ \* Note: outstanding is 0. 
                                \* / proposal has already been committed.
                                \/ ~outstanding
                                \/ hasCommitted
                             /\ Discard(j, i)
                             /\ UNCHANGED <<daInv, lastCommitted, lastProcessed>>
                          \/ /\ outstanding
                             /\ ~hasCommitted
                             /\ LeaderTryToCommit(i, index, msg.mzxid, txnAfterAddAck, j)
                 \/ /\ \/ ~exist
                       \/ ~monotonicallyInc
                    /\ PrintT("Exception: No such zxid. " \o 
                           " / ackIndex doesn't inc monotonically.")
                    /\ daInv' = [daInv EXCEPT !.ackConsistent = FALSE ]
                    /\ Discard(j, i)
                    /\ UNCHANGED <<history, lastCommitted, lastProcessed>>
        /\ UNCHANGED <<serverVars, initialHistory, leaderVars, followerVars, electionVars,
                envVars, verifyVars>>
        /\ LET msg == rcvBuffer[j][i][1]
           IN  UpdateRecorder(<<"LeaderProcessACK", i, j, msg.mzxid>>)
        /\ UpdateAfterAction 

(* Follower processes COMMIT in BROADCAST. See processPacket
   in Follower for details. *)
FollowerProcessCOMMIT(i, j) ==
        /\ IsON(i)
        /\ IsFollower(i)
        /\ PendingCOMMIT(i, j)
        /\ zabState[i] = BROADCAST
        /\ LET msg == rcvBuffer[j][i][1]
               infoOk == IsMyLeader(i, j)
               pendingTxns == PendingTxns(i)
               noPending == Len(pendingTxns) = 0
           IN
           /\ infoOk  
           /\ \/ /\ noPending
                 /\ PrintT("Warn: Committing zxid without seeing txn.")
                 /\ UNCHANGED <<lastCommitted, lastProcessed, daInv>>
              \/ /\ ~noPending
                 /\ LET firstElementZxid == pendingTxns[1].zxid
                        match == ZxidEqualPredicate(firstElementZxid, msg.mzxid)
                    IN 
                    \/ /\ ~match
                       /\ PrintT("Exception: Committing zxid not equals" \o
                                " next pending txn zxid.")
                       /\ daInv' = [daInv EXCEPT !.commitConsistent = FALSE]
                       /\ UNCHANGED <<lastCommitted, lastProcessed>>
                    \/ /\ match
                       /\ lastCommitted' = [lastCommitted EXCEPT 
                                ![i] = [ index |-> lastCommitted[i].index + 1,
                                         zxid  |-> firstElementZxid ] ]
                       /\ lastProcessed' = [lastProcessed EXCEPT 
                                ![i] = [ index |-> lastCommitted[i].index + 1,
                                         zxid  |-> firstElementZxid ] ]
                       /\ UNCHANGED daInv
        /\ Discard(j, i)
        /\ UNCHANGED <<serverVars, history, initialHistory, leaderVars, followerVars,
                electionVars, envVars, verifyVars>>
        /\ LET msg == rcvBuffer[j][i][1]
           IN  UpdateRecorder(<<"FollowerProcessCOMMIT", i, j, msg.mzxid>>)
        /\ UpdateAfterAction 
-----------------------------------------------------------------------------
(* Used to discard some messages which should not exist in network channel.
   This action should not be triggered. *)
FilterNonexistentMessage(i) ==
        /\ \E j \in Server \ {i}: /\ rcvBuffer[j][i] /= << >>
                                  /\ LET msg == rcvBuffer[j][i][1]
                                     IN 
                                        \/ /\ IsLeader(i)
                                           /\ LET infoOk == IsMyLearner(i, j)
                                              IN
                                              \/ msg.mtype = LEADERINFO
                                              \/ msg.mtype = NEWLEADER
                                              \/ msg.mtype = UPTODATE
                                              \/ msg.mtype = PROPOSAL
                                              \/ msg.mtype = COMMIT
                                              \/ /\ ~infoOk
                                                 /\ \/ msg.mtype = FOLLOWERINFO
                                                    \/ msg.mtype = ACKEPOCH
                                                    \/ msg.mtype = ACKLD
                                                    \/ msg.mtype = ACK
                                        \/ /\ IsFollower(i)
                                           /\ LET infoOk == IsMyLeader(i, j) 
                                              IN
                                              \/ msg.mtype = FOLLOWERINFO
                                              \/ msg.mtype = ACKEPOCH
                                              \/ msg.mtype = ACKLD
                                              \/ msg.mtype = ACK
                                              \/ /\ ~infoOk
                                                 /\ \/ msg.mtype = LEADERINFO
                                                    \/ msg.mtype = NEWLEADER
                                                    \/ msg.mtype = UPTODATE
                                                    \/ msg.mtype = PROPOSAL
                                                    \/ msg.mtype = COMMIT   
                                        \/ IsLooking(i)
                                  /\ Discard(j, i)
        /\ daInv' = [daInv EXCEPT !.messageLegal = FALSE ]
        /\ UNCHANGED <<serverVars, logVars, leaderVars, followerVars, electionVars,
                envVars, verifyVars>>
        /\ UnchangeRecorder      
        /\ UpdateAfterAction   
-----------------------------------------------------------------------------
\* Next
Next == \/ /\ ~AfterInitState
           /\ SetInitState
        \/ /\ AfterInitState
           /\ 
        (* FLE modlue *)
           \* \/ \E i \in Server:    ElectLeader(i)
           \* \/ \E i \in Server:    FollowLeader(i)
              \/ \E i \in Server, S \in Quorums: ElectionAndDiscovery(i, S)
        (* Some conditions like failure, network delay *)
           \* \/ \E i \in Server:    FollowerTimeout(i)
           \* \/ \E i \in Server:    LeaderTimeout(i)
           \* \/ \E i, j \in Server: Timeout(i, j)
              \/ \E i, j \in Server: PartitionStart(i, j)
              \/ \E i, j \in Server: PartitionRecover(i, j)
              \/ \E i \in Server:    NodeCrash(i)
              \/ \E i \in Server:    NodeStart(i)
        (* Zab module - Discovery and Synchronization part *)
           \* \/ \E i, j \in Server: ConnectAndFollowerSendFOLLOWERINFO(i, j)
           \* \/ \E i, j \in Server: LeaderProcessFOLLOWERINFO(i, j)
           \* \/ \E i, j \in Server: FollowerProcessLEADERINFO(i, j)
           \* \/ \E i, j \in Server: LeaderProcessACKEPOCH(i, j)
              \/ \E i, j \in Server: LeaderSyncFollower(i, j)
              \/ \E i, j \in Server: FollowerProcessSyncMessage(i, j)
              \/ \E i, j \in Server: FollowerProcessPROPOSALInSync(i, j)
              \/ \E i, j \in Server: FollowerProcessCOMMITInSync(i, j)
              \/ \E i, j \in Server: FollowerProcessNEWLEADER(i, j)
              \/ \E i, j \in Server: LeaderProcessACKLD(i, j)
              \/ \E i, j \in Server: FollowerProcessUPTODATE(i, j)
        (* Zab module - Broadcast part *)
              \/ \E i \in Server:    LeaderProcessRequest(i)
              \/ \E i, j \in Server: FollowerProcessPROPOSAL(i, j)
              \/ \E i, j \in Server: LeaderProcessACK(i, j) \* Sync + Broadcast
              \/ \E i, j \in Server: FollowerProcessCOMMIT(i, j)
        (* An action used to judge whether there are redundant messages in network *)
              \/ \E i \in Server:    FilterNonexistentMessage(i)
       \* /\ UpdateAfterAction

Spec == Init /\ [][Next]_vars
------

Value == Nat

ServersIncNullPoint == Server \union {NullPoint} 

Zxid ==
    Seq(Nat \union {-1}) 
    
HistoryItem ==
     [ zxid: Zxid,
       value: Value,
       ackSid: SUBSET Server,
       epoch: Nat ]    
    
Proposal ==
    [ source: Server, 
      epoch: Nat,
      zxid: Zxid,
      data: Value ]   

LastItem ==
    [ index: Nat, zxid: Zxid ]

SyncPackets == 
    [ notCommitted: Seq(HistoryItem),
      committed: Seq(Zxid) ]

Message ==
    [ mtype: {FOLLOWERINFO}, mzxid: Zxid ] \union
    [ mtype: {LEADERINFO}, mzxid: Zxid ] \union
    [ mtype: {ACKEPOCH}, mzxid: Zxid, mepoch: Nat \union {-1} ] \union
    [ mtype: {DIFF}, mzxid: Zxid ] \union 
    [ mtype: {TRUNC}, mtruncZxid: Zxid ] \union 
    [ mtype: {PROPOSAL}, mzxid: Zxid, mdata: Value ] \union 
    [ mtype: {COMMIT}, mzxid: Zxid ] \union 
    [ mtype: {NEWLEADER}, mzxid: Zxid ] \union 
    [ mtype: {ACKLD}, mzxid: Zxid ] \union 
    [ mtype: {ACK}, mzxid: Zxid ] \union 
    [ mtype: {UPTODATE}, mzxid: Zxid ]

ElectionState == {LOOKING, FOLLOWING, LEADING}

ZabState == {ELECTION, DISCOVERY, SYNCHRONIZATION, BROADCAST}

DaInvSet == {"stateConsistent", "proposalConsistent", 
                 "commitConsistent", "ackConsistent", 
                 "messageLegal" }

AaInvSet == {"leadership", "prefixConsistency", "integrity",      
              "agreement", "totalOrder", "primaryOrder", "monotonicRead",
              "processConsistency", "leaderLogCompleteness",
              "committedLogDurability" }

Connecting == [ sid : Server,
                connected: BOOLEAN ]

AckldRecv  == Connecting

Electing == [ sid: Server,
              peerLastZxid: Zxid,
              inQuorum: BOOLEAN  ]

TypeOK == 
    /\ state \in [Server -> ElectionState]
    /\ zabState \in [Server -> ZabState]
    /\ acceptedEpoch \in [Server -> Nat]
    /\ currentEpoch \in [Server -> Nat]
    /\ history \in [Server -> Seq(HistoryItem)]
    /\ initialHistory \in [Server -> Seq(HistoryItem)] 
    /\ lastCommitted \in [Server -> LastItem]
    /\ lastProcessed \in [Server -> LastItem]
    /\ learners \in [Server -> SUBSET Server]
    /\ connecting \in [Server -> SUBSET Connecting]
    /\ electing \in [Server -> SUBSET Electing]
    /\ ackldRecv \in [Server -> SUBSET AckldRecv]
    /\ forwarding \in [Server -> SUBSET Server]
    /\ tempMaxEpoch \in [Server -> Nat]
    /\ leaderAddr \in [Server -> ServersIncNullPoint]
    /\ packetsSync \in [Server -> SyncPackets]
    /\ leaderOracle \in ServersIncNullPoint
    /\ rcvBuffer \in [Server -> [Server -> Seq(Message)]]
    /\ status \in [Server -> {ONLINE, OFFLINE}]
    /\ partition \in [Server -> [Server -> BOOLEAN ]]
    /\ proposalMsgsLog \in SUBSET Proposal
    /\ epochLeader \in [1..MAXEPOCH -> SUBSET Server]
    /\ daInv \in [DaInvSet -> BOOLEAN ]
    /\ aaInv \in [AaInvSet -> BOOLEAN ] 
    /\ committedLog \in Seq(HistoryItem)
=============================================================================