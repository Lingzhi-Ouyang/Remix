package org.disalg.met.api;

import org.disalg.met.api.state.LeaderElectionState;
import org.disalg.met.api.state.Vote;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;

public interface TestingRemoteService extends Remote {

    String REMOTE_NAME = "TestingRemoteService";

    /**
     * Registers a subnode of a node. This is usually a thread within the node that sends and receives messages.
     * The concept is necessary for systems in which several threads within one node send messages concurrently,
     * and all these threads need to be correctly synchronized. If a system only has one message-sending thread,
     * one (main) subnode suffices.
     *
     * <p>The subnode starts in the state <code>{@link SubnodeState.PROCESSING}</code>.</p>
     *
     * @param nodeId Node's id
//     * @param mainReceiver Indicates a subnode should move from <code>{@link SubnodeState.RECEIVING}</code> to
//     *                     <code>{@link SubnodeState.PROCESSING}</code> when a message for its node is released
    * @param subnodeType the type of a subnode
     * @return The scheduler generates and returns the subnode's identifier
     * @throws RemoteException
     */
    int registerSubnode(int nodeId, SubnodeType subnodeType) throws RemoteException;

    /**
     * Deregisters a subnode.
     *
     * @param subnodeId Subnode's id
     * @throws RemoteException
     */
    void deregisterSubnode(int subnodeId) throws RemoteException;

    /**
     * Notify the scheduler that the node is online.
     *
     * @param nodeId Node's id
     * @throws RemoteException
     */
    void nodeOnline(int nodeId) throws RemoteException;

    /**
     * Notify the scheduler that the node is offline
     * @param nodeId Node's id
     * @throws RemoteException
     */
    void nodeOffline(int nodeId) throws RemoteException;

    /**
     * Indicates a subnode is about to send a message during election. Change its state to <code>{@link SubnodeState.SENDING}</code>,
     * and blocks execution until the scheduler decides to release the message.
     *
     * @param sendingSubnodeId Id of the subnode that is sending the message
     * @param receivingNodeId Id of the *node* that is receiving the message
     * @param predecessorMessageIds Set of message identifiers that directly precede the current message
     * @param payload String representation of the message payload
     * @return The scheduler generates and returns a unique identifier of the current message
     */
    int offerElectionMessage(int sendingSubnodeId,
                             int receivingNodeId,
                             long electionEpoch,
                             int leader,
                             Set<Integer> predecessorMessageIds,
                             String payload) throws RemoteException;

    /**
     * Indicates an internal message between nodes is about to be sent. Change its state to <code>{@link SubnodeState.SENDING}</code>,
     * and blocks execution until the scheduler decides to release the message.
     *
     * @return The scheduler generates and returns a unique identifier of the current message
     */
    int offerFollowerToLeaderMessage(int sendingSubnodeId, long zxid, String payload, int type) throws RemoteException;

    /**
     * Indicates a learnerHandler subnode is about to send a message to a learner. Change its state to <code>{@link SubnodeState.SENDING}</code>,
     * and blocks execution until the scheduler decides to release the message.
     *
     * @return The scheduler generates and returns a unique identifier of the current message
     */
    int offerLeaderToFollowerMessage(int sendingSubnodeId, String receivingAddr, long zxid, String payload, int type) throws RemoteException;

    /**
     * Indicates a subnode is about to do a local event (e.g. sync a request log to disk, commit a request, etc). Change its state to <code>{@link SubnodeState.SENDING}</code>,
     * and blocks execution until the scheduler decides to release the message.
     *
     * @return The scheduler generates and returns a unique identifier of the current message
     */
    int offerLocalEvent(int subnodeId, SubnodeType subnodeType, long zxidOrEpoch, String payload, int type) throws RemoteException;

    /**
     * If the subnode is in the state <code>{@link SubnodeState.RECEIVING}</code>, change it to
     * <code>{@link SubnodeState.PROCESSING}</code>. In other cases the state remains unchanged.
     *
     * @param subnodeId The subnode's id
     * @throws RemoteException
     */
    void setProcessingState(int subnodeId) throws RemoteException;

    /**
     * If the subnode is in the state <code>{@link SubnodeState.PROCESSING}</code>, change it to
     * <code>{@link SubnodeState.RECEIVING}</code>. In other cases the state remains unchanged.
     *
     * @param subnodeId The subnode's id
     * @throws RemoteException
     */
    void setReceivingState(int subnodeId) throws RemoteException;

    /**
     * Returns the identifier of the message currently in flight
     * @return The message identifier
     */
    int getMessageInFlight() throws RemoteException;

    int getLogRequestInFlight() throws RemoteException;

    void updateVote(int nodeId, Vote vote) throws RemoteException;

    void initializeVote(int nodeId, Vote vote) throws RemoteException;

    void updateLeaderElectionState(int nodeId, LeaderElectionState state) throws RemoteException;

    void initializeLeaderElectionState(int nodeId, LeaderElectionState state) throws RemoteException;

    void updateLastProcessedZxid(int nodeId, long lastProcessedZxid) throws RemoteException;

    int writeLongToFile(int nodeId, String name, long e) throws RemoteException;

    void registerFollowerSocketInfo(int node, String socketAddress) throws RemoteException;

    void deregisterFollowerSocketInfo(int node) throws RemoteException;

    void readyForBroadcast(int subnodeId) throws RemoteException;

//    ClientRequest getNextClientRequest(int clientId, String result) throws RemoteException;
//
//    int replyLastResult(ClientRequest clientRequest) throws RemoteException;
}
