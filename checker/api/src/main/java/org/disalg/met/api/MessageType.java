package org.disalg.met.api;

public class MessageType {
    /**
     * This message is for follower to expect diff
     */
    public final static int DIFF = 13;

    /**
     * This is for follower to truncate its logs
     */
    public final static int TRUNC = 14;

    /**
     * This is for follower to download the snapshots
     */
    public final static int SNAP = 15;

    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    public final static int OBSERVERINFO = 16;

    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    public final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    public final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    public final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;

    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    public final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;

    public final static int PROPOSAL_IN_SYNC = -1;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    public final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    public final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    public final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    public final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    public final static int SYNC = 7;

    /**
     * This message type informs observers of a committed proposal.
     */
    public final static int INFORM = 8;
}
