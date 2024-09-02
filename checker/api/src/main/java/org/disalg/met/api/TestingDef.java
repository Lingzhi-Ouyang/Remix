package org.disalg.met.api;

public class TestingDef {
    public interface RetCode {
        int CLIENT_INITIALIZATION_NOT_DONE = 0;
        int NODE_CRASH = -1;
        int NODE_PAIR_IN_PARTITION = -2;
        int NO_WAIT = -3;
        int SUBNODE_UNREGISTERED = -4;

        int UNDEFINED = -10;

        int NOT_INTERCEPTED = -100;
        int BACK_TO_LOOKING = -200;
        int EXIT = -400;
    }

    public interface OpCode {
        int notification = 0;
        int create = 1;
        int delete = 2;
        int exists = 3;
        int getData = 4;
        int setData = 5;
        int getACL = 6;
        int setACL = 7;
        int getChildren = 8;
        int sync = 9;
        int ping = 11;
        int getChildren2 = 12;
        int check = 13;
        int multi = 14;
        int auth = 100;
        int setWatches = 101;
        int sasl = 102;
        int createSession = -10;
        int closeSession = -11;
        int error = -1;
    }

    public interface MessageType {
        int DIFF = 13;
        int TRUNC = 14;
        int SNAP = 15;
        int OBSERVERINFO = 16;
        int NEWLEADER = 10;
        int FOLLOWERINFO = 11;
        int UPTODATE = 12;
        int LEADERINFO = 17;
        int ACKEPOCH = 18;
        int REQUEST = 1;
        int PROPOSAL = 2;
        int PROPOSAL_IN_SYNC = -1;
        int ACK = 3;
        int COMMIT = 4;
        int PING = 5;
        int REVALIDATE = 6;
        int SYNC = 7;
        int INFORM = 8;
        int waitForEpochAck = 200;
        int learnerHandlerReadRecord = 300;
        int leaderJudgingIsRunning = 400;
    }

}
