package org.apache.zookeeper.server.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect QuorumCnxManagerAspect {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManagerAspect.class);

    // Intercept operating on the recvQueue within QuorumCnxManager.addToRecvQueue
    // For version 3.6+
    pointcut addToRecvQueue():
            withincode(* QuorumCnxManager.addToRecvQueue(..))
            && call(* java.util.concurrent.BlockingQueue.offer(..));

    after() returning: addToRecvQueue() {
        final WorkerReceiverAspect workerReceiverAspect = WorkerReceiverAspect.aspectOf();
        workerReceiverAspect.getMsgsInRecvQueue().incrementAndGet();
    }
}
