package org.apache.zookeeper.server.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect QuorumCnxManagerAspect {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManagerAspect.class);

    // Intercept operating on the recvQueue within QuorumCnxManager.addToRecvQueue

//    // For version 3.4 & 3.5
//    pointcut addToRecvQueue():
//            withincode(* QuorumCnxManager.addToRecvQueue(..))
//            && call(* java.util.concurrent.ArrayBlockingQueue.add(..));
//
//    after() returning: addToRecvQueue() {
//        final WorkerReceiverAspect workerReceiverAspect = WorkerReceiverAspect.aspectOf();
//        workerReceiverAspect.getMsgsInRecvQueue().incrementAndGet();
//    }
//
//    pointcut removeFromRecvQueue():
//            withincode(* QuorumCnxManager.addToRecvQueue(..))
//            && call(* java.util.concurrent.ArrayBlockingQueue.remove());
//
//    after() returning: removeFromRecvQueue() {
//        final WorkerReceiverAspect workerReceiverAspect = WorkerReceiverAspect.aspectOf();
//        workerReceiverAspect.getMsgsInRecvQueue().decrementAndGet();
//    }

    // For version 3.6 & 3.7 & 3.8
    pointcut addToRecvQueue():
            withincode(* QuorumCnxManager.addToRecvQueue(..))
            && call(* java.util.concurrent.BlockingQueue.offer(..));

    after() returning: addToRecvQueue() {
        final WorkerReceiverAspect workerReceiverAspect = WorkerReceiverAspect.aspectOf();
        workerReceiverAspect.getMsgsInRecvQueue().incrementAndGet();
    }
}
