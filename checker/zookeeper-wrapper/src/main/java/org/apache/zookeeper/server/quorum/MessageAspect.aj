package org.apache.zookeeper.server.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public aspect MessageAspect {

    private static final Logger LOG = LoggerFactory.getLogger(MessageAspect.class);

    // A new field in QuorumCnxManager.Message to keep track of the message id

    private int QuorumCnxManager.Message.messageId;

    public int QuorumCnxManager.Message.getMessageId() {
        return messageId;
    }

    // Intercept QuorumCnxManager.Message constructor to set the id of the message in flight

    pointcut messageInitialization(QuorumCnxManager.Message message):
            initialization(QuorumCnxManager.Message.new(..)) && this(message);

    after(final QuorumCnxManager.Message message) returning: messageInitialization(message) {
        final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();
        try {
            message.messageId = quorumPeerAspect.getTestingService().getMessageInFlight();
            LOG.debug("Got messageInFlight id = {}", message.messageId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }
}
