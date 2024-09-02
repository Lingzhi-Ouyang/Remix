package org.apache.zookeeper.server.quorum;

public aspect NotificationAspect {

    // A new field in FastLeaderElection.Notification to keep tract of the message id

    private int FastLeaderElection.Notification.messageId;

    public int FastLeaderElection.Notification.getMessageId() {
        return messageId;
    }

    // Intercept Notification constructor and set the message id

    pointcut notificationInitialization(FastLeaderElection.Notification notification):
            initialization(FastLeaderElection.Notification.new(..)) && this(notification);

    after(final FastLeaderElection.Notification notification) returning: notificationInitialization(notification) {
        final WorkerReceiverAspect workerReceiverAspect = WorkerReceiverAspect.aspectOf();
        notification.messageId = workerReceiverAspect.getResponse().getMessageId();
    }
}
