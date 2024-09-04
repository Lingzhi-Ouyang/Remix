package org.disalg.remix.zookeeper;

import org.disalg.remix.api.configuration.SchedulerConfigurationException;
import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;

public class ZookeeperMain {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMain.class);

    public static void main(final String[] args) {
        final ApplicationContext applicationContext = new AnnotationConfigApplicationContext(ZookeeperSpringConfig.class);
        final ReplayService replayService = applicationContext.getBean(ReplayService.class);

        try {
            replayService.loadConfig(args);
            replayService.initRemote();
            replayService.startWithExternalModel();
            System.exit(0);
        } catch (final SchedulerConfigurationException e) {
            LOG.error("Error while reading configuration.", e);
        } catch (final IOException e) {
            LOG.error("IO exception", e);
        }
    }

}
