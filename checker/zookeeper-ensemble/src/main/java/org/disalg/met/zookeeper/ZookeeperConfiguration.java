package org.disalg.met.zookeeper;

import org.disalg.met.api.configuration.SchedulerConfiguration;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.api.configuration.SchedulerConfigurationPostLoadListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ZookeeperConfiguration implements SchedulerConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperConfiguration.class);

    private static final String DEFAULT_NUM_NODES = "3";
    private static final String DEFAULT_NUM_CRASHES = "0";
    private static final String DEFAULT_NUM_REBOOTS = "0";
    private static final String DEFAULT_NUM_CRASHES_AFTER_ELECTION = "0";
    private static final String DEFAULT_NUM_REBOOTS_AFTER_ELECTION = "0";
    private static final String DEFAULT_MAX_EVENTS = "100";
    private static final String DEFAULT_NUM_PRIORITY_CHANGE_POINTS = "1";
    private static final String DEFAULT_TICK_TIME = "1000"; // this decides the frequency of ping

    private static final String DEFAULT_INIT_LIMIT = "5";
    private static final String DEFAULT_SYNC_LIMIT = "10";
    private static final String DEFAULT_CLIENT_PORT = "6000";
    private static final String DEFAULT_BASE_QUORUM_PORT = "2000";
    private static final String DEFAULT_BASE_LEADER_ELECTION_PORT = "5000";
    private static final String DEFAULT_ADMIN_SERVER_PORT = "3000";

    private static final String DEFAULT_NUM_EXECUTIONS = "1";
    private static final String DEFAULT_EXECUTION_FILE = "execution";
    private static final String DEFAULT_STATISTICS_FILE = "statistics";
    private static final String DEFAULT_BUG_REPORT_FILE = "bugReport";
    private static final String DEFAULT_MATCH_REPORT_FILE = "matchReport";

    private static final String DEFAULT_NUM_CLIENTS = "1";
    private static final String DEFAULT_NUM_READERS = "1";
    private static final String DEFAULT_NUM_WRITERS = "1";
    private static final String DEFAULT_NUM_CLIENT_REQUESTS = "5";

    private static final String DEFAULT_SCHEDULING_STRATEGY = "pctcp";

    private int numNodes;
    private int numCrashes;
    private int numReboots;
    private int numCrashesAfterElection;
    private int numRebootsAfterElection;
    private int numClients;
    private int numReaders;
    private int numWriters;
    private int numClientRequests;
    private int maxEvents;
    private int numPriorityChangePoints;
    private boolean hasRandomSeed = false;
    private long randomSeed;
    private long tickTime;

    private String classpath;
    private File workingDir;
    private File traceDir;
    private File log4JConfig;
    private int initLimit;
    private int syncLimit;
    private int clientPort;
    private int baseQuorumPort;
    private int baseLeaderElectionPort;
    private int adminServerPort;

    private int numExecutions;
    private String executionFile;
    private String statisticsFile;
    private String bugReportFile;
    private String matchReportFile;

    private String schedulingStrategy;

    private final List<SchedulerConfigurationPostLoadListener> listeners = new ArrayList<>();

    @Override
    public void load(final String[] args) throws SchedulerConfigurationException {
        if (args.length < 1) {
            LOG.error("Please provide a configuration file as a command-line argument.");
            throw new SchedulerConfigurationException();
        }

        if (args.length < 2) {
            LOG.error("Please provide a tag as a command-line argument.");
            throw new SchedulerConfigurationException();
        }

        final Properties properties = new Properties();

        try {
            final FileInputStream fileInputStream = new FileInputStream(args[0]);
            properties.load(fileInputStream);
        } catch (final IOException e) {
            LOG.error("Error while loading configuration from file {}", args[0]);
            throw new SchedulerConfigurationException(e);
        }

        numNodes = Integer.parseInt(properties.getProperty("numNodes", DEFAULT_NUM_NODES));
        numCrashes = Integer.parseInt(properties.getProperty("numCrashes", DEFAULT_NUM_CRASHES));
        numReboots = Integer.parseInt(properties.getProperty("numReboots", DEFAULT_NUM_REBOOTS));
        numCrashesAfterElection = Integer.parseInt(properties.getProperty("numCrashesAfterElection", DEFAULT_NUM_CRASHES_AFTER_ELECTION));
        numRebootsAfterElection = Integer.parseInt(properties.getProperty("numRebootsAfterElection", DEFAULT_NUM_REBOOTS_AFTER_ELECTION));

        numClients = Integer.parseInt(properties.getProperty("numClients", DEFAULT_NUM_CLIENTS));
        numReaders = Integer.parseInt(properties.getProperty("numReaders", DEFAULT_NUM_READERS));
        numWriters = Integer.parseInt(properties.getProperty("numWriters", DEFAULT_NUM_WRITERS));
        numClientRequests = Integer.parseInt(properties.getProperty("numClientRequests", DEFAULT_NUM_CLIENT_REQUESTS));

        maxEvents = Integer.parseInt(properties.getProperty("maxEvents", DEFAULT_MAX_EVENTS));
        numPriorityChangePoints = Integer.parseInt(properties.getProperty("numPriorityChangePoints", DEFAULT_NUM_PRIORITY_CHANGE_POINTS));
        final String randomSeedProperty = properties.getProperty("randomSeed");
        if (null != randomSeedProperty) {
            hasRandomSeed = true;
            randomSeed = Long.parseLong(randomSeedProperty);
        }
        tickTime = Long.parseLong(properties.getProperty("tickTime", DEFAULT_TICK_TIME));

        classpath = properties.getProperty("classpath");

        String userDir = properties.getProperty("workingDir", System.getProperty("user.dir"));
        workingDir = new File(userDir, args[1]);
        LOG.debug("Working dir: {}", workingDir);

        traceDir = new File(userDir, properties.getProperty("traceDir", "traces"));
        LOG.debug("trace dir: {}", traceDir);

        log4JConfig = new File(properties.getProperty("log4JConfig", "zk_log.properties"));
        initLimit = Integer.parseInt(properties.getProperty("initLimit", DEFAULT_INIT_LIMIT));
        syncLimit = Integer.parseInt(properties.getProperty("syncLimit", DEFAULT_SYNC_LIMIT));
        clientPort = Integer.parseInt(properties.getProperty("clientPort", DEFAULT_CLIENT_PORT));
        baseQuorumPort = Integer.parseInt(properties.getProperty("baseQuorumPort", DEFAULT_BASE_QUORUM_PORT));
        baseLeaderElectionPort = Integer.parseInt(properties.getProperty("initLimit", DEFAULT_BASE_LEADER_ELECTION_PORT));
        adminServerPort = Integer.parseInt(properties.getProperty("admin.serverPort", DEFAULT_ADMIN_SERVER_PORT));

        numExecutions = Integer.parseInt(properties.getProperty("numExecutions", DEFAULT_NUM_EXECUTIONS));
        executionFile = properties.getProperty("executionFile", DEFAULT_EXECUTION_FILE);
        statisticsFile = properties.getProperty("statisticsFile", DEFAULT_STATISTICS_FILE);
        bugReportFile = properties.getProperty("bugReportFile", DEFAULT_BUG_REPORT_FILE);
        matchReportFile = properties.getProperty("matchReportFile", DEFAULT_MATCH_REPORT_FILE);

        schedulingStrategy = properties.getProperty("schedulingStrategy", DEFAULT_SCHEDULING_STRATEGY);

        notifyPostLoadListeners();
    }

    @Override
    public void registerPostLoadListener(final SchedulerConfigurationPostLoadListener listener) {
        listeners.add(listener);
    }

    @Override
    public void notifyPostLoadListeners() throws SchedulerConfigurationException {
        for (final SchedulerConfigurationPostLoadListener listener : listeners) {
            listener.postLoadCallback();
        }
    }

    @Override
    public int getNumNodes() {
        return numNodes;
    }

    @Override
    public void setNumNodes(int num) {
        numNodes = num;
    }

    @Override
    public int getNumCrashes() {
        return numCrashes;
    }

    @Override
    public int getNumReboots() {
        return numReboots;
    }

    @Override
    public int getNumCrashesAfterElection() {
        return numCrashesAfterElection;
    }

    @Override
    public int getNumRebootsAfterElection() {
        return numRebootsAfterElection;
    }

    @Override
    public int getNumClients() {
        return numClients;
    }

    @Override
    public int getNumReaders() {
        return numReaders;
    }

    @Override
    public int getNumWriters() {
        return numWriters;
    }

    @Override
    public int getNumClientRequests() {
        return numClientRequests;
    }

    @Override
    public int getMaxEvents() {
        return maxEvents;
    }

    @Override
    public int getNumPriorityChangePoints() {
        return numPriorityChangePoints;
    }

    @Override
    public boolean hasRandomSeed() {
        return hasRandomSeed;
    }

    @Override
    public long getRandomSeed() {
        return randomSeed;
    }

    @Override
    public long getTickTime() {
        return tickTime;
    }

    public String getClasspath() {
        return classpath;
    }

    public File getWorkingDir() {
        return workingDir;
    }

    public File getTraceDir() {
        return traceDir;
    }

    public File getLog4JConfig() {
        return log4JConfig;
    }

    public int getInitLimit() {
        return initLimit;
    }

    public int getSyncLimit() {
        return syncLimit;
    }

    @Override
    public int getClientPort() {
        return clientPort;
    }

    public int getAdminServerPort() {
        return adminServerPort;
    }

    public int getBaseQuorumPort() {
        return baseQuorumPort;
    }

    public int getBaseLeaderElectionPort() {
        return baseLeaderElectionPort;
    }

    @Override
    public int getNumExecutions() {
        return numExecutions;
    }

    @Override
    public String getExecutionFile() {
        return executionFile;
    }

    @Override
    public String getStatisticsFile() {
        return statisticsFile;
    }

    @Override
    public String getBugReportFile() {
        return bugReportFile;
    }

    @Override
    public String getMatchReportFile() {
        return matchReportFile;
    }

    @Override
    public String getSchedulingStrategy() {
        return schedulingStrategy;
    }

    @Override
    public void configureNode(final String executionId, final int nodeId, String tag) throws SchedulerConfigurationException {
        final File nodeDir = new File(getWorkingDir(), executionId + File.separator + tag + "s" + File.separator + nodeId);
        final File dataDir = new File(nodeDir, "data");

        // Create the data directory if it is missing
        dataDir.mkdirs();

        // Assemble the configuration file properties
        final Properties properties = new Properties();
        properties.setProperty("tickTime", String.valueOf(getTickTime()));
        properties.setProperty("initLimit", String.valueOf(getInitLimit()));
        properties.setProperty("syncLimit", String.valueOf(getSyncLimit()));
        properties.setProperty("dataDir", dataDir.getPath());
        final int clientPort = getClientPort() + nodeId;
        properties.setProperty("clientPort", String.valueOf(clientPort));
        for (int i = 0; i < getNumNodes(); ++i) {
            final int quorumPort = getBaseQuorumPort() + i;
            final int leaderElectionPort = getBaseLeaderElectionPort() + i;
            properties.setProperty("server." + i, "localhost:" + quorumPort + ":" + leaderElectionPort);
        }
        final int adminServerPort = getAdminServerPort() + nodeId;
        properties.setProperty("admin.serverPort", String.valueOf(adminServerPort));

        final File confFile = new File(nodeDir, "conf");
        final File myidFile = new File(dataDir, "myid");
        try {
            final FileWriter confFileWriter = new FileWriter(confFile);
            properties.store(confFileWriter, "Automatically generated configuration for " + tag + nodeId);
            confFileWriter.close();

            final FileWriter myidFileWriter = new FileWriter(myidFile);
            myidFileWriter.write(String.valueOf(nodeId));
            myidFileWriter.close();
        }
        catch (final IOException e) {
            LOG.error("Could not write to a configuration file for {} {}", tag, nodeId);
            throw new SchedulerConfigurationException(e);
        }
    }

}
