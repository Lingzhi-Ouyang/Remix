package org.disalg.met.server.executor;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.disalg.met.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperClient {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class);

    private static final int SESSION_TIME_OUT = 1000000;
    private static final String CONNECT_STRING = "127.0.0.1:4002,127.0.0.1:4001,127.0.0.1:4000";
    private static final String ZNODE_PATH = "/test";
    private static final String INITIAL_VAL = "0";

    private static CountDownLatch countDownLatch;

    private boolean isSyncConnected;

    private ClientProxy clientProxy;

    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            LOG.debug(">>>>>> event.getPath: " + event.getPath() +
                    " >>>>>> event.getType: " + event.getType() +
                    " >>>>>> event.getState: " + event.getState());
            if (Watcher.Event.KeeperState.SyncConnected.equals(event.getState())){
                LOG.debug("connected successfully!");
                countDownLatch.countDown();
                isSyncConnected = true;
            }
            else {
                try {
                    LOG.warn("非SyncConnected状态！");
                    // Do not close this session since it may become normal later
//                    countDownLatch = new CountDownLatch(1);
//                    isSyncConnected = false;
//                    zk.close();
                } catch (Exception e) {
                    LOG.debug("----caught Exception: {} ", e.toString());
                    e.printStackTrace();
                }
            }
        }
    };

    public CountDownLatch getCountDownLatch(){
        return countDownLatch;
    }

    public boolean syncConnected(){
        return isSyncConnected;
    }

    private ZooKeeper zk;

    public ZooKeeperClient(ClientProxy clientProxy, String serverList, boolean readOnly) throws IOException, InterruptedException, KeeperException {
        countDownLatch = new CountDownLatch(1);
        isSyncConnected = false;
        clientProxy = clientProxy;
        LOG.debug("---to get new ZooKeeperClient!");
//        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIME_OUT, watcher);
//        if (readOnly) {
//            zk = new ZooKeeper(serverList, SESSION_TIME_OUT, watcher, true);
//        } else {
//            zk = new ZooKeeper(serverList, SESSION_TIME_OUT, watcher);
//        }
        zk = new ZooKeeper(serverList, SESSION_TIME_OUT, watcher, true);
        LOG.debug("---new ZooKeeperClient got!");
    }

    public ZooKeeperClient(String serverList, boolean readOnly) throws IOException, InterruptedException, KeeperException {
        countDownLatch = new CountDownLatch(1);
        isSyncConnected = false;
        clientProxy = null;
        LOG.debug("---to get new ZooKeeperClient!");
        zk = new ZooKeeper(serverList, SESSION_TIME_OUT, watcher, true);
        LOG.debug("---new ZooKeeperClient got!");
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public boolean existTestPath() throws KeeperException, InterruptedException {
        return zk.exists(ZNODE_PATH, false) != null;
    }

    // In the test, the create request is only used in the session initialization phase
    public String create() throws KeeperException, InterruptedException {
        Stat stat = zk.exists(ZNODE_PATH, false);
        if (stat != null) {
            return null;
        }
        String createdPath = zk.create(ZNODE_PATH, INITIAL_VAL.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOG.debug("CREATE PATH {}: {}", createdPath, INITIAL_VAL);
        return createdPath;
    }

    // In the test, the create request is only used in the session initialization phase
    // Async
    public String create(String path) {
//        Stat stat = zk.exists(path, false);
//        if (stat != null) {
//            return null;
//        }
        zk.create(path,
                INITIAL_VAL.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                (rc, path1, ctx, name) -> {},
                "状态异步修改");
        LOG.debug("CREATE PATH {}: {}", path, INITIAL_VAL);
        return path;
    }

    public List<String> ls() throws KeeperException, InterruptedException {
        List<String> data = zk.getChildren(ZNODE_PATH, null);
        LOG.debug("ls {}: {}", ZNODE_PATH, data);
        return data;
    }

    public String getData() throws KeeperException, InterruptedException {
        byte[] data = zk.getData(ZNODE_PATH, false, null);
        String result = new String(data);
        LOG.debug("after GET data of {}: {}", ZNODE_PATH, result);
        return result;
    }

    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    LOG.debug("OK");
                    break;
                case CONNECTIONLOSS:
                    LOG.debug("CONNECTIONLOSS");
                    // TODO: reconnect
//                    getDataAsync(event);
                    return;
                default:
                    // 其他异常，抛出或记录异常
                    KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path);
                    LOG.error("get_data error", e);
            }
            String result = new String(data);
            LOG.debug("异步回调方法获取的数据："+path+":"+ result);
//            LinkedBlockingQueue<String> responseQueue = clientProxy.getResponseQueue();
//            responseQueue.add(result);
//            synchronized (clientProxy.getTestingService().getControlMonitor()) {
//                event.setData(result);
//                LOG.debug("set异步回调方法获取的数据："+path+":"+ result);
//                clientProxy.getTestingService().getControlMonitor().notifyAll();
//            }
        }
    };

    public void getDataAsync(final ClientRequestEvent event) {
        zk.getData(ZNODE_PATH, null, getDataCallback, "数据异步查询");
    }


    public void setData(String val) {
        int version = -1;
        zk.setData(ZNODE_PATH, val.getBytes(), version, null,"状态异步修改");
        LOG.debug("after Set data of {}: {}", ZNODE_PATH, val);
    }

}
