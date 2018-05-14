package com.chenshun.storm.zk;

import org.apache.kafka.common.utils.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * User: mew <p />
 * Time: 18/5/14 15:37  <p />
 * Version: V1.0  <p />
 * Description:  <p />
 */
public class ZookeeperSession {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperSession.class);

    /** 存储所有 TaskId 的节点，TaskId 之间使用逗号隔开 */
    public static final String NODEDATA_PATH = "/taskid-list";

    private static final String DISTRIBUTE_LOCK_PATH = "/taskid-list-lock";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zookeeper;

    private ZookeeperSession() {
        try {
            // 去连接 zookeeper server，异步创建会话，因此需要一个监听器来侦测什么时候完成和 zk server 的连接
            zookeeper = new ZooKeeper("ci-server:2181", 60000, new ZookeeperWatcher());
            // 给一个 CONNECTING，连接中
            LOGGER.info(zookeeper.getState().toString());
            try {
                connectedSemaphore.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("ZooKeeper session established......");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取一个分布式锁
     */
    public void acquireDistributedLock() {
        try {
            zookeeper.create(DISTRIBUTE_LOCK_PATH, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.debug("success to acquire lock for " + DISTRIBUTE_LOCK_PATH);
        } catch (Exception e) {
            // 如果那个商品对应的锁 node，已经存在，就是已经被别人加锁，就会抛出异常
            int count = 0;
            while (true) {
                Utils.sleep(1000);
                try {
                    zookeeper.create(DISTRIBUTE_LOCK_PATH, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (Exception e1) {
                    count++;
                    LOGGER.debug("the {} times try to acquire lock for " + DISTRIBUTE_LOCK_PATH + "......", count);
                    continue;
                }
                LOGGER.debug("success to acquire lock for " + DISTRIBUTE_LOCK_PATH + " after {} times try......", count);
                break;
            }
        }
    }

    /**
     * 释放掉一个分布式锁
     */
    public void releaseDistributedLock() {
        try {
            zookeeper.delete(DISTRIBUTE_LOCK_PATH, -1);
            LOGGER.debug("release the lock for taskid-list-lock......");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getNodeData() {
        try {
            return new String(zookeeper.getData(NODEDATA_PATH, false, new Stat()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void setNodeData(String path, String data) {
        try {
            zookeeper.setData(path, data.getBytes(), -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean createNode(String path) {
        try {
            zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 建立 zk session 的 watcher
     */
    private class ZookeeperWatcher implements Watcher {

        public void process(WatchedEvent event) {
            LOGGER.debug("Receive watched event: {}", event.getState());
            if (KeeperState.SyncConnected == event.getState()) {
                connectedSemaphore.countDown();
            }
        }

    }

    private static class Singleton {

        private static ZookeeperSession instance;

        static {
            instance = new ZookeeperSession();
        }

        public static ZookeeperSession getInstance() {
            return instance;
        }

    }

    public static ZookeeperSession getInstance() {
        return Singleton.getInstance();
    }

    public static void init() {
        getInstance();
    }

}
