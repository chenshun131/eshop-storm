package com.chenshun.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.chenshun.storm.zk.ZookeeperSession;
import org.apache.storm.shade.org.apache.http.util.TextUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: mew <p />
 * Time: 18/5/14 13:57  <p />
 * Version: V1.0  <p />
 * Description: 商品访问次数统计 bolt <p />
 */
public class ProductCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1763627557492537016L;

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductCountBolt.class);

    /** LRUMap => productId-count */
    private LRUMap<Long, Long> productCountMap = new LRUMap<Long, Long>(1000);

    private ZookeeperSession zkSession;

    private int taskId;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.zkSession = ZookeeperSession.getInstance();
        this.taskId = context.getThisTaskId();
        new Thread(new ProductCountThread()).start();

        // 1、将自己的taskid写入一个zookeeper node中，形成taskid的列表
        // 2、然后每次都将自己的热门商品列表，写入自己的taskid对应的zookeeper节点
        // 3、然后这样的话，并行的预热程序才能从第一步中知道，有哪些taskid
        // 4、然后并行预热程序根据每个taskid去获取一个锁，然后再从对应的znode中拿到热门商品列表
        initTaskId(this.taskId);
    }

    private void initTaskId(int taskId) {
        // ProductCountBolt 所有的 task启动的时候，都会将自己的 taskid 写到同一个 node的值中
        // 格式就是逗号分隔，拼接成一个列表
        // 111,211,355
        zkSession.acquireDistributedLock();
        String taskIdList = zkSession.getNodeData();
        LOGGER.info("【ProductCountBolt获取到taskid list】taskIdList={}", taskIdList);
        if (TextUtils.isEmpty(taskIdList)) {
            taskIdList = Integer.toString(taskId);
        } else {
            taskIdList += "," + taskId;
        }
        zkSession.setNodeData(ZookeeperSession.NODEDATA_PATH, taskIdList);
        LOGGER.info("【ProductCountBolt设置taskid list】taskIdList={}", taskIdList);
        zkSession.releaseDistributedLock();
    }

    private class ProductCountThread implements Runnable {

        public void run() {
            // 临时 Topn 列表
            List<Map.Entry<Long, Long>> topnProductList = new ArrayList<Map.Entry<Long, Long>>();
            // 产品 id 列表
            List<Long> productIdList = new ArrayList<Long>();
            while (true) {
                topnProductList.clear();
                productIdList.clear();

                // 指定获取 Top 的数量，将会在 topnProductList 按索引顺序从大到小排序的 Topn
                int topn = 3;

                if (productCountMap.size() == 0) {
                    Utils.sleep(100);
                    continue;
                }
                LOGGER.info("【ProductCountThread打印productCountMap的长度】size={}", productCountMap.size());

                for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                    if (topnProductList.size() == 0) {
                        topnProductList.add(productCountEntry);
                    } else {
                        boolean lesser = true;
                        for (int i = 0; i < topnProductList.size(); i++) {
                            Map.Entry<Long, Long> topnProductCountEntry = topnProductList.get(i);
                            if (productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                int lastIndex = topnProductList.size() < topn ? topnProductList.size() - 1 : topn - 2;
                                for (int j = lastIndex; j >= i; j--) {
                                    if (j + 1 == topnProductList.size()) {
                                        // 扩容 临时 Topn 列表
                                        topnProductList.add(null);
                                    }
                                    topnProductList.set(j + 1, topnProductList.get(j));
                                }
                                topnProductList.set(i, productCountEntry);
                                lesser = false;
                                break;
                            }
                        }
                        if (lesser && topnProductList.size() < topn) {
                            topnProductList.add(productCountEntry);
                        }
                    }
                }
                // 获取到一个 topn list
                for (Map.Entry<Long, Long> topnProductEntry : topnProductList) {
                    productIdList.add(topnProductEntry.getKey());
                }
                String topnProductListJSON = JSONArray.toJSONString(productIdList);
                zkSession.createNode("/task-hot-product-list-" + taskId);
                zkSession.setNodeData("/task-hot-product-list-" + taskId, topnProductListJSON);
                LOGGER.info("【ProductCountThread计算出一份top3热门商品列表】zk path=/task-hot-product-list-{}, topnProductListJSON={}", taskId,
                        topnProductListJSON);
                Utils.sleep(60000);
            }
        }

    }

    public void execute(Tuple input) {
        Long productId = input.getLongByField("productId");
        LOGGER.info("【ProductCountBolt接收到一个商品id】 productId={}", productId);
        Long count = productCountMap.get(productId);
        if (count == null) {
            count = 0L;
        }
        count++;
        productCountMap.put(productId, count);
        LOGGER.info("【ProductCountBolt完成商品访问次数统计】productId={}, count={}", productId, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
