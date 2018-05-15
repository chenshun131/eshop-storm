package com.chenshun.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.chenshun.storm.http.HttpClientUtils;
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
        new Thread(new HotProductFindThread()).start();

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
        zkSession.createNode(ZookeeperSession.NODEDATA_PATH);
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

    private class HotProductFindThread implements Runnable {

        public void run() {
            List<Map.Entry<Long, Long>> productCountList = new ArrayList<Map.Entry<Long, Long>>();
            // 保存所有热点数据的 List
            List<Long> hotProductIdList = new ArrayList<Long>();
            while (true) {
                // 1、将 LRUMap 中的数据按照访问次数，进行全局的排序
                // 2、计算 95% 的商品的访问次数的平均值
                // 3、遍历排序后的商品访问次数，从最大的开始
                // 4、如果某个商品比如它的访问量是平均值的10倍，就认为是缓存的热点
                try {
                    productCountList.clear();
                    hotProductIdList.clear();
                    if (productCountMap.size() == 0) {
                        Utils.sleep(100);
                        continue;
                    }
                    LOGGER.debug("【HotProductFindThread打印productCountMap的长度】size={}", productCountMap.size());
                    // 1、先做全局排序
                    for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                        if (productCountList.size() == 0) {
                            productCountList.add(productCountEntry);
                        } else {
                            boolean lesser = true;
                            for (int i = 0; i < productCountList.size(); i++) {
                                Map.Entry<Long, Long> topnProductCountEntry = productCountList.get(i);
                                if (productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                    int lastIndex = productCountList.size() < productCountMap.size() ? productCountList.size() - 1 :
                                            productCountMap.size() - 2;
                                    for (int j = lastIndex; j >= i; j--) {
                                        if (j + 1 == productCountList.size()) {
                                            productCountList.add(null);
                                        }
                                        productCountList.set(j + 1, productCountList.get(j));
                                    }
                                    productCountList.set(i, productCountEntry);
                                    lesser = false;
                                    break;
                                }
                            }
                            if (lesser && productCountList.size() < productCountMap.size()) {
                                productCountList.add(productCountEntry);
                            }
                        }
                    }
                    // 2、计算出 95% 商品访问次数的平均值
                    int calculateCount = (int) Math.floor(productCountList.size() * 0.95);
                    Long totalCount = 0L;
                    for (int i = productCountList.size() - 1; i >= productCountList.size() - calculateCount; i--) {
                        totalCount += productCountList.get(i).getValue();
                    }
                    Long avgCount = totalCount / calculateCount;
                    // 3、从第一个元素开始遍历，判断是否是平均值的 10倍
                    for (Map.Entry<Long, Long> productCountEntry : productCountList) {
                        if (productCountEntry.getValue() > 10 * avgCount) {
                            hotProductIdList.add(productCountEntry.getKey());

                            // 将缓存热点反向推送到流量分发的 nginx 中
                            String distributeNginxURL = "http://192.168.31.179:8080/getProductInfo?productId=" + productCountEntry.getKey();
                            HttpClientUtils.sendGetRequest(distributeNginxURL);

                            // 将缓存热点，那个商品对应的完整的缓存数据，发送请求到缓存服务去获取，反向推送到所有的后端应用nginx服务器上去
                            String cacheServiceURL = "http://192.168.31.179:8080/getProductInfo?productId=" + productCountEntry.getKey();
                            String response = HttpClientUtils.sendGetRequest(cacheServiceURL);
                            String[] appNginxURLs = new String[]{
                                    "http://192.168.31.187/hot?productId=" + productCountEntry.getKey() + "&productInfo=" + response,
                                    "http://192.168.31.19/hot?productId=" + productCountEntry.getKey() + "&productInfo=" + response
                            };
                            for (String appNginxURL : appNginxURLs) {
                                HttpClientUtils.sendGetRequest(appNginxURL);
                            }
                        }
                    }

                    Utils.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

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

                // 指定获取 Top 的数量，将会在 topnProductList 按索引顺序从大到小排序的 Topn，只能获取指定数量个数的数据
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
