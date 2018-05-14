package com.chenshun.storm.spout;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * User: mew <p />
 * Time: 18/5/14 11:53  <p />
 * Version: V1.0  <p />
 * Description:  <p />
 */
public class AccessLogKafkaSpout extends BaseRichSpout {

    private static final long serialVersionUID = 6756401018373995504L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogKafkaSpout.class);

    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

    private SpoutOutputCollector collector;

    /**
     * open 方法，是堆 spout 尽心初始化，比如创建一个线程池或者创建一个数据库连接池，或者构造一个 httpclient
     *
     * @param conf
     * @param context
     * @param collector
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        new Thread(new KafkaMessageProcessor()).start();
    }

    private class KafkaMessageProcessor implements Runnable {

        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ci-server:9092,ci-server:9093,ci-server:9094");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "eshop-cache-group");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            List<String> topics = new ArrayList<String>(1);
            topics.add("access-log");
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.debug("【AccessLogKafkaSpout中的Kafka消费者接收到一条日志】message={}", record.value());
                    if (record.value() != null) {
                        try {
                            queue.put(record.value());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

    }

    /**
     * spout 会运行在 task 中，某个 workr 进程的某个 executor 线程内部的某个 task 中，那个
     * task 会负责去不断的无限循环调用 nextTuple() 方法，只要无限循环就可以不断发射最新的数据出去，形成一个数据流
     */
    public void nextTuple() {
        if (queue.size() > 0) {
            try {
                String message = queue.take();
                // 这个 Values 可以认为是构建的 tuple
                // tuple 是最小的数据单元，无限个 tuple 组成的流就是一个 stream
                collector.emit(new Values(message));
                LOGGER.debug("【AccessLogKafkaSpout发射出去一条日志】message={}", message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            Utils.sleep(100);
        }
    }


    /**
     * 定义发射出去的每个 tuple 中 field 的名称
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

}
