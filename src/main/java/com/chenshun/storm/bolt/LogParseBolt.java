package com.chenshun.storm.bolt;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * User: mew <p />
 * Time: 18/5/14 13:49  <p />
 * Version: V1.0  <p />
 * Description: 日志解析的 bolt，获取 productId <p />
 */
public class LogParseBolt extends BaseRichBolt {

    private static final long serialVersionUID = -8013371823786778530L;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogParseBolt.class);

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String message = input.getStringByField("message");
        LOGGER.info("【LogParseBolt接收到一条日志】message={}", message);

        JSONObject messageJSON = JSONObject.parseObject(message);
        JSONObject uriArgsJSON = messageJSON.getJSONObject("uri_args");
        Long productId = uriArgsJSON.getLong("productId");
        if (productId != null) {
            collector.emit(new Values(productId));
            LOGGER.info("【LogParseBolt发射出去一个商品id】productId={}", productId);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productId"));
    }

}
