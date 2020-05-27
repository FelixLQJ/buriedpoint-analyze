package com.glive.buriedanalyze.myspout;

import com.glive.buriedanalyze.mybolt.MyTaskBolt;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class MyKafkaSpout extends BaseRichSpout {
    private KafkaConsumer<String, String> consumer = null;
    private SpoutOutputCollector collector;
    private TopologyContext topologyContext;
    private static final Logger logger = Logger.getLogger(MyKafkaSpout.class);
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.topologyContext = topologyContext;
        Properties sysProps = new Properties();
        InputStream is = this.getClass().getResourceAsStream("/system.properties");
        try {
            sysProps.load(is);
        } catch (IOException e) {
        }
        Properties props = new Properties();
        props.put("bootstrap.servers",sysProps.getProperty("bootstrap.servers"));
        props.put("group.id", sysProps.getProperty("group.id"));
        props.put("auto.offset.reset",sysProps.getProperty("auto.offset.reset"));
        props.put("max.poll.records",5000);
        props.put("enable.auto.commit",sysProps.getProperty("enable.auto.commit"));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(sysProps.getProperty("topic").split(",")));
    }

    public void nextTuple() {
        try {
            // 1000 是超时时间（ms），在该时间内 poll 会等待服务器返回数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    collector.emit(new Values(record.value()));
                }
                consumer.commitSync();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }
}
