package com.glive.buriedanalyze;



import com.glive.buriedanalyze.mybolt.MyAnalyzeBolt;
import com.glive.buriedanalyze.mybolt.MyTaskBolt;
import com.glive.buriedanalyze.myspout.MyKafkaSpout;
import com.glive.buriedanalyze.util.DruidPoolConnection;
import com.glive.buriedanalyze.util.IPUtil;
import com.glive.buriedanalyze.util.JedisUtil;
import com.glive.buriedanalyze.util.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class buriedanalyzeTopology {

    private static MongoUtil mongo = new MongoUtil();
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        conf.setMessageTimeoutSecs(conf,700);
        builder.setSpout("KafkaSpout",new MyKafkaSpout(),2);
        builder.setBolt("liveCount",new MyAnalyzeBolt(),2).shuffleGrouping("KafkaSpout");
        builder.setBolt("liveTask",new MyTaskBolt(),10).shuffleGrouping("liveCount");
//////
//////
        StormSubmitter.submitTopology("my-live-buried-analyze",conf,builder.createTopology());
//        conf.setMaxTaskParallelism(1);
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("simple", conf, builder.createTopology());



//        // 1000 是超时时间（ms），在该时间内 poll 会等待服务器返回数据
//        Properties sysProps = new Properties();
//        InputStream is = buriedanalyzeTopology.class.getResourceAsStream("/system.properties");
//        try {
//            sysProps.load(is);
//        } catch (IOException e) {
//        }
//        KafkaConsumer<String, String> consumer = null;
//        Properties props = new Properties();
//        props.put("bootstrap.servers","192.168.244.131:9092");
//        props.put("group.id", sysProps.getProperty("group.id"));
//        props.put("auto.offset.reset",sysProps.getProperty("auto.offset.reset"));
//        props.put("max.poll.records",1000);
//        props.put("fetch.max.wait.ms",5000);
//        props.put("max.partition.fetch.byte",5000);
//        props.put("enable.auto.commit",sysProps.getProperty("enable.auto.commit"));
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumer = new KafkaConsumer<String, String>(props);
//        consumer.subscribe(Arrays.asList(new String[]{"glive_server"}));
//        while(true) {
//            Connection conn = null;
//            try {
//                 conn = DruidPoolConnection.druidDataSource.getConnection();
////            Connection conn1 = DruidPoolConnection.druidDataSource.getConnection();
////            Connection conn2 = DruidPoolConnection.druidDataSource.getConnection();
//                conn.prepareStatement("select count(1) from gcard_temp_event_hour_statistical").executeQuery();
//                System.out.println(DruidPoolConnection.druidDataSource.getActiveCount());
////            conn.close();
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//                if (!records.isEmpty()) {
//                    System.out.println(records.count());
//                    for (ConsumerRecord<String, String> record : records) {
//                        System.out.println(record.value());
//                    }
//                    consumer.commitSync();
//                }
//            }catch (Exception e){
//                e.printStackTrace();
//            }finally {
//                if(conn!=null){
////                    conn.close();
//                }
//            }
//        }




    }
}
