package com.glive.buriedanalyze.mybolt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.glive.buriedanalyze.Entity.IPEntity;
import com.glive.buriedanalyze.util.*;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import sun.misc.BASE64Decoder;

import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MyTaskBolt  extends BaseRichBolt {
    private Map<String, Object> map;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
    private static final Logger logger = Logger.getLogger(MyTaskBolt.class);
    private static final String REDIS_SET_KEY_PRE = "glive_buried_set:";
    private static final String REDIS_SORT_SET_KEY_PRE = "glive_buried_sort_set:";
    private static MongoUtil mongoUtil = new MongoUtil();

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Long start = System.currentTimeMillis();
        Jedis jedis = null;
        String cursorDay = "0";
        String cursorHour = "0";
        Connection conn = null;
        PreparedStatement prepareStatement = null;
        try {
            conn = DruidPoolConnection.druidDataSource.getConnection();
            jedis = JedisUtil.getJedis();
            List<String> updateKeys = (ArrayList)tuple.getValueByField("updateKeys");
            String day = tuple.getStringByField("day");
            String hour = tuple.getStringByField("hour");

            Map<String,Long> dayMap = new HashMap<>();
            Map<String,Long> hourMap = new HashMap<>();

            for(String updateKey:updateKeys){
               Long dayScore = jedis.zscore(REDIS_SORT_SET_KEY_PRE+day,updateKey).longValue();
                Long hourScore = jedis.zscore(REDIS_SORT_SET_KEY_PRE+hour,updateKey).longValue();
                dayMap.put(updateKey,dayScore);
                dayMap.put(updateKey,hourScore);
            }


//            Long start_day = System.currentTimeMillis();
//            while(true) {
//                ScanResult<redis.clients.jedis.Tuple> result = jedis.zscan(REDIS_SORT_SET_KEY_PRE + day, cursorDay, new ScanParams().count(50));
//                cursorDay = result.getCursor();
//                dayList.addAll(result.getResult());
//                if ("0".equals(cursorDay)) {
//                    break; //遍历完成跳出循环
//                }
//            }
//            Long end_day = System.currentTimeMillis();
//            logger.info("redis_day:"+(end_day-start_day)+"ms");
//
//            Long start_hour = System.currentTimeMillis();
//            while(true) {
//                ScanResult<redis.clients.jedis.Tuple> result = jedis.zscan(REDIS_SORT_SET_KEY_PRE + hour, cursorHour, new ScanParams().count(50));
//                cursorHour = result.getCursor();
//                hourList.addAll(result.getResult());
//                if ("0".equals(cursorHour)) {
//                    break; //遍历完成跳出循环
//                }
//            }
//            Long end_hour = System.currentTimeMillis();
//            logger.info("redis_hour:"+(end_hour-start_hour)+"ms");


           Set<String> dayKeys = dayMap.keySet();
            Set<String> hourKeys = hourMap.keySet();
            //处理天数据
            conn.setAutoCommit(false);
            int times = 0;
            StringBuilder sql = new StringBuilder("INSERT INTO gcard_temp_event_day_statistical VALUES(?,?,?,?)")
                    .append(" ON DUPLICATE KEY UPDATE")
                    .append(" count=?");

            prepareStatement =  conn.prepareStatement(sql.toString());
            for(String key :dayKeys){
               String[] keys = key.split(":");
               Long score = dayMap.get(key);

                prepareStatement.setString(1,day);
                prepareStatement.setString(2,keys[0]);
                prepareStatement.setString(3,keys[1]);
                prepareStatement.setLong(4,score);
                prepareStatement.setLong(5,score);
                prepareStatement.addBatch();
                times++;
                if(times%1000==0){
                    prepareStatement.executeBatch();
                    conn.commit();
                }
            }
            prepareStatement.executeBatch();
            conn.commit();
            //处理天数据
            times=0;
            sql = new StringBuilder("INSERT INTO gcard_temp_event_hour_statistical VALUES(?,?,?,?)")
                    .append(" ON DUPLICATE KEY UPDATE")
                    .append(" count=?");

            prepareStatement =  conn.prepareStatement(sql.toString());
            for(String key :hourKeys){
                String[] keys = key.split(":");
                Long score = dayMap.get(key);

                prepareStatement.setString(1,hour);
                prepareStatement.setString(2,keys[0]);
                prepareStatement.setString(3,keys[1]);
                prepareStatement.setLong(4,score);
                prepareStatement.setLong(5,score);
                prepareStatement.addBatch();
                times++;
                if(times%1000==0){
                    prepareStatement.executeBatch();
                    conn.commit();
                }
            }
            prepareStatement.executeBatch();
            conn.commit();

            prepareStatement.close();
            conn.close();
            jedis.close();
        }catch(Exception e){
            try {
                if(!conn.isClosed()){
                    conn.rollback();//4,当异常发生执行catch中SQLException时，记得要rollback(回滚)；
                                       System.out.println("插入失败，回滚！");
                    conn.setAutoCommit(true);
                                   }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            if(jedis!=null){
                jedis.close();
            }
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if(prepareStatement!=null){
                try {
                    prepareStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
