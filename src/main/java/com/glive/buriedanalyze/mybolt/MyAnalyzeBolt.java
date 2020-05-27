package com.glive.buriedanalyze.mybolt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.glive.buriedanalyze.Entity.IPEntity;
import com.glive.buriedanalyze.util.*;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;
import redis.clients.jedis.Jedis;
import sun.misc.BASE64Decoder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;

public class MyAnalyzeBolt extends BaseRichBolt {
    private Map<String, Object> map;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
    private static final Logger logger = Logger.getLogger(MyAnalyzeBolt.class);
    private static final String REDIS_SET_KEY_PRE = "glive_buried_set:";
    private static final String REDIS_SORT_SET_KEY_PRE = "glive_buried_sort_set:";

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Long start = System.currentTimeMillis();
        Map<String,String> map = new HashMap<>();
        Jedis jedis = JedisUtil.getJedis();
        try {
            String body = tuple.getStringByField("data");
            String[] bodySp = body.split("&");
            for (int i = 0; i < bodySp.length; i++) {
                String[] kv = bodySp[i].split("=");
                map.put(kv[0], kv[1]);
            }
            String gzip = map.get("gzip");
            String ip = map.get("ip");
            IPEntity ipEntity = new IPEntity();
            try {
                ipEntity = IPUtil.getIPMsg(ip);
            } catch (Exception e) {
                logger.info("本地局域网：" + ip);
            }
            String provinceName = ipEntity.getProvinceName();
            String cityName = ipEntity.getCityName();
            String datalist = map.get("data_list");
            String json = "";
            if ("1".equals(gzip)) {
                datalist = URLDecoder.decode(datalist, "UTF-8");
                json = Gzip.uncompress(datalist);
            } else if ("0".equals(gzip)) {
                String url = URLDecoder.decode(datalist, "UTF-8");
                json = new String(new BASE64Decoder().decodeBuffer(url), "UTF-8");
            }
            if(JSONArray.isValidArray(json)){
            JSONArray jsonArray = JSONArray.parseArray(json);
            String day = "";
            String hour = "";
            Long timedate =0L;
                List<String> updateKey = new ArrayList<>();
            for (int i = 0; i < jsonArray.size(); i++) {
                Map<String, String> event_map = new HashMap<>();
                JSONObject data = jsonArray.getJSONObject(i);
                String distinct_id = data.getString("distinct_id");
                Long time = data.getLong("time");
                String time_hour = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(time));
                String time_day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(time));
                timedate =time;
                day = time_day;
                hour = time_hour;
                JSONObject event_id = data.getJSONObject("properties");
                if (!StringUtils.isNotBlank(distinct_id)) {
                    distinct_id = event_id.getString("$device_id");
                }
                if (event_id != null) {
                    event_map.put("carrier", event_id.getString("$carrier"));
                    event_map.put("os_version", event_id.getString("$os_version"));
                    event_map.put("lib_version", event_id.getString("$lib_version"));
                    event_map.put("model", event_id.getString("$model"));
                    event_map.put("os", event_id.getString("$os"));
                    event_map.put("screen_width", event_id.getString("$screen_width"));
                    event_map.put("screen_height", event_id.getString("$screen_height"));
                    event_map.put("manufacturer", event_id.getString("$manufacturer"));
                    event_map.put("app_version", event_id.getString("$app_version"));
                    event_map.put("wifi", event_id.getString("$wifi"));
                    event_map.put("network_type", event_id.getString("$network_type"));
                    event_map.put("province", provinceName);
                    event_map.put("city", cityName);
                    Set<String> keys = event_map.keySet();
                    Long start_redis = System.currentTimeMillis();
                    for (String key : keys) {
                        String value = event_map.get(key);
                        String redis_set_mem = distinct_id + "_" + key + "_" + value;
                        String set_mem_value = key + ":" + value;
                        //处理小时统计
                        if (!jedis.sismember(REDIS_SET_KEY_PRE + time_hour, redis_set_mem)) {
                            jedis.zincrby(REDIS_SORT_SET_KEY_PRE + time_hour, 1, set_mem_value);
                            jedis.sadd(REDIS_SET_KEY_PRE + time_hour, redis_set_mem);
                        }
                        //处理天统计
                        if (!jedis.sismember(REDIS_SET_KEY_PRE + time_day, redis_set_mem)) {
                            jedis.zincrby(REDIS_SORT_SET_KEY_PRE + time_day, 1, set_mem_value);
                            jedis.sadd(REDIS_SET_KEY_PRE + time_day, redis_set_mem);
                        }
                        updateKey.add(set_mem_value);
                    }
                    Long end_redis = System.currentTimeMillis();
                    logger.info("redis："+(end_redis-start_redis)+"ms");
                    jedis.expire(REDIS_SET_KEY_PRE + time_hour, 3600 * 2);
                    jedis.expire(REDIS_SORT_SET_KEY_PRE + time_hour, 3600 * 2);
                    jedis.expire(REDIS_SET_KEY_PRE + time_day, 60 * 60 * 24 + (3600 * 2));
                    jedis.expire(REDIS_SORT_SET_KEY_PRE + time_day, 60 * 60 * 24 + (3600 * 2));

                }
            }
                this.outputCollector.emit(new Values(updateKey,day,hour));

                String nowSt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timedate));
                Long end = System.currentTimeMillis();
                logger.info("总："+(end-start)+"ms"+"  time:"+nowSt);
        }

            jedis.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            if(jedis!=null){
                jedis.close();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("updateKeys","day","hour"));
    }
}
