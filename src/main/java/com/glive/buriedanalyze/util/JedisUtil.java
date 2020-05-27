package com.glive.buriedanalyze.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class JedisUtil {
         private static JedisPool pool;
         private static String redisIp;
         private static Integer redisPort;
     private static String redispd;
     private static void initPool(){
          JedisPoolConfig config = new JedisPoolConfig();

          Properties sysProps = new Properties();
          InputStream is = JedisUtil.class.getResourceAsStream("/system.properties");
          try {
               sysProps.load(is);
          } catch (IOException e) {
          }
          config.setMaxTotal(Integer.parseInt(sysProps.getProperty("maxTotal")));
          config.setMaxIdle(Integer.parseInt(sysProps.getProperty("maxIdle")));
          config.setMinIdle(Integer.parseInt(sysProps.getProperty("minIdle")));
          config.setBlockWhenExhausted(true);//连接耗尽的时候，是否阻塞，false会抛出异常，true阻塞直到超时。默认为true。
          redisIp = sysProps.getProperty("redisIp");
          redispd = sysProps.getProperty("redispd");
          redisPort = (Integer.parseInt(sysProps.getProperty("redisPort")));
          if(redispd!=null){
               pool = new JedisPool(config,redisIp,redisPort,1000*2,redispd);
          }else{
               pool = new JedisPool(config,redisIp,redisPort,1000*2);
          }
     }

     static{
          initPool();
     }

     public static synchronized Jedis getJedis(){
          return pool.getResource();
     }

     public static JedisPool getPool(){
          return pool;
     }

}
