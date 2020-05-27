package com.glive.buriedanalyze.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;

import java.util.Properties;

public class MongoUtil {
    private static MongoClient mongoClient = null;

//   static Properties properties = new Properties();
//    static{
//        try {
//            properties.load(MongoUtil.class.getResourceAsStream("/mongo.properties"));
//
//        MongoClientOptions.Builder build = new MongoClientOptions.Builder();
//
//        MongoClientOptions myOptions = build.build();
//            mongoClient = new MongoClient(new ServerAddress(properties.getProperty("ip"), Integer.parseInt(properties.getProperty("port"))), myOptions);
//        } catch (Exception e)
//        {
//            e.printStackTrace();
//        }
//    }
//
//    public MongoCollection getConnection(String collectionName){
//        return mongoClient.getDatabase(properties.getProperty("def-db")).getCollection(collectionName);
//    }

}
