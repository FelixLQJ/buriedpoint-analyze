package com.glive.buriedanalyze.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.SQLException;
import java.util.Properties;

public class DruidPoolConnection {
    public static DruidDataSource druidDataSource = null;


    public static void init(){
        Properties properties = new Properties();
        try {
            properties.load(DruidPoolConnection.class.getResourceAsStream("/druid.properties"));
            druidDataSource = (DruidDataSource)DruidDataSourceFactory.createDataSource(properties); //DruidDataSrouce工厂模式
        } catch (Exception e) {
        }
    }
    static{
        init();
    }



    /**
     * 返回druid数据库连接
     * @return
     * @throws SQLException
     */
    public DruidPooledConnection getConnection() throws SQLException {
        return druidDataSource.getConnection();
    }

}
