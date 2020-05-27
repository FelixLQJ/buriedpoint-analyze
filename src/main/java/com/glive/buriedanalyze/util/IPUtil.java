package com.glive.buriedanalyze.util;

import com.glive.buriedanalyze.Entity.IPEntity;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import org.springframework.core.io.ClassPathResource;

import java.io.*;
import java.net.InetAddress;

public class IPUtil {
    private static DatabaseReader reader;

    /**
     * 静态代码块，保证项目启动只获取一次文件
     */
    static {

        File database = null;

        try {
            //绝对路径读取文件方式
            //database = new File("F:\\IDEA project\\my_demo\\src\\main\\resources\\GeoLite2-City.mmdb");

            // 通过 InputStream 流式读取文件，目的解决无法通过File方式读取jar包内的文件的问题·1
            database = getFile("/GeoLite2-City.mmdb","geolite2.mmdb");
            reader = new DatabaseReader.Builder(database).build();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 解析IP
     * @param ip
     * @return
     */
    public static IPEntity getIPMsg(String ip){

        IPEntity msg = new IPEntity();

        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            CityResponse response = reader.city(ipAddress);
            Country country = response.getCountry();
            Subdivision subdivision = response.getMostSpecificSubdivision();
            City city = response.getCity();
            Postal postal = response.getPostal();
            Location location = response.getLocation();

            msg.setCountryName(country.getNames().get("zh-CN"));
            msg.setCountryCode(country.getIsoCode());
            msg.setProvinceName(subdivision.getNames().get("zh-CN"));
            msg.setProvinceCode(subdivision.getIsoCode());
            msg.setCityName(city.getNames().get("zh-CN"));
            msg.setPostalCode(postal.getCode());
            //经度
            msg.setLongitude(location.getLongitude());
            //纬度
            msg.setLatitude(location.getLatitude());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (GeoIp2Exception e) {
            e.printStackTrace();
        }

        return msg;
    }


    /**
     * 读取classpath下的文件
     * @param fileName 原文件全名
     * @param newFileName  缓存的新文件的名称
     * @return
     * @throws IOException
     */
    public static File getFile(String fileName, String newFileName) throws IOException {
        //读取 ClassPath 路径下指定资源的输入流

//        InputStream inputStream = new FileInputStream(new File(fileName));
        InputStream inputStream = IPUtil.class.getResourceAsStream(fileName);
        File file = new File(newFileName);

        inputstreamToFile(inputStream, file);

        return file;
    }

    /**
     * InputStream -> File
     * @param inputStream
     * @param file
     */
    private static void inputstreamToFile(InputStream inputStream, File file) {
        try {
            OutputStream os = new FileOutputStream(file);
            int bytesRead = 0;
            byte[] buffer = new byte[8192];
            while ((bytesRead = inputStream.read(buffer, 0, 8192)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            os.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
