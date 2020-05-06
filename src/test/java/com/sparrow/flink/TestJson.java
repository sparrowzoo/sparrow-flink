package com.sparrow.flink;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJson {
    private static Logger logger = LoggerFactory.getLogger(TestJson.class);

    public static void main(String[] args) {
        try {
            String json = "615947,15041,28288,5891,117534,2035290,1266940,1337,5137,156465,113775,17728,64904,33004,7507,22801,553981,7517,3568,5961,926596,12389,1757871,658612,3576,7513,65153,45313,206241,8143,8210,531595,79487,500122,117531,153147,16208,11237,119004,187161,23197,1557849,2273010,198003,1880340,187269,14510,7497,2266629,11453";
            JSON.parseArray(json, Integer.class);
        } catch (Throwable e) {
            System.out.println("" + "-" + e.getMessage());
            logger.error("aaa", e);
        }
    }
}
