package org.poem;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AzkabanDataPushJobApplication {

    private static final Logger logger = LoggerFactory.getLogger(AzkabanDataPushJobApplication.class);

    public static void main(String[] args) {
        logger.info("pars:\n {}", JSONObject.toJSONString(args));
        SpringApplication.run(AzkabanDataPushJobApplication.class, args);
    }

}
