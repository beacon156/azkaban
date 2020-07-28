package org.poem;

import com.alibaba.fastjson.JSONObject;
import org.poem.exec.DataPushApp;
import org.poem.vo.ExecTaskDetailPlanVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;

import static org.poem.exec.DataClear.importData;

@SpringBootApplication
public class AzkabanDataPushJobApplication {

    private static final Logger logger = LoggerFactory.getLogger(AzkabanDataPushJobApplication.class);

    public static void main(String[] args) {
        for (String arg : args) {
            logger.info("pars:\n {}", arg);
        }

        String decode = "{}";
        try {
            decode = URLDecoder.decode(args[0], "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        logger.info("decode:\n {}", decode);
        ExecTaskDetailPlanVO execTaskDetailPlanVO = JSONObject.parseObject(decode, ExecTaskDetailPlanVO.class);
        if ("clear".equals(args[1])) {
            try {
                importData(execTaskDetailPlanVO);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        } else {
            try {
                DataPushApp.importData(execTaskDetailPlanVO);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }
    }

}
