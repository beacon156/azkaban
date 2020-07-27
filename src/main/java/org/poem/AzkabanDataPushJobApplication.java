package org.poem;

import com.alibaba.fastjson.JSONObject;
import org.poem.exec.DataClear;
import org.poem.exec.DataPushApp;
import org.poem.vo.ExecTaskDetailPlanVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.SQLException;

import static org.poem.exec.DataClear.importData;

@SpringBootApplication
public class AzkabanDataPushJobApplication {

    private static final Logger logger = LoggerFactory.getLogger(AzkabanDataPushJobApplication.class);

    public static void main(String[] args) {
        for (String arg : args) {
            logger.info("pars:\n {}", arg);
        }

        ExecTaskDetailPlanVO execTaskDetailPlanVO = JSONObject.parseObject(args[0], ExecTaskDetailPlanVO.class);
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
