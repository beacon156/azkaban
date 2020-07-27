package org.poem.exec;

import com.alibaba.fastjson.JSONObject;
import org.poem.config.DatabaseContainer;
import org.poem.vo.ExecTaskDetailPlanVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;


public class DataClear {

    private static final Logger logger = LoggerFactory.getLogger(DataClear.class);

    /**
     * 清除数据
     *
     * @param args
     */
    public static void main(String[] args) {
        String par = args[0];
        ExecTaskDetailPlanVO execTaskDetailPlanVO = JSONObject.parseObject(par, ExecTaskDetailPlanVO.class);
        try {
            importData(execTaskDetailPlanVO);
        } catch (SQLException throwables) {
            logger.error(throwables.getMessage(), throwables);
            throwables.printStackTrace();
        }
    }

    /**
     * 开始导数据了
     *
     * @param dataTransformVO
     */
    public static void importData(ExecTaskDetailPlanVO dataTransformVO) throws SQLException {
        JdbcTemplate sourceJdbc = DatabaseContainer.getSourceJdbc(dataTransformVO);
        JdbcTemplate targetJdbc = DatabaseContainer.getTargetJdbc(dataTransformVO);
        clear(targetJdbc, dataTransformVO);
    }

    /**
     * 前置
     *
     * @param targetJdbc      目标库
     * @param dataTransformVO 结果
     * @return
     */
    public static void clear(JdbcTemplate targetJdbc, ExecTaskDetailPlanVO dataTransformVO) {
        String aroundSql = dataTransformVO.getAroundSql();
        targetJdbc.execute(aroundSql);
    }
}
