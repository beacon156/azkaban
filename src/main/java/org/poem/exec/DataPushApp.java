package org.poem.exec;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.poem.config.DatabaseContainer;
import org.poem.vo.ExecTaskDetailPlanVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author sangfor
 */
public class DataPushApp {


    private static final Logger logger = LoggerFactory.getLogger(DataPushApp.class);

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
        JdbcTemplate targetJdbc = DatabaseContainer.getSourceJdbc(dataTransformVO);
        insertInto(pre(targetJdbc, dataTransformVO), sourceJdbc, dataTransformVO);
    }


    /**
     * 前置
     *
     * @param targetJdbc      目标库
     * @param dataTransformVO 结果
     * @return
     */
    private static List<Map<String, Object>> pre(JdbcTemplate targetJdbc, ExecTaskDetailPlanVO dataTransformVO) {
        String aroundSql = dataTransformVO.getAroundSql();
        return targetJdbc.queryForList(aroundSql);
    }


    /**
     * 写入数据
     *
     * @param sourceJdbc
     */
    private static void insertInto(List<Map<String, Object>> data, JdbcTemplate sourceJdbc, ExecTaskDetailPlanVO dataTransformVO) {
        String aroundSql = dataTransformVO.getAroundSql();
        List<String> manySQl = Lists.newArrayList();
        for (Map<String, Object> datum : data) {
            String sql = String.copyValueOf(aroundSql.toCharArray());
            for (String s : datum.keySet()) {
                sql = sql.replaceAll("@" + s, datum.get(s).toString());
            }
            manySQl.add(sql);
        }
        for (String insertSql : manySQl) {
            if (!StringUtils.isEmpty(insertSql.trim())) {
                sourceJdbc.update(insertSql);
            }
        }
    }
}
