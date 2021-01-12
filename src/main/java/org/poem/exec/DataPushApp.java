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
//        String par = "{\"aroundSql\":\"insert into wedding_view_bigdata (id, view_count,view_people_count,view_date,attr) values('1', '@count','@user_id', '@time' ,null)\",\"beforeSql\":\"SELECT\\n\\t`code`,\\n\\tDATE_FORMAT(time, \\\"%Y-%m-%d\\\") AS `time`,\\n\\tcount(1) AS count,\\n\\tCOUNT(DISTINCT user_id) as user_id\\nFROM\\n\\tt_event_tracking_frequency_info_origin\\nwhere 1=1\\nAND code = \\\"10001\\\"\\nand time >  CONCAT(DATE_FORMAT(NOW(), \\\"%Y-%m-%d\\\"),\\\" 00:00:00\\\")\\nand time <= CONCAT(DATE_FORMAT(NOW(), \\\"%Y-%m-%d\\\"),\\\" 23:59:59\\\")\\ngroup by `code`,DATE_FORMAT(time, \\\"%Y-%m-%d\\\")   \\norder by DATE_FORMAT(time, \\\"%Y-%m-%d\\\") desc\",\"sourceIp\":\"39.105.196.142\",\"sourcePasswd\":\"zgdc\",\"sourcePort\":43306,\"sourceSchema\":\"zgdc_warehouse_source\",\"sourceSourceType\":\"1\",\"sourceUserName\":\"root\",\"targetIp\":\"192.168.51.152\",\"targetPasswd\":\"123456\",\"targetPort\":3306,\"targetSchema\":\"xxl_job\",\"targetSourceType\":\"1\",\"targetUserName\":\"root\"}";
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
        insertInto(pre(sourceJdbc, dataTransformVO), targetJdbc, dataTransformVO);
    }


    /**
     * 前置
     *
     * @param sourceJdbc      目标库
     * @param dataTransformVO 结果
     * @return
     */
    private static List<Map<String, Object>> pre(JdbcTemplate sourceJdbc, ExecTaskDetailPlanVO dataTransformVO) {
        String aroundSql = dataTransformVO.getBeforeSql();
        return sourceJdbc.queryForList(aroundSql);
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
