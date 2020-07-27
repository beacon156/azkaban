package org.poem.config;

import com.zaxxer.hikari.HikariDataSource;
import org.poem.util.DataSourceDriverHelper;
import org.poem.util.DataSourceUrlHelper;
import org.poem.vo.ExecTaskDetailPlanVO;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author sangfor
 */
public class DatabaseContainer {

    /**
     * 任务执行
     */
    private static final ConcurrentHashMap<String, JdbcTemplate> JDBC_TEMPLATECON_CURRENT_HASHMAP = new ConcurrentHashMap<>();

    /**
     * 初始化链接
     *
     * @param url
     * @param password
     * @param userName
     * @return
     */
    private static JdbcTemplate initJdbcTemplate(String url, String password, String userName) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(DataSourceDriverHelper.getJdbc(url));
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(userName);
        dataSource.setPassword(password);
        dataSource.addDataSourceProperty("prepStmtCacheSize", "500");
        dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "5000");
        dataSource.addDataSourceProperty("sendStringParametersAsUnicode", "false");

        dataSource.addDataSourceProperty("cachePrepStmts", "true");
        dataSource.addDataSourceProperty("useServerPrepStmts", "true");
        dataSource.addDataSourceProperty("useLocalSessionState", "true");
        dataSource.addDataSourceProperty("useLocalTransactionState", "true");
        dataSource.addDataSourceProperty("rewriteBatchedStatements", "true");
        dataSource.addDataSourceProperty("cacheResultSetMetadata", "true");
        dataSource.addDataSourceProperty("cacheServerConfiguration", "true");
        dataSource.addDataSourceProperty("elideSetAutoCommits", "true");
        dataSource.addDataSourceProperty("maintainTimeStats", "false");
        dataSource.setConnectionTestQuery("select 1");
        int core = Runtime.getRuntime().availableProcessors();
        dataSource.setMaximumPoolSize(2 * core);
        return new JdbcTemplate(dataSource);
    }

    /**
     * 数据目标
     *
     * @param dataTransformVO
     * @return
     */
    public static JdbcTemplate getTargetJdbc(ExecTaskDetailPlanVO dataTransformVO) {
        String url = DataSourceUrlHelper.getDatabaseUrl(dataTransformVO.getTargetIp(),
                dataTransformVO.getTargetPort(), dataTransformVO.getTargetSchema(),
                dataTransformVO.getTargetSourceType());
        JdbcTemplate jdbcTemplate = JDBC_TEMPLATECON_CURRENT_HASHMAP.get(url);
        if (jdbcTemplate == null) {
            jdbcTemplate = initJdbcTemplate(url, dataTransformVO.getTargetPasswd(), dataTransformVO.getTargetUserName());
            JDBC_TEMPLATECON_CURRENT_HASHMAP.put(url, jdbcTemplate);
        }
        return jdbcTemplate;
    }


    /**
     * 来源的数据源类型
     *
     * @param dataTransformVO
     * @return
     */
    public static JdbcTemplate getSourceJdbc(ExecTaskDetailPlanVO dataTransformVO) {
        String url = DataSourceUrlHelper.getDatabaseUrl(dataTransformVO.getSourceIp(), dataTransformVO.getSourcePort(),
                dataTransformVO.getSourceSchema(), dataTransformVO.getSourceSourceType());
        JdbcTemplate jdbcTemplate = JDBC_TEMPLATECON_CURRENT_HASHMAP.get(url);
        if (jdbcTemplate == null) {
            jdbcTemplate = initJdbcTemplate(url, dataTransformVO.getSourcePasswd(), dataTransformVO.getSourceUserName());
            JDBC_TEMPLATECON_CURRENT_HASHMAP.put(url, jdbcTemplate);
        }
        return jdbcTemplate;
    }


}
