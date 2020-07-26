package org.poem.util;


/**
 * 数据库类型
 */
public class DataSourceUrlHelper {


    /**
     * 获取数据库库的连接信息
     * @param ip 数据库的id
     * @param port 数据库的地址
     * @param schema  数据库schema
     * @param dataType  TDataSourceType 数据
     * @return 数据
     */
    public static String getDatabaseUrl(String ip, Integer port , String schema, String dataType){
        String url = "jdbc:mysql://" + ip + ":" + port + "/" + schema +
                "?characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai";
        if ("1".equals(dataType)) {
            url = "jdbc:mysql://" + ip + ":" + port + "/" + schema +
                    "?characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai";
        } else if ("7".equals(dataType)) {
            url = "jdbc:postgresql://" + ip + ":" + port + "/" + schema;
        } else if ("8".equals(dataType)) {
            url = "jdbc:sqlserver://" + ip + ":" + port + ";DatabaseName=" + schema;
        }
        return url;
    }
}
