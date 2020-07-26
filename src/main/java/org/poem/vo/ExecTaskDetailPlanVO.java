package org.poem.vo;

import lombok.Data;

/**
 * @author sangfor
 */
@Data
public class ExecTaskDetailPlanVO {


    /**
     * 名称
     */
    private String name;

    /**
     * 需要执行的sql
     */
    private String beforeSql;

    /**
     * 执行sql
     */
    private String aroundSql;

    /**
     * 数据源的地址
     */
    private String sourceIp;
    /**
     * 数据源的端口
     */
    private Integer sourcePort;

    /**
     * 数据库
     */
    private String sourceSchema;
    /**
     * 数据源的密码
     */
    private String sourcePasswd;

    /**
     * 数据源的用户信息
     */
    private String sourceUserName;

    /**
     * 目标库的数据的类型
     * TDataSourceType
     */
    private String sourceSourceType;


    /**
     * 目标数据源的url
     */
    private String targetIp;

    /**
     * 数据源的端口
     */
    private Integer targetPort;

    /**
     * 目标数据源的密码
     */
    private String targetPasswd;

    /**
     * 数据库
     */
    private String targetSchema;

    /**
     * 目标数据源的用户信息
     */
    private String targetUserName;
    /**
     * 目标库的数据的类型
     * TDataSourceType
     */
    private String targetSourceType;
}
