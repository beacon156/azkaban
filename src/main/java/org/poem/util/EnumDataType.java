package org.poem.util;

/**
 * @author sangfor
 */
public enum EnumDataType {
    /**
     * null
     */
    NULL(""),
    /**
     * mysql
     */
    MYSQL("mysql"),
    /**
     * hive
     */
    HIVE("hive"),

    /**
     * postgres
     */
    POSTGRES("postgres"),
    /**
     * sqlserver
     */
    SQLSERVER("sqlserver"),

    /**
     *
     */
    HBASE("hbase"),
    /**
     * oracle
     */
    ORACLE("oracle");

    private String type;

    EnumDataType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return type;
    }
}
