package com.feng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;


/**
 * @author fqy
 * @since 2023-09-09
 */

public class TableUtils {
    /**
     * 用于连接Hive所需的一些参数设置 driverName:用于连接 hive 的 JDBC 驱动名 When connecting to
     * HiveServer2 with Kerberos authentication, the URL format is:
     * jdbc:hive2://<host>:<port>/<db>;principal=
     * <Server_Principal_of_HiveServer2>
     */
    private static final boolean isWin = System.getProperty("os.name").toLowerCase().startsWith("win");
    private static final String sep = isWin ? "\\" : "/";
    private static final String prefix = ClassLoader.getSystemResource("").getPath();

    private static final String krbUsername = "hive@TESTCLUSTER.COM";
    private static final String keytabPath = prefix + "keytab" + sep + "hive.keytab";
    private static final String krbConf = prefix + "krb5conf" + sep + "krb5.conf";

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String url = "jdbc:hive2://hadoop97.macro.com:10000/;principal=hive/_HOST@TESTCLUSTER.COM";

    private static ResultSet res;

    public Connection getConn() throws SQLException, ClassNotFoundException {
        System.setProperty("java.security.krb5.conf", krbConf);

        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        try {
            UserGroupInformation.loginUserFromKeytab(krbUsername, keytabPath);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        Class.forName(driverName);
        return DriverManager.getConnection(url);
    }

    /**
     * 查看所有的数据库
     *
     */
    public boolean showDatabases(Statement statement) {
        String sql = "SHOW DATABASES";
        return getStringFromSQL(statement, sql);
    }

    /**
     * 查看数据库下所有的表
     */
    public boolean showTables(Statement statement) {
        String sql = "SHOW TABLES";
        return getStringFromSQL(statement, sql);
    }

    /**
     * 获取表的描述信息
     */
    public boolean describeTable(Statement statement, String tableName) {
        String sql = "DESCRIBE " + tableName;
        return getStringFromSQL(statement, sql);
    }

    /**
     * 显示创建表语句
     */
    public boolean showCreateTable(Statement statement, String tableName) {
        String sql = "SHOW CREATE TABLE " + tableName;
        return getStringFromSQL(statement, sql);
    }

    /**
     * 查看表数据
     */
    public boolean queryData(Statement statement, String tableName) {
        String sql = "SELECT * FROM " + tableName + " LIMIT 20";
        return getStringFromSQL(statement, sql);
    }

    /**
     * 创建表
     */
    public boolean createTable(Statement statement, String tableName) {
        String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM test1";
        return getResultFromSQL(statement, sql, tableName + " CREATE ");
    }

    /**
     * 删除表
     */
    public boolean dropTable(Statement statement, String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        return getResultFromSQL(statement, sql, tableName + " DROP ");
    }

    private boolean getStringFromSQL(Statement statement, String sql) {
        System.out.println("===> " + sql);
        try {
            ResultSet res = statement.executeQuery(sql);
            ResultSetMetaData metaData = res.getMetaData();
            int count = metaData.getColumnCount();
            while (res.next()) {
                for (int i = 1; i <= count; ++i) {
                    System.out.print(res.getString(i));
                    System.out.print("\t\t");
                }
                System.out.println();
            }
            System.out.println("<=== ");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean getResultFromSQL(Statement statement, String sql, String actionDesc) {
        System.out.println("===> " + sql);
        try {
            statement.execute(sql);
            System.out.println("<=== " + actionDesc + "Success");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("<=== " + actionDesc + "Failure");
        return false;
    }
}
