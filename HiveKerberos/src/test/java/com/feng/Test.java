package com.feng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author fqy
 * @since 2021-06-24
 */

public class Test {
    /**
     * 用于连接Hive所需的一些参数设置 driverName:用于连接 hive 的 JDBC 驱动名 When connecting to
     * HiveServer2 with Kerberos authentication, the URL format is:
     * jdbc:hive2://<host>:<port>/<db>;principal=
     * <Server_Principal_of_HiveServer2>
     */
    private static final Boolean isWin = System.getProperty("os.name").toLowerCase().startsWith("win");
    private static final String sep = isWin ? "\\" : "/";
    private static final String prefix = System.getProperty("user.dir") + sep + "HiveKerberos" + sep;

    private static final String krbUsername = "hive@TESTCLUSTER.COM";
    private static final String keytabPath = prefix + "keytab" + sep + "hive.keytab";
    private static final String krbConf = prefix + "krb5conf" + sep + "krb5.conf";

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String url = "jdbc:hive2://hadoop97.macro.com:10000/;principal=hive/_HOST@TESTCLUSTER.COM";

    private static ResultSet res;

    public static Connection getConn() throws SQLException, ClassNotFoundException {
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
    public static boolean showDatabases(Statement statement) {
        String sql = "SHOW DATABASES";
        try {
            System.out.println("===> " + sql);
            ResultSet res = statement.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            System.out.println("<=== ");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 查看数据库下所有的表
     */
    public static boolean showTables(Statement statement) {
        String sql = "SHOW TABLES";
        try {
            System.out.println("===> " + sql);
            ResultSet res = statement.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            System.out.println("<=== ");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取表的描述信息
     */
    public static boolean describeTable(Statement statement, String tableName) {
        String sql = "DESCRIBE " + tableName;
        try {
            System.out.println("===> " + sql);
            res = statement.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            System.out.println("<=== ");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除表
     */
    public static boolean dropTable(Statement statement, String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        System.out.println("===> " + sql);
        try {
            statement.execute(sql);
            System.out.println(tableName + " Drop success");
            return true;
        } catch (SQLException e) {
            System.out.println(tableName + " Drop failure");
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 查看表数据
     */
    public static boolean queryData(Statement statement, String tableName) {
        String sql = "SELECT * FROM " + tableName + " LIMIT 20";
        try {
            System.out.println("===> " + sql);
            res = statement.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "," + res.getString(2) + "," + res.getString(3));
            }
            System.out.println("<=== ");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建表
     */
    public static boolean createTable(Statement statement, String tableName) {
        String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM test1";
        System.out.println("===> " + sql);
        try {
            boolean execute = statement.execute(sql);
            System.out.println("<=== " + execute);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 显示创建表语句
     */
    public static boolean showCreateTable(Statement statement, String tableName) {
        String sql = "SHOW CREATE TABLE " + tableName;
        System.out.println("===> " + sql);
        try {
            res = statement.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            System.out.println("<=== ");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        String tableName = "aaa";
        try {
            Connection conn = getConn();
            Statement stmt = conn.createStatement();

            showDatabases(stmt);
            showTables(stmt);
            createTable(stmt, tableName);
            describeTable(stmt, tableName);
            showCreateTable(stmt, tableName);
            queryData(stmt, tableName);
            dropTable(stmt, tableName);

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("======= END ========");
        }
    }
}
