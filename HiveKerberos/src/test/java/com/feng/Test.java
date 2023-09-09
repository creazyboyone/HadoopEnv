package com.feng;


import java.sql.Connection;
import java.sql.Statement;

/**
 * @author fqy
 * @since 2021-06-24
 */

public class Test {

    public static void main(String[] args) {
        TableUtils tableUtils = new TableUtils();
        String tableName = "aaa";

        try {
            Connection conn = tableUtils.getConn();
            Statement stmt = conn.createStatement();

            tableUtils.showDatabases(stmt);
            tableUtils.showTables(stmt);
            tableUtils.createTable(stmt, tableName);
            tableUtils.describeTable(stmt, tableName);
            tableUtils.showCreateTable(stmt, tableName);
            tableUtils.queryData(stmt, tableName);
            tableUtils.dropTable(stmt, tableName);

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("======= END ========");
        }
    }
}
