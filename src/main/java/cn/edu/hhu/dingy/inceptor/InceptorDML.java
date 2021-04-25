package cn.edu.hhu.dingy.inceptor;

import cn.edu.hhu.dingy.utils.AbnormalUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class InceptorDML {

    //Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    //Hive2 JDBC URL with LDAP
    private static String jdbcURL = "jdbc:hive2://10.101.20.11:10000/soil_test";

    private static String user = "tom";
    private static String password = "tom";

    private static Connection conn = null;

    private static Statement stmt = null;

    private static ResultSet rs = null;

    private static void init() {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcURL, user, password);
            stmt = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void createDatabase(String databaseName) {
        try {
            init();
            String sql = "create database " + databaseName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);

        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }

    }

    /**
     * 查询所有数据库
     *
     * @return
     */
    public static List<String> showDatabases() {
        List<String> list = new ArrayList<>();
        try {
            init();
            String sql = "show databases";
            System.out.println("Running: " + sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                list.add(rs.getString(1));
            }
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }
        return list;
    }


    /**
     * 删除数据库
     *
     * @param databaseName
     */
    public static void dropDatabase(String databaseName) {
        try {
            init();
            String sql = "drop database if exists " + databaseName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }
    }


    /**
     * 创建表
     *
     * @param createTableSql
     */
    public static void createTable(String createTableSql) {
        try {
            init();
            System.out.println("Running: " + createTableSql);
            stmt.execute(createTableSql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }
    }

    /**
     * 查询所有表
     *
     * @return
     */
    public static List<String> showTables(String databaseName) {
        List<String> list = new ArrayList<>();
        try {
            init();
            String useSql = "use " + databaseName;
            System.out.println("Running: " + useSql);
            stmt.execute(useSql);
            String sql = "show tables";
            System.out.println("Running: " + sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                list.add(rs.getString(1));
            }
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }
        return list;
    }


    /**
     * 加载数据
     *
     * @param hdfsPath
     * @param tableName
     */

    public static void loadData(String hdfsPath, String tableName) {
        try {
            init();
            String sql = "load data inpath '" + hdfsPath + "' insert into table " + tableName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }
    }


    /**
     * 查询数据
     *
     * @param selectSql
     * @return
     */
    public static List<String> selectData(String selectSql) {
        List<String> list = new ArrayList<>();
        try {
            init();
            System.out.println("Running: " + selectSql);
            rs = stmt.executeQuery(selectSql);
            while (rs.next()) {
                list.add(rs.getString(2));
            }
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }
        return list;
    }


    /**
     * 删除数据库表
     *
     * @param databaseName
     * @param tableName
     */

    public static void dropTable(String databaseName, String tableName) {
        try {
            init();
            String sql = "drop table if exists " + databaseName + "." + tableName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
            destory();
        }
    }


    /**
     * 释放资源
     */

    private static void destory() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }

            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        }
    }


    public static void main(String[] args) throws SQLException {

//        createDatabase("soil_test");
        List<String> databases = showDatabases();
        System.out.println(databases);
//        boolean soil_test = databases.contains("soil_test");
//        System.out.println(soil_test);
//        List<String> strings = selectData("SELECT * FROM customer");
//        System.out.println(strings);
//        String createTableSql = "CREATE TABLE soil {name  STRING, password STRING};";
//        createTable(createTableSql);
    }
}
