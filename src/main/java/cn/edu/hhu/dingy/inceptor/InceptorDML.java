package cn.edu.hhu.dingy.inceptor;

import cn.edu.hhu.dingy.entity.HanJang;
import cn.edu.hhu.dingy.utils.AbnormalUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class InceptorDML {

    //Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    //Hive2 JDBC URL with LDAP
    private static String jdbcURL = "jdbc:hive2://10.101.20.11:10000/evaporation";

    private static String user = "tom";
    private static String password = "tom";

    private static Connection conn = null;

    private static Statement stmt = null;

    private static ResultSet rs = null;

    static {
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
//            init();
            String sql = "create database " + databaseName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);

        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
//            destory();
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
//            init();
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
//            destory();
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
//            init();
            String sql = "drop database if exists " + databaseName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
//            destory();
        }
    }


    /**
     * 创建表
     *
     * @param createTableSql
     */
    public static void createTable(String createTableSql) {
        try {
//            init();
            System.out.println("Running: " + createTableSql);
            stmt.execute(createTableSql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
//            destory();
        }
    }

    public static boolean isTableExist(String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        boolean result = false;
        ResultSet set = metaData.getTables(null, null, tableName, null);
        if (set.next()) {
            result = true;
        }
        return result;
    }


    /**
     * 查询所有表
     *
     * @return
     */
    public static List<String> showTables(String databaseName) {
        List<String> list = new ArrayList<>();
        try {
//            init();
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
//            destory();
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
//            init();
            String sql = "load data inpath '" + hdfsPath + "' insert into table " + tableName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
//            destory();
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
//            init();
            System.out.println("Running: " + selectSql);
            rs = stmt.executeQuery(selectSql);
            while (rs.next()) {
                list.add(rs.getString(1));
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int size = rsmd.getColumnCount();
            while (rs.next()) {
                StringBuffer value = new StringBuffer();
                for (int i = 0; i < size; i++) {
                    value.append(rs.getString(i + 1)).append("\t");
                }
                System.out.println(value.toString());
            }
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
//            destory();
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
//            init();
            String sql = "drop table if exists " + databaseName + "." + tableName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
//            destory();
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

    /**
     * 批量插入
     *
     * @param tableName
     * @param dataList  中的 data 按顺序封装数据信息(四个信息) getValue getLongitude getLatitude getDate
     */
    public static void batchInsert(String tableName, List<List<String>> dataList) {
        try {
//            init();
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + "(data_value, longitude, latitude, observation_time)" + " VALUES (?, ?, ?, ?)");
            conn.setAutoCommit(false); // 关闭自动执行
            for (int i = 0; i < dataList.size(); i++) {
                List<String> data = dataList.get(i);
                for (int j = 0; j < data.size(); j++) {
                    //getValue
                    //getLongitude
                    //getLatitude
                    //getDate
                    pstmt.setString(j + 1, data.get(j));
                }
                pstmt.addBatch();
            }
            int[] ints = pstmt.executeBatch(); // 使用executeBatch执行批量SQL语句
            for (int i = 0; i < ints.length; i++) {
                System.out.println(ints[i]);
            }
            conn.commit();
            if (pstmt != null) {
                pstmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
//            destory();
        }
    }

    /**
     * 单插
     *
     * @param tableName
     * @param hanjiang
     */
    public static void singleInsert(String tableName, HanJang hanjiang) {
        try {
//            init();
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES (?, ?, ?, ?)");

            pstmt.setString(1, hanjiang.getValue());
            pstmt.setString(2, hanjiang.getLongitude());
            pstmt.setString(3, hanjiang.getLatitude());
            pstmt.setString(4, hanjiang.getDate());

            boolean execute = pstmt.execute();// 使用executeBatch执行批量SQL语句
            System.out.println(execute);

            if (pstmt != null) {
                pstmt.close();
            }
        } catch (SQLException e) {
            Object object = AbnormalUtils.getAbnormal(e);
            System.err.println(object);
        } finally {
//            destory();
        }
    }

    public static void main(String[] args) throws SQLException {

//        createDatabase("soil_test");
//        List<String> databases = showDatabases();
//        System.out.println(databases);
//        List<String> zhangji = showTables("zhangji");
//        System.out.println(zhangji);
//        selectData("SELECT * FROM ad_feature_orc");
//        boolean soil_test = databases.contains("soil_test");
//        System.out.println(soil_test);
//        List<String> strings = selectData("SELECT * FROM customer");
//        System.out.println(strings);
//        String createTableSql = "CREATE TABLE soil {name  STRING, password STRING};";
//        createTable(createTableSql);

//        init();
//        stmt.execute("CREATE TABLE user_info_tab (\n" +
//                "  name STRING,\n" +
//                "  acc_num STRING,\n" +
//                "  password STRING,\n" +
//                "  citizen_id STRING,\n" +
//                "  bank_acc STRING,\n" +
//                "  reg_date DATE,\n" +
//                "  acc_level STRING\n" +
//                ");\n");

//    stmt.execute("CREATE  TABLE ET_1Day_1km_ziyahe(\n" +
//            "data_value string DEFAULT NULL,\n" +
//            "longitude string DEFAULT NULL,\n" +
//            "latitude string DEFAULT NULL,\n" +
//            "observation_time string DEFAULT NULL\n" +
//            ")CLUSTERED BY (observation_time) INTO 40 BUCKETS\n" +
//            "STORED AS ORC\n" +
//            "TBLPROPERTIES('transactional' = 'true');");

//        stmt.execute("CREATE  TABLE user_info_tab(\n" +
//                "name string DEFAULT NULL,\n" +
//                "acc_num string DEFAULT NULL,\n" +
//                "password string DEFAULT NULL,\n" +
//                "citizen_id string DEFAULT NULL,\n" +
//                "bank_acc string DEFAULT NULL,\n" +
//                "reg_date date DEFAULT NULL,\n" +
//                "acc_level string DEFAULT NULL\n" +
//                ")CLUSTERED BY (acc_num) INTO 7 BUCKETS\n" +
//                "STORED AS ORC\n" +
//                "TBLPROPERTIES('transactional' = 'true');");
//        stmt.execute("CREATE TABLE user_table (username STRING,password STRING)");

//        HanJang hanJang = new HanJang();
//        hanJang.setDate("2020");
//        hanJang.setDate("2021");
//        hanJang.setLatitude("100");
//        hanJang.setLongitude("200");
//        System.out.println(hanJang);
//
//        List<HanJang> list = new ArrayList<>();
//        list.add(hanJang);
//
//        for (HanJang hanjiang1 : list) {
//            String latitude = hanjiang1.getLatitude();
//            System.out.println(latitude);
//        }
//        list.add(hanJang);
//        batchInsert("ET_1Day_1km_ziyahe", list);


//        dropTable("evaporation" ,"ET_1Day_1km_ziyahe");
//        stmt.execute("INSERT INTO ET_1Day_1km_ziyahe (data_value, longitude,latitude,observation_time) VALUES ('10010','123456', '654321', '20102020')");
//        ResultSet rs = stmt.executeQuery("SELECT * FROM ET_1Day_1km_ziyahe;");
//        ResultSetMetaData rsmd = rs.getMetaData();
//        int size = rsmd.getColumnCount();
//        while (rs.next()) {
//            StringBuffer value = new StringBuffer();
//            for (int i = 0; i < size; i++) {
//                value.append(rs.getString(i + 1)).append("\t");
//            }
//            System.out.println(value.toString());
//        }
//        rs.close();


//        createTable("CREATE TABLE ET_1Day_1km_ziyahe(\n" +
//                "data_value string DEFAULT NULL,\n" +
//                "longitude string DEFAULT NULL,\n" +
//                "latitude string DEFAULT NULL,\n" +
//                "observation_time string DEFAULT NULL\n" +
//                ")CLUSTERED BY (observation_time) INTO 40 BUCKETS\n" +
//                "STORED AS ORC\n" +
//                "TBLPROPERTIES('transactional' = 'true');");

//        boolean user_info_tab = isTableExist("ET_1Day_1km_ziyahe");
//        System.out.println(user_info_tab);

        stmt.close();
        conn.close();

    }

}
