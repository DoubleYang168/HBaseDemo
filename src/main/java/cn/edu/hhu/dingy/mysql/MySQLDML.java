package cn.edu.hhu.dingy.mysql;

import cn.edu.hhu.dingy.entity.HanJang;

import java.sql.*;
import java.util.List;

public class MySQLDML {
    public static String DRIVENAME = "com.mysql.jdbc.Driver";
    public static String URL = "jdbc:mysql//10.101.20.101:31001/ET_1Day_1km_hanjiang";
    public static String USER = "root";
    public static String PASSWORD = "password";

    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static ResultSet rs = null;

    /**
     * 连接数据库
     *
     * @return 数据库连接对象
     */
    public static Connection getConn() {
        try {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        if (conn != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } finally {
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
    }


    public static void insert(List<HanJang> etHanJangList) {

        //获取连接
        conn = getConn();
        try {
            String sql = "INSERT INTO ET_1Day_1km_hanjiang(value,longitude,latitude,date) VALUES (?,?,?,?)";
            ps = conn.prepareStatement(sql);

            ps.setString(1, etHanJangList.get(0).getValue());
            ps.setString(2, etHanJangList.get(0).getLongitude());
            ps.setString(3, etHanJangList.get(0).getLatitude());
            ps.setString(4, etHanJangList.get(0).getDate());
            ps.execute();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            MySQLDML.close(conn, ps, rs);
        }
    }
}
