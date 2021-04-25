package cn.edu.hhu.dingy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDML {
    public static Configuration conf;

    public static String ZK_QUORUM = "88.18.0.101";//"docker-hbase";
    //    public static String ZK_ZNODE = "/hbase";
    public static String ZK_PORT = "32419";//"2181";
    public static String SUPER_USER = "hbase";

    static {
        // 使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK_QUORUM);
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
//        conf.set("zookeeper.znode.parent", ZK_ZNODE);
        conf.set("hbase.master.kerberos.principal", "hbase/DingYang@TDH");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/DingYang@TDH");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("zookeeper.znode.parent", "/hyperbase1");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hbase/tw-node2125", "/etc/hyperbase1/hbase.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {
        try {
            createTable("people", "info");
            addRowData("people", "2016001", "info", "name", "Cindy");
            getRow("people", "2016001");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        // 判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
//            dropTable(tableName);
//            System.out.println("表" + tableName + "已删除");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在");
        }
    }

    /**
     * 往指定表添加数据
     *
     * @param tableName 表名
     * @param puts      需要添加的数据
     * @return long                返回执行时间
     * @throws IOException
     */
    public static long putByHTable(String tableName, List<?> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        HTable hTable = new HTable(conf, tableName);
        hTable.setAutoFlushTo(false);
        hTable.setWriteBufferSize(5 * 1024 * 1024);
        try {
            hTable.put((List<Put>) puts);
            hTable.flushCommits();
        } finally {
            hTable.close();
        }
        return System.currentTimeMillis() - currentTime;
    }


    /**
     * 向表中插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
//        System.out.println("插入数据成功");
    }

    /**
     * 删除多行数据
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    /**
     * 获取所有数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowKey
                System.out.println("行键" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值" + Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }
    }

    /**
     * 获取某一行数据
     *
     * @param tableName
     * @param rowKey
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()) {
            //得到rowKey
            System.out.println("行键" + Bytes.toString(CellUtil.cloneRow(cell)));
            //得到列族
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳" + cell.getTimestamp());
        }
    }
}
