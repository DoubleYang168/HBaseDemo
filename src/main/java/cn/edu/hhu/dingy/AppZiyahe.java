package cn.edu.hhu.dingy;

import cn.edu.hhu.dingy.directory.TraverseDirectoryFiles;
import cn.edu.hhu.dingy.hbase.HbaseDML;
import cn.edu.hhu.dingy.hdf5.ReadHdf5Files;
import cn.edu.hhu.dingy.hdf5.ReadLatLon;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AppZiyahe {
    public static void main(String[] args) throws Exception {
        writeZiyaheToHbase();
    }

    public static void writeZiyaheToHbase() throws Exception {
        System.out.println("Ziyahe任务启动!");
        String directory = "F:\\Transwarp_Data\\evaporation2010-2018\\evaporation2010-2018\\evaporation2010-2018\\ziyahe";
        String latLonFilePath = "F:\\Transwarp_Data\\LatLon\\LatLon\\ET_1Day_1km_ziyahe_20151219.h5";
        List<String> hdf5FilesList = TraverseDirectoryFiles.nonRecursionTraverseFolder(directory);
        String tableName = "ET_1Day_1km_ziyahe";
        HbaseDML hbaseDML = new HbaseDML();
        ReadHdf5Files hdf5Files = new ReadHdf5Files();
        ReadLatLon readLatLon = new ReadLatLon();
        hbaseDML.createTable(tableName, "location", "data", "matedata");
        String[] latArray = readLatLon.latLon(latLonFilePath, "lat");
        String[] lonArray = readLatLon.latLon(latLonFilePath, "lon");
        for (int h = 0; h < hdf5FilesList.size(); h++) {
            String hdf5FilePath = hdf5FilesList.get(h);
            String lastModified = hdf5FilePath.split("\\\\")[6].replace(".h5", "").split("_")[4];
            String hdf5 = hdf5Files.readHdf5(hdf5FilePath, "ET");
            String[][] etData = hdf5Files.stringToStringArray2(hdf5);
            List<Put> dataList = new ArrayList<>();
            for (int i = 0; i < etData.length; i++) { // max i = 984
                for (int j = 0; j < etData[i].length; j++) { // max j = 495
                    //定义rowKey
                    String rowKey = lastModified + i + j + "";

                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.add(Bytes.toBytes("location"), Bytes.toBytes("longitude"), Bytes.toBytes(latArray[j]));
                    put.add(Bytes.toBytes("location"), Bytes.toBytes("latitude"), Bytes.toBytes(lonArray[i]));
                    put.add(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(etData[i][j]));
                    put.add(Bytes.toBytes("matedata"), Bytes.toBytes("lastmodified"), Bytes.toBytes(lastModified));
                    dataList.add(put);
                    /*
                    //添加经度
                    hbaseDML.addRowData(tableName, rowKey, "location", "longitude", latArray[j]);
                    //添加纬度
                    hbaseDML.addRowData(tableName, rowKey, "location", "latitude", lonArray[i]);
                    //添加数据
                    hbaseDML.addRowData(tableName, rowKey, "data", "value", etData[i][j]);
                    //添加数据日期
                    hbaseDML.addRowData(tableName, rowKey, "matedata", "lastmodified", lastModified);
                    */
                }

                System.out.println("到第" + i + "行啦！" + "总共" + etData.length + "行");
            }
            hbaseDML.putByHTable(tableName, dataList);
            Date data = new Date();
            System.out.println("----------目前为第" + h + "个文件----------" + data);
        }
        System.out.println("任务结束");
    }
}
