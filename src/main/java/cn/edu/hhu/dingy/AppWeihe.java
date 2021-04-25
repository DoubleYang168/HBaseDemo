package cn.edu.hhu.dingy;

import cn.edu.hhu.dingy.directory.TraverseDirectoryFiles;
import cn.edu.hhu.dingy.hbase.HbaseDML;
import cn.edu.hhu.dingy.hdf5.ReadHdf5Files;
import cn.edu.hhu.dingy.hdf5.ReadLatLon;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AppWeihe {
    public static void main(String[] args) throws IOException {
        writeWeiheToHbase();
    }

    public static void writeWeiheToHbase() throws IOException {
        System.out.println("Weihe任务启动!");
        String directory = "F:\\Transwarp_Data\\evaporation2010-2018\\evaporation2010-2018\\evaporation2010-2018\\Weihe";
        String latLonFilePath = "F:\\Transwarp_Data\\LatLon\\LatLon\\ET_1Day_1km_Weihe_20120322.h5";
        List<String> hdf5FilesList = TraverseDirectoryFiles.nonRecursionTraverseFolder(directory);
        String tableName = "ET_1Day_1km_Weihe";
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
            for (int i = 0; i < etData.length; i++) { // max i = 984
                for (int j = 0; j < etData[i].length; j++) { // max j = 495
                    //定义rowKey
                    String rowKey = lastModified + i + j + "";
                    //添加经度
                    hbaseDML.addRowData(tableName, rowKey, "location", "longitude", latArray[j]);
                    //添加纬度
                    hbaseDML.addRowData(tableName, rowKey, "location", "latitude", lonArray[i]);
                    //添加数据
                    hbaseDML.addRowData(tableName, rowKey, "data", "value", etData[i][j]);
                    //添加数据日期
                    hbaseDML.addRowData(tableName, rowKey, "matedata", "lastmodified", lastModified);
                }
            }
            System.out.println("----------目前为第" + h + "个文件----------");
        }
        System.out.println("任务结束");
    }
}
