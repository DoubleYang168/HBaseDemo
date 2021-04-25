package cn.edu.hhu.dingy;


import cn.edu.hhu.dingy.directory.TraverseDirectoryFiles;
import cn.edu.hhu.dingy.hbase.HbaseDML;
import cn.edu.hhu.dingy.hdf5.ReadHdf5Files;
import cn.edu.hhu.dingy.hdf5.ReadLatLon;

import java.io.IOException;
import java.util.List;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        writeHanJiangToHbase();
    }

    public static void writeHanJiangToHbase() throws IOException {
        System.out.println("HanJiang任务启动!");
        String directory = "C:\\Users\\dingy\\Desktop\\evaporation2010-2018\\evaporation2010-2018\\hanjiang";
//        String hdf5FilePath = "C:\\Users\\dingy\\Desktop\\hdf5_files\\hdf5_files\\evaporation2010-2018\\ET_1Day_1km_hanjiang_20100101.h5";
        String latLonFilePath = "C:\\Users\\dingy\\Desktop\\LatLon\\LatLon\\ET_1Day_1km_hanjiang_20180302.h5";
        List<String> hdf5FilesList = TraverseDirectoryFiles.nonRecursionTraverseFolder(directory);
        for (int h = 0; h < hdf5FilesList.size(); h++) {
            String hdf5FilePath = hdf5FilesList.get(h);
            String tableName = hdf5FilePath.split("\\\\")[7].replace(".h5", "");
            HbaseDML hbaseDML = new HbaseDML();
            ReadHdf5Files hdf5Files = new ReadHdf5Files();
            ReadLatLon readLatLon = new ReadLatLon();
            hbaseDML.createTable(tableName, "location", "data");
            String hdf5 = hdf5Files.readHdf5(hdf5FilePath, "ET");
            String[] latArray = readLatLon.latLon(latLonFilePath, "lat");
            String[] lonArray = readLatLon.latLon(latLonFilePath, "lon");

            String[][] etData = hdf5Files.stringToStringArray2(hdf5);

            System.out.println(etData.length);
            System.out.println(etData[1].length);
            for (int i = 0; i < etData.length; i++) { // max i = 984
                for (int j = 0; j < etData[i].length; j++) { // max j = 495
                    //添加经度
                    hbaseDML.addRowData(tableName, i + "" + j, "location", "longitude", latArray[j]);
                    //添加纬度
                    hbaseDML.addRowData(tableName, i + "" + j, "location", "latitude", lonArray[i]);
                    //添加数据
                    hbaseDML.addRowData(tableName, i + "" + j, "data", "value", etData[i][j]);
                }
            }
            System.out.println("表格" + tableName + "插入数据完毕");
        }
        System.out.println("任务结束");
    }
}
