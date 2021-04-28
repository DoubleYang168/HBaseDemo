package cn.edu.hhu.dingy;

import cn.edu.hhu.dingy.directory.TraverseDirectoryFiles;
import cn.edu.hhu.dingy.hdf5.ReadHdf5Files;
import cn.edu.hhu.dingy.hdf5.ReadLatLon;
import cn.edu.hhu.dingy.inceptor.InceptorDML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AppHuaiheThread extends Thread {
//    public static void main(String[] args) throws IOException {
//        writeHanJiangToHbase();
//    }

    public static void writeHanJiangToHbase() throws IOException {
        System.out.println("HanJiang任务启动!");
        String directory = "C:\\Users\\dingy\\Desktop\\hdf5_files\\hdf5_files\\evaporation2010-2018\\1";
        String latLonFilePath = "C:\\Users\\dingy\\Desktop\\LatLon\\LatLon\\ET_1Day_1km_hanjiang_20180302.h5";
        List<String> hdf5FilesList = TraverseDirectoryFiles.nonRecursionTraverseFolder(directory);
        String tableName = "ET_1Day_1km_huaihe";

        ReadHdf5Files hdf5Files = new ReadHdf5Files();
        ReadLatLon readLatLon = new ReadLatLon();

        String[] latArray = readLatLon.latLon(latLonFilePath, "lat");
        String[] lonArray = readLatLon.latLon(latLonFilePath, "lon");
        for (int h = 0; h < hdf5FilesList.size(); h++) {
            String hdf5FilePath = hdf5FilesList.get(h);
            String lastModified = hdf5FilePath.split("\\\\")[8].replace(".h5", "").split("_")[4];
            String hdf5 = hdf5Files.readHdf5(hdf5FilePath, "ET");
            List<List<String>> hanjiangList = new ArrayList<>();
            String[][] etData = hdf5Files.stringToStringArray2(hdf5);
            for (int i = 0; i < etData.length; i++) { // max i = 984
                for (int j = 0; j < etData[i].length; j++) { // max j = 495

                    // 按顺序封装数据信息(四个信息) getValue getLongitude getLatitude getDate
                    List<String> data = new ArrayList<>();
                    data.add(etData[i][j]);
                    data.add(lonArray[i]);
                    data.add(latArray[j]);
                    data.add(lastModified);
                    hanjiangList.add(data);
                    /*
                    //添加经度
                    hbaseDML.addRowData(tableName, rowKey, "location", "longitude", lonArray[j]);
                    //添加纬度
                    hbaseDML.addRowData(tableName, rowKey, "location", "latitude", latArray[i]);
                    //添加数据
                    hbaseDML.addRowData(tableName, rowKey, "data", "value", etData[i][j]);
                    //添加数据日期
                    hbaseDML.addRowData(tableName, rowKey, "matedata", "lastmodified", lastModified);
                    */
                }
            }
            InceptorDML.batchInsert(tableName, hanjiangList);
            System.out.println("----------目前为第" + h + "个文件----------");
        }
        System.out.println("任务结束");
    }

    @Override
    public void run() {
        try {
            writeHanJiangToHbase();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
