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
    public static void main(String[] args) {
        Thread hanjiang = new AppHanjiangThread();
        Thread huaihebenbu = new AppHuaihebenbuThread();
        Thread huaihe = new AppHuaiheThread();
        Thread weihe = new AppWeiheThread();
        Thread ziyahe = new AppZiyaheThread();

        hanjiang.start();
        huaihebenbu.start();
        huaihe.start();
        weihe.start();
        ziyahe.start();
    }
}
