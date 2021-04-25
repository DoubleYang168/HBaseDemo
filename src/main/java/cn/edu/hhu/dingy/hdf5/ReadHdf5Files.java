package cn.edu.hhu.dingy.hdf5;


import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import org.apache.commons.lang.ArrayUtils;
import com.alibaba.fastjson.JSON;

import java.io.File;

public class ReadHdf5Files {

    /**
     * 根据 path 和 dataset 读取数据集中的数据，将结果以字符串的形式返回
     * @param path
     * @param datasetName
     * @return
     */
    public String readHdf5(String path, String datasetName) {
        File file = new File(path);
        String s = null;
        try (HdfFile hdfFile = new HdfFile(file)) {
            Dataset dataset = hdfFile.getDatasetByPath("/"+datasetName);
            // data will be a Java array with the dimensions of the HDF5 dataset
            Object data = dataset.getData();
            s = ArrayUtils.toString(data);
            String name = hdfFile.getName();
            System.out.println(name);
        }
        return s;
    }

    /**
     * 将字符串的结果转成String类型的二维数组
     * @param str
     * @return
     */
    public String[][] stringToStringArray2(String str) {
        str = str.replace("{", "[").replace("}", "]");
        String[][] arr = JSON.parseObject(str, String[][].class);
        String[][] ds = new String[arr.length][arr[0].length];
        for (int j = 0; j < arr.length; j++) {
            for (int i = 0; i < arr[0].length; i++) {
                ds[j][i] = String.valueOf(arr[j][i]);
            }
        }
        return ds;
    }

    /**
     * 遍历 String 二维数组
     * @param arr
     */
    public void traverseStringArray2(String[][] arr) {
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr[i].length; j++) {
                System.out.print(arr[i][j] + " ");
            }
            System.out.println();
        }
    }
}
