package cn.edu.hhu.dingy.hdf5;

public class ReadLatLon {

    /**
     * 读取经纬度文件
     * @param latLonFilePath
     * @param location
     * @return
     */
    public String[] latLon(String latLonFilePath, String location) {
        ReadHdf5Files hdf5Files = new ReadHdf5Files();
        String result = hdf5Files.readHdf5(latLonFilePath, location);
        String replace = result.replace("{", "").replace("}", "");
        String[] latLonArray = replace.split(",");
        return latLonArray;
    }
}
